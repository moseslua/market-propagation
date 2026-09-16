"""GET-only public HTTP transport with bounded retries, pacing and raw archival.

Three rules hold for every byte this module touches:

1. The response body reaches ``RawStore`` before any parser sees it, so every
   normalized record can point back at the payload it came from.
2. Retries are bounded and ``Retry-After`` is honored, including by *not*
   retrying early when the server asks for longer than the run budget allows.
3. A blocked endpoint produces a recorded blocked status. It never becomes an
   empty successful result.

No request in this module carries credentials, and there is deliberately no
code path that issues a non-GET request.
"""

from __future__ import annotations

import datetime as dt
import email.utils
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx

from ..storage import Provenance, RawStore

USER_AGENT = "market-propagation-research/0.1 (public read-only data acquisition)"

#: Statuses Kalshi documents as transient. The rate-limit page states that 429
#: carries no ``Retry-After`` header and that exponential backoff is the
#: documented client response, so 429 is retried on the backoff curve.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class TransportError(RuntimeError):
    """A request that did not produce a usable 2xx response.

    Carries the archived payload hash for the failing body, so a blocked
    endpoint is auditable rather than merely absent.
    """

    def __init__(
        self,
        message: str,
        *,
        url: str,
        reason: str,
        status_code: int | None = None,
        attempts: int = 0,
        payload_hash: str | None = None,
        observed_at: dt.datetime | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.reason = reason
        self.status_code = status_code
        self.attempts = attempts
        self.payload_hash = payload_hash
        self.observed_at = observed_at

    def as_blocked_record(self) -> dict[str, Any]:
        observed = self.observed_at
        if observed is None:
            observed = dt.datetime.now(dt.UTC)
        return blocked_record(
            url=self.url,
            status_code=self.status_code,
            reason=self.reason,
            attempts=self.attempts,
            payload_hash=self.payload_hash,
            observed_at=observed,
        )


class WireShapeError(RuntimeError):
    """A response body did not match the documented schema.

    Raised instead of guessing at missing fields. This is the schema-change
    alarm: an upstream rename surfaces here rather than as a silently empty
    study.
    """


def blocked_record(
    *,
    url: str,
    status_code: int | None,
    reason: str,
    attempts: int,
    payload_hash: str | None,
    observed_at: dt.datetime,
) -> dict[str, Any]:
    """A machine-readable access failure.

    ``empty_result`` is always ``False``. Callers write this when an endpoint
    refuses access, so downstream code cannot mistake an access restriction for
    an observed absence of data.
    """
    return {
        "recorded": True,
        "empty_result": False,
        "url": url,
        "status_code": status_code,
        "reason": reason,
        "attempts": attempts,
        "payload_hash": payload_hash,
        "observed_at": observed_at.astimezone(dt.UTC).isoformat(),
    }


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded retry and pacing configuration.

    ``max_retry_after_seconds`` exists so a server-supplied wait cannot stall a
    bounded audit. When the requested wait exceeds it, the transport stops and
    records the endpoint as blocked rather than retrying sooner than asked.
    """

    attempts: int = 3
    initial_backoff_seconds: float = 0.5
    max_backoff_seconds: float = 8.0
    respect_retry_after: bool = True
    max_retry_after_seconds: float = 60.0
    retry_statuses: frozenset[int] = RETRYABLE_STATUSES
    #: Minimum seconds between consecutive requests in one transport instance.
    #: Configured pacing, not a hardcoded quota. The documented Kalshi budgets
    #: are token buckets per tier, so the conservative default keeps a single
    #: sequential client far below the lowest documented read budget.
    min_interval_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be at least 1")
        if self.initial_backoff_seconds < 0 or self.max_backoff_seconds < 0:
            raise ValueError("backoff must not be negative")

    def backoff_for(self, attempt: int) -> float:
        """Exponential backoff for a 1-based ``attempt`` that just failed."""
        raw = self.initial_backoff_seconds * (2.0 ** (attempt - 1))
        return min(raw, self.max_backoff_seconds)


def retry_after_seconds(headers: Mapping[str, str], *, now: dt.datetime) -> float | None:
    """Parse ``Retry-After`` in either documented form, or ``None`` if absent.

    Kalshi documents that 429 responses carry no ``Retry-After``; other public
    sources do send it, so the parse is shared.
    """
    raw = None
    for key, value in headers.items():
        if key.lower() == "retry-after":
            raw = value.strip()
            break
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=dt.UTC)
    return max(0.0, (when - now).total_seconds())


def server_date(headers: Mapping[str, str]) -> dt.datetime | None:
    """The response's own ``Date`` instant, or ``None`` when it states none."""
    raw = headers.get("date")
    if not raw:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(str(raw))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def build_url(base: str, params: Mapping[str, Any] | None = None) -> str:
    """Append query parameters, dropping ``None`` values.

    Dropping ``None`` matters for Kalshi: its timestamp filters are mutually
    exclusive, so an unset filter must be omitted rather than sent empty.
    """
    if not params:
        return base
    clean = {k: v for k, v in params.items() if v is not None}
    if not clean:
        return base
    joiner = "&" if "?" in base else "?"
    return f"{base}{joiner}{urlencode(clean, doseq=True)}"


@dataclass(frozen=True, slots=True)
class ResponseEnvelope:
    """One archived response. ``provenance.raw_hash`` addresses the body."""

    url: str
    status_code: int
    body: bytes
    content_type: str
    received_time: dt.datetime
    monotonic_ns: int
    provenance: Provenance
    attempts: int
    params: tuple[tuple[str, str], ...] = ()
    #: The instant the *serving* system states it answered at, read from the
    #: response's own ``Date`` header. It is never this run's clock: a caller that
    #: needs an instant the source records reads this, and a response that states
    #: none leaves this ``None`` rather than falling back to ``received_time``.
    server_date: dt.datetime | None = None

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        try:
            return json.loads(self.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WireShapeError(f"response body from {self.url} is not JSON: {exc}") from exc


class HttpTransport:
    """Sequential GET client that archives every body it receives.

    The absence of a ``method`` argument is intentional: there is no supported
    way to make this class issue anything but a GET.
    """

    def __init__(
        self,
        store: RawStore,
        *,
        client: httpx.Client | None = None,
        timeout_seconds: float = 30.0,
        policy: RetryPolicy | None = None,
        sleep: Any = None,
        now: Any = None,
    ) -> None:
        self._store = store
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        self._policy = policy or RetryPolicy()
        self._sleep = sleep if sleep is not None else time.sleep
        self._now = now if now is not None else (lambda: dt.datetime.now(dt.UTC))
        self._last_request_ns: int | None = None

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> HttpTransport:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _pace(self) -> None:
        interval = self._policy.min_interval_seconds
        if interval <= 0 or self._last_request_ns is None:
            return
        elapsed = (time.monotonic_ns() - self._last_request_ns) / 1e9
        if elapsed < interval:
            self._sleep(interval - elapsed)

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        source: str,
        record_id: str | None = None,
        accept: str | None = None,
        note: str | None = None,
    ) -> ResponseEnvelope:
        """GET ``url``, archiving the body, honoring Retry-After, bounded.

        ``record_id`` is a **request label**, not a raw occurrence identity: it
        names the request or the page the caller asked for ("cutoff",
        "hist-trades-00001"). Two genuine GETs can carry the same label while
        returning different bytes, because a moving cutoff advances and a page
        repeats with new content. The label is therefore retained in the receipt
        metadata for checkpoint auditing, and each received response is archived
        under a freshly generated occurrence identity. Two byte-identical
        responses remain two occurrences even when this transport reads the same
        wall clock for both; only the blob is deduplicated by content.

        Raises :class:`TransportError` on terminal failure. The failing body is
        archived before the raise so ``error.payload_hash`` is always a real
        reference.
        """
        request_url = build_url(url, params)
        policy = self._policy
        headers = {"Accept": accept} if accept else None
        params_snapshot = tuple(
            (str(k), str(v)) for k, v in (params or {}).items() if v is not None
        )

        for attempt in range(1, policy.attempts + 1):
            self._pace()
            self._last_request_ns = time.monotonic_ns()
            try:
                response = self._client.get(request_url, headers=headers)
            except httpx.HTTPError as exc:
                observed = self._now()
                if attempt >= policy.attempts:
                    raise TransportError(
                        f"transport failure for {request_url}: {exc}",
                        url=request_url,
                        reason="transport_error",
                        attempts=attempt,
                        observed_at=observed,
                    ) from exc
                self._sleep(policy.backoff_for(attempt))
                continue

            received = self._now()
            monotonic_ns = time.monotonic_ns()
            body = response.content

            if response.status_code in policy.retry_statuses and attempt < policy.attempts:
                delay = None
                if policy.respect_retry_after:
                    delay = retry_after_seconds(response.headers, now=received)
                if delay is not None and delay > policy.max_retry_after_seconds:
                    provenance = self._archive(
                        body,
                        source=source,
                        request_label=record_id,
                        url=request_url,
                        status_code=response.status_code,
                        content_type=response.headers.get("content-type", ""),
                        received_time=received,
                        attempts=attempt,
                        note=note or "retry-after exceeded run budget",
                    )
                    raise TransportError(
                        f"{request_url} requested a {delay:.0f}s wait, beyond the "
                        f"{policy.max_retry_after_seconds:.0f}s run budget",
                        url=request_url,
                        reason="retry_after_exceeds_budget",
                        status_code=response.status_code,
                        attempts=attempt,
                        payload_hash=provenance.raw_hash,
                        observed_at=received,
                    )
                self._sleep(delay if delay is not None else policy.backoff_for(attempt))
                continue

            is_success = 200 <= response.status_code < 300
            provenance = self._archive(
                body,
                source=source,
                request_label=record_id,
                url=request_url,
                status_code=response.status_code,
                content_type=response.headers.get("content-type", ""),
                received_time=received,
                attempts=attempt,
                note=None if is_success else f"http {response.status_code}",
            )
            if not is_success:
                raise TransportError(
                    f"HTTP {response.status_code} for {request_url}",
                    url=request_url,
                    reason="http_status",
                    status_code=response.status_code,
                    attempts=attempt,
                    payload_hash=provenance.raw_hash,
                    observed_at=received,
                )
            return ResponseEnvelope(
                url=request_url,
                status_code=response.status_code,
                body=body,
                content_type=response.headers.get("content-type", ""),
                received_time=received,
                monotonic_ns=monotonic_ns,
                provenance=provenance,
                attempts=attempt,
                params=params_snapshot,
                server_date=server_date(response.headers),
            )

        raise TransportError(  # pragma: no cover - loop always returns or raises
            f"retry budget exhausted for {request_url}",
            url=request_url,
            reason="budget_exhausted",
            attempts=policy.attempts,
            observed_at=self._now(),
        )

    def _archive(
        self,
        body: bytes,
        *,
        source: str,
        request_label: str | None,
        url: str,
        status_code: int,
        content_type: str,
        received_time: dt.datetime,
        attempts: int,
        note: str | None,
    ) -> Provenance:
        metadata: dict[str, Any] = {
            "method": "GET",
            "url": url,
            "http_status": status_code,
            "content_type": content_type,
            "request_url": url,
            "attempts": attempts,
            "received_time": received_time.isoformat(),
            "credentials": "none",
        }
        if request_label is not None:
            metadata["request_label"] = request_label
        if note:
            metadata["note"] = note
        # No ``record_id`` is passed: this is one received response, and the
        # store mints a fresh occurrence identity for it. Passing the request
        # label here would key the receipt on the label alone, which is exactly
        # how an advancing cutoff or a repeating page collides with itself.
        return self._store.put(
            body,
            source=source,
            received_time=received_time,
            metadata=metadata,
        )
