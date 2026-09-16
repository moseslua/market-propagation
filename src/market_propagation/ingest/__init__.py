"""Read-only public data acquisition, normalization and the G0 coverage audit.

This is a package marker, not an export barrel. Each public symbol lives in the
module that owns it and callers import from there:

* :mod:`~market_propagation.ingest.transport` — GET-only transport, bounded
  retries, raw archival.
* :mod:`~market_propagation.ingest.pagination` — bounded cursor pagination with
  loop detection.
* :mod:`~market_propagation.ingest.kalshi_rest` — Kalshi public market data.
* :mod:`~market_propagation.ingest.macro_releases` — BLS calendar and archived
  first releases.
* :mod:`~market_propagation.ingest.normalize` — wire payloads to domain records.
* :mod:`~market_propagation.ingest.polymarket_public` — Polymarket public REST
  and subscription capture.
* :mod:`~market_propagation.ingest.audit` — the bounded G0 coverage audit.

Two invariants apply to everything the package exposes.

**Raw before normalized.** Every byte is written to a content-addressed
:class:`~market_propagation.storage.RawStore` before any parser sees it, so every
normalized record's ``provenance.raw_hash`` resolves to a stored payload.

**Backed by GET only.** No module here issues a non-GET request, carries a
credential, or provides a route around an access restriction. An endpoint that
refuses access produces a recorded blocked status with ``empty_result: False``,
never an empty successful result.

Observed access status at implementation time, which callers must not overstate:

* Kalshi public market data — reachable without credentials, verified against live
  responses. The WebSocket order-book channel requires authentication and is
  therefore **not** implemented.
* BLS calendars and archived releases — reachable, verified against live
  responses. The archive-payload URLs return 403 to a plain command-line client
  while succeeding through a browser user agent, so a fetch gap there is a client
  restriction rather than a missing release.
* Polymarket — **unreachable** in this environment across every documented host.
  The client is fail-closed: :class:`~market_propagation.ingest.polymarket_public.PolymarketUnreachable`
  is raised rather than an empty dataset returned.

Typical use::

    from pathlib import Path
    from market_propagation.storage import RawStore
    from market_propagation.ingest.transport import HttpTransport, RetryPolicy
    from market_propagation.ingest.kalshi_rest import KalshiClient
    from market_propagation.ingest.audit import CohortAuditor

    store = RawStore(Path("data/raw"))
    with HttpTransport(store, policy=RetryPolicy(min_interval_seconds=0.1)) as transport:
        kalshi = KalshiClient(store, transport=transport)
        cutoff = kalshi.get_historical_cutoff()
        decision = kalshi.resolve_partition(window_start, window_end, cutoff=cutoff)
"""

from __future__ import annotations

__all__: list[str] = []
