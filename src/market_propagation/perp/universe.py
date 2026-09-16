"""Enumerate the asset universe PerpDexList publishes, from its own index.

The universe is read rather than hard-coded because the source adds and drops
assets continuously and a stale list would silently shrink the sample. The index
page is one fetch, so re-reading it each sweep is cheap.

Two shapes on the index page matter:

* A linked asset, ``BTC``, is one the source gives a
  market page.
* A linked funding route, ``BTC/funding``, is
  the subset for which a settled-funding history page exists. That subset is
  strictly smaller, so the two are tracked separately and an asset without a
  funding route is recorded as such rather than fetched and 404ed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

LINK_RE = re.compile(r"\[([^\]]+)\]\((/(?:markets)/[A-Za-z0-9_./%-]+)\)")

#: The index groups assets under ``### ... (n)`` headings. Only crypto and rwa
#: are published today; the class is read off the path anyway so a new heading
#: needs no code change.
ASSET_PATH_RE = re.compile(r"^/markets/(?P<asset_class>[a-z0-9_]+)/(?P<asset>[^/]+)$")
FUNDING_PATH_RE = re.compile(r"^/markets/(?P<asset_class>[a-z0-9_]+)/(?P<asset>[^/]+)/funding$")


@dataclass(frozen=True, slots=True)
class AssetRef:
    """One tradable asset, keyed by its ``(class, ticker)`` path pair."""

    asset_class: str
    asset: str

    @property
    def market_path(self) -> str:
        return f"/markets/{self.asset_class}/{self.asset}"

    @property
    def funding_path(self) -> str:
        return f"{self.market_path}/funding"

    @property
    def key(self) -> tuple[str, str]:
        return (self.asset_class, self.asset)


@dataclass(frozen=True, slots=True)
class Universe:
    """The assets the source publishes, and which of them have funding history."""

    assets: tuple[AssetRef, ...]
    with_funding_history: frozenset[tuple[str, str]]

    @property
    def count(self) -> int:
        return len(self.assets)

    def has_funding_history(self, ref: AssetRef) -> bool:
        return ref.key in self.with_funding_history


def parse_priority(text: str) -> tuple[AssetRef, ...]:
    """The source's own "Most traded assets" ranking, in the order published.

    Read from the page rather than configured, so the priority set tracks the
    source's liquidity ranking instead of freezing whatever was liquid on the
    day the collector was written. The section is the last link group on each
    market page, under a heading whose text is matched loosely so a wording
    change degrades to "no priority" rather than to a wrong one.
    """
    lines = text.splitlines()
    start: int | None = None
    for index, line in enumerate(lines):
        if line.strip().lower().startswith("## most traded"):
            start = index + 1
            break
    if start is None:
        return ()

    refs: dict[tuple[str, str], AssetRef] = {}
    for line in lines[start:]:
        stripped = line.strip()
        if stripped.startswith("## "):
            break
        for _, path in LINK_RE.findall(stripped):
            plain = ASSET_PATH_RE.match(path)
            if plain is not None:
                key = (plain.group("asset_class"), plain.group("asset"))
                refs.setdefault(key, AssetRef(*key))
    return tuple(refs.values())


def parse_universe(text: str) -> Universe:
    """Read the asset list and the funding-route subset from the index page.

    An asset appearing only as a funding route is still counted as an asset,
    because the market page necessarily exists for its funding page to be
    linked. That keeps the universe complete when the index lists the two
    asymmetrically.
    """
    assets: dict[tuple[str, str], AssetRef] = {}
    with_funding: set[tuple[str, str]] = set()

    for _, path in LINK_RE.findall(text):
        funding = FUNDING_PATH_RE.match(path)
        if funding is not None:
            key = (funding.group("asset_class"), funding.group("asset"))
            with_funding.add(key)
            assets.setdefault(key, AssetRef(*key))
            continue
        plain = ASSET_PATH_RE.match(path)
        if plain is not None:
            key = (plain.group("asset_class"), plain.group("asset"))
            assets.setdefault(key, AssetRef(*key))

    ordered = tuple(assets[key] for key in sorted(assets))
    return Universe(assets=ordered, with_funding_history=frozenset(with_funding))
