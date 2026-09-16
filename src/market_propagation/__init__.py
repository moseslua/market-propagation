"""market_propagation: point-in-time prediction-market information research.

This is a package marker, not an export barrel. Each public symbol lives in the
module that owns it, under the name it has there, and callers import from that
module:

* :mod:`~market_propagation.domain` — frozen records, identity keys, parsing.
* :mod:`~market_propagation.point_in_time` — as-of features and the event panel.
* :mod:`~market_propagation.replay` — book reconstruction and dual-order folds.
* :mod:`~market_propagation.storage` — content-addressed raw store and Parquet.
* :mod:`~market_propagation.models`, :mod:`~market_propagation.simulation`,
  :mod:`~market_propagation.evaluation`,
  :mod:`~market_propagation.falsification` — the fitted ladder, its gates and
  their audits.
* :mod:`~market_propagation.sample`, :mod:`~market_propagation.reporting` — the
  packaged sample and the reproduction that publishes it.
* :mod:`~market_propagation.operations`, :mod:`~market_propagation.cli` and
  :mod:`~market_propagation.ingest` — acquisition, audit and the command line.

A name re-exported here would be a second import path to keep in step with the
owning module, so no symbol is re-exported.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
