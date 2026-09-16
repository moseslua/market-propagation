"""``python -m market_propagation`` entry point.

The console script and the module entry point share one implementation and one
exit-code convention, so ``python -m market_propagation <cmd>`` and
``market-propagation <cmd>`` behave identically.
"""

from __future__ import annotations

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
