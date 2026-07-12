import runpy
import sys


if __name__ == "__main__":
    runpy.run_module("mlb_pitcher_report.odds.oddapi", run_name="__main__")
else:
    from mlb_pitcher_report.odds import oddapi as _impl

    sys.modules[__name__] = _impl

