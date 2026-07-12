import runpy
import sys


if __name__ == "__main__":
    runpy.run_module("mlb_pitcher_report.reports.pitchers", run_name="__main__")
else:
    from mlb_pitcher_report.reports import pitchers as _impl

    sys.modules[__name__] = _impl

