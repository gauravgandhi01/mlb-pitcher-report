import sys

from mlb_pitcher_report.shared import report_data as _impl

sys.modules[__name__] = _impl

