from enum import Enum
from adapters.models.data_invalid_dates_model import DataInvalidDates
from adapters.models.fund_nav_daily_model import FundNavDaily
from adapters.models.set_model import SET
from adapters.models.ytm_model import YTM

class ModelEnum(Enum):
    FUND_NAV_DAILY = FundNavDaily
    SET = SET
    YTM = YTM
    DATA_INVALID_DATES = DataInvalidDates
    