from adapters.repositories.fund_nav_daily_repository import FundNavDailyRepository
from adapters.models.fund_nav_daily_model import FundNavDaily

from adapters.repositories.set_repository import SETRepository
from adapters.models.set_model import SET

from adapters.repositories.ytm_repository import YTMRepository
from adapters.models.ytm_model import YTM

model_mapping = {
    FundNavDailyRepository: FundNavDaily,
    SETRepository: SET,
    YTMRepository: YTM,
}