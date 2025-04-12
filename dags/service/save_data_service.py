import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from utils.func import *


from utils.indicator_processor import IndicatorProcessor

from adapters.database.mysql_database import MysqlDatabase

from adapters.repositories.fund_nav_daily_repository import FundNavDailyRepository
from adapters.models.fund_nav_daily_model import FundNavDaily

from adapters.repositories.data_invalid_dates_repository import DataInvalidDatesRepository
from adapters.models.data_invalid_dates_model import DataInvalidDates

from adapters.repositories.set_repository import SETRepository
from adapters.models.set_model import SET

from adapters.repositories.ytm_repository import YTMRepository
from adapters.models.ytm_model import YTM

from adapters.repositories.send_predict_invalid_dates_repository import SendPredictInvalidDatesRepository
from adapters.models.send_predict_invalid_dates_model import SendPredictInvalidDates

from adapters.repositories.Irepository import IRepository

from adapters.exceptions import DatabaseException

from utils.model_mapping import model_mapping

class SaveDataService:
    def __init__(self, repository: IRepository,
                 send_predict_invalid_dates_repo: SendPredictInvalidDatesRepository,
                 is_send_predict: bool = False):
        self._repository = repository
        self._is_send_predict = is_send_predict
        self._send_predict_invalid_dates_repo = send_predict_invalid_dates_repo

    def save_data(self, data_invalid_date: DataInvalidDates, data: list):

        # Get the model class for the repository
        model_class = model_mapping.get(type(self._repository))
        # Determine the date field key based on the model class
        date_field = "nav_date" if model_class == FundNavDaily else "date"
        saved_dates = []
        print(data)
        data_sorted = sorted(data, key=lambda x: x[date_field])
        print(f"Data sorted: {data_sorted}")
        for entry in data:
            try:
                # Convert the date field to datetime
                entry[date_field] = datetime.strptime(entry[date_field], '%Y-%m-%d')

                # Add additional fields if dealing with FundNavDaily
                if model_class == FundNavDaily:
                    entry["name"] = data_invalid_date.name
                    entry["proj_id"] = data_invalid_date.proj_id

                # Create or update the record
                if (self._is_send_predict):
                    self._send_predict_invalid_dates_repo \
                        .create_or_update(SendPredictInvalidDates(proj_id=data_invalid_date.proj_id, 
                                                                  nav_date=entry[date_field], 
                                                                  name=data_invalid_date.name,
                                                                  is_predict=bool(False),
                                                                  is_data_send=bool(False),
                                                                  is_predict_send=bool(False)))
                self._repository.create_or_update(self._repository.model_class(**entry))
                saved_dates.append(entry[date_field])

            except DatabaseException as e:
                # Handle invalid dates
                self._handle_invalid_date(entry, date_field, data_invalid_date)
                print(e)
                raise e
        return saved_dates
    def _handle_invalid_date(self, entry: dict, date_field: str, data_invalid_date: DataInvalidDates):
        try:
            # แปลงวันที่ให้เป็น datetime
            invalid_date = entry[date_field] if isinstance(entry[date_field], datetime) else datetime.strptime(entry[date_field], '%Y-%m-%d')
            
            # แปลง data_invalid_date.invalid_date เป็น datetime ก่อนลบ
            invalid_date_dt = datetime.combine(data_invalid_date.invalid_date, datetime.min.time())

            # อัปเดตข้อมูลวันที่ไม่ถูกต้อง
            data_invalid_date_repo = DataInvalidDatesRepository(self._repository.database())
            data_invalid_date_repo.update_invalid_date(data_invalid_date.name, "invalid_date", (invalid_date - invalid_date_dt).days)

            # ลบข้อมูลที่ผิดพลาด
            existing_send_predict_invalid_date = self._send_predict_invalid_dates_repo.get(
                SendPredictInvalidDates(proj_id=data_invalid_date.proj_id, nav_date=invalid_date)
            )
            if existing_send_predict_invalid_date:
                self._send_predict_invalid_dates_repo.delete(existing_send_predict_invalid_date)

        except Exception as ex:
            print(f"Error handling invalid date: {ex}")
            raise ex

    # def _handle_invalid_date(self, entry: dict, date_field: str, data_invalid_date: DataInvalidDates):
    #     try:
    #         # Parse invalid date and set it in the data_invalid_date object
    #         if isinstance(entry[date_field], datetime):
    #             invalid_date = entry[date_field]
    #         else:
    #             invalid_date = datetime.strptime(entry[date_field], '%Y-%m-%d')

    #         # Update the invalid date record in the repository
    #         data_invalid_date_repo = DataInvalidDatesRepository(self._repository.database())
    #         data_invalid_date_repo.update_invalid_date(data_invalid_date.name, "invalid_date", (invalid_date - data_invalid_date.invalid_date).days)
            
    #         existing_send_predict_invalid_date = self._send_predict_invalid_dates_repo.get(SendPredictInvalidDates(proj_id=data_invalid_date.proj_id, nav_date=invalid_date))
    #         if existing_send_predict_invalid_date is not None:
    #             self._send_predict_invalid_dates_repo.delete(existing_send_predict_invalid_date)
            
    #     except Exception as ex:
    #         print(f"Error handling invalid date: {ex}")
    #         raise ex
        # try:
        #     # Parse invalid date and set it in the data_invalid_date object
        #     if isinstance(entry[date_field], datetime):
        #         data_invalid_date.invalid_date = entry[date_field]
        #     else:
        #         data_invalid_date.invalid_date = datetime.strptime(entry[date_field], '%Y-%m-%d')

        #     # Update the invalid date record in the repository
        #     data_invalid_date_repo = DataInvalidDatesRepository(self._repository.database())
        #     data_invalid_date_repo.update(data_invalid_date)
        # except Exception as ex:
        #     print(f"Error handling invalid date: {ex}")
        #     raise ex

            