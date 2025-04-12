import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, requests
import yfinance as yf

from utils.func import *


from utils.indicator_processor import IndicatorProcessor

from adapters.database.mysql_database import MysqlDatabase

from adapters.repositories.prediction_ml_repository import PredictionMLRepository
from adapters.models.prediction_ml_model import PredictionML

from adapters.repositories.data_invalid_dates_repository import DataInvalidDatesRepository
from adapters.models.data_invalid_dates_model import DataInvalidDates

from adapters.repositories.send_predict_invalid_dates_repository import SendPredictInvalidDatesRepository
from adapters.models.send_predict_invalid_dates_model import SendPredictInvalidDates


from adapters.exceptions import DatabaseException
class SaveDataMLService:
    def __init__(self, prediction_ml_repo: PredictionMLRepository,
                 send_predict_invalid_dates_repo: SendPredictInvalidDatesRepository,
                 ):
        self._prediction_ml_repository = prediction_ml_repo
        self._send_predict_invalid_dates_repository = send_predict_invalid_dates_repo

    def save_data(self, prediction_ml: PredictionML):
        try:
            proj_id = prediction_ml.proj_id
            nav_date = prediction_ml.nav_date
            self._prediction_ml_repository.create_or_update(prediction_ml)
            self._send_predict_invalid_dates_repository.update_is_predict(proj_id,nav_date, True)
        except DatabaseException as e:
            print(e)
            self._handle_invalid_date(proj_id,nav_date, "nav_date")
            raise e
        
        # saved_dates = []
        # data = sorted(data, key=lambda x: x["nav_date"])
        # for entry in data:
        #     try:
        #         # Convert the date field to datetime
        #         entry["nav_date"] = datetime.strptime(entry["nav_date"], '%Y-%m-%d')

        #         # Add additional fields if dealing with PredictionML
        #         entry["name"] = data_invalid_date.name
        #         entry["proj_id"] = data_invalid_date.proj_id

        #         # Create or update the record
        #         self._prediction_ml_repository.create_or_update(self._prediction_ml_repository.model_class(**entry))
        #         saved_dates.append(entry["nav_date"])

        #     except DatabaseException as e:
        #         # Handle invalid dates
        #         self._handle_invalid_date(entry, "nav_date", data_invalid_date)
        #         print(e)
        #         raise e
        # data_invalid_date.ml_invalid_date = data_invalid_date.ml_invalid_date + timedelta(days=1)
        # self._data_invalid_dates_repository.update(data_invalid_date)
        # return saved_dates
    
    def _handle_invalid_date(self, proj_id,nav_date, date_field: str):
        try:
            self._send_predict_invalid_dates_repository.update_is_predict(proj_id, nav_date, False)
        except Exception as ex:
            print(f"Error handling invalid date: {ex}")
            raise ex