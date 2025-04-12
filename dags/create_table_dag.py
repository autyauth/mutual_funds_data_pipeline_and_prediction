from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta, date
import json
import requests
import pymysql
import holidays
from pendulum import timezone



import yaml
import pickle
from sklearn.preprocessing import StandardScaler
import xgboost as xgb

import pandas as pd
import numpy as np
import re
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

from adapters.repositories.prediction_ml_repository import PredictionMLRepository
from adapters.models.prediction_ml_model import PredictionML

# from adapters.repositories.ml_invalid_dates_repository import MLInvalidDatesRepository
# from adapters.models.ml_invalid_dates_model import MLInvalidDates

from adapters.repositories.send_predict_invalid_dates_repository import SendPredictInvalidDatesRepository
from adapters.models.send_predict_invalid_dates_model import SendPredictInvalidDates

from adapters.repositories.Irepository import IRepository


from adapters.exceptions import DatabaseException

from service.fetch_data_service import FetchDataService
from service.save_data_service import SaveDataService
from service.save_data_ml_service import SaveDataMLService
from service.send_data_service import SendDataService

from utils.model_enum import ModelEnum

from utils.model_mapping import model_mapping

from dto.nav_history_dto import NavHistoryDTO
# ตั้งค่า timezone ให้เป็น UTC+7
local_tz = timezone("Asia/Bangkok")


def create_table_add_default_data_if_not_exists(database, data_path_repo_dict, **kwargs):
    """
    ตรวจสอบและสร้างตารางในฐานข้อมูลหากยังไม่มี และเพิ่มข้อมูล MutualFund
    จากไฟล์ CSV ไปยังฐานข้อมูล
    """
    # ตรวจสอบสถานะผ่าน Variable

    database.create_tables_if_not_exists()
    for model_name, (repo, data_path) in data_path_repo_dict.items():
        print(f"Loading data for {model_name} from {data_path}...")
        data = read_csv_with_auto_delimiter(data_path)
        data = data.where(pd.notnull(data), None)
        models = []
        for _, row in data.iterrows():
            row_dict = row.to_dict()
            print(f"Row: {row_dict}")
            try:
                model = repo.model_class(**row_dict)
            except TypeError as e:
                print(f"Error creating model instance for {model_name}: {e}")
                raise
            models.append(model)
        repo.create_or_update_all(models)
    
    # บันทึกว่า task นี้ทำงานเสร็จแล้ว
    Variable.set("fund_tables_created", value={"is_created": True}, serialize_json=True)
    
default_args = {
    'owner': 'mafi1a',
    'depends_on_past': False,
    'retries': 11,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='create_table_dag',
    default_args=default_args,
    description='Create tables and add default data if not exists',
    schedule_interval=None,
):
    mysql_uri = "mysql+pymysql://airflow_user:airflow_password@mysql:3306/airflow_db"
    database = MysqlDatabase(uri=mysql_uri)
    
    data_invalid_dates_repo = DataInvalidDatesRepository(database=database)
    fund_nav_daily_repo = FundNavDailyRepository(database=database)
    set_repo = SETRepository(database=database)
    ytm_repo = YTMRepository(database=database)
    send_predict_invalid_dates_repo = SendPredictInvalidDatesRepository(database=database)
    
    
    data_path_repo_dict = {
        'data_invalid_dates': (data_invalid_dates_repo, '/opt/airflow/data/data_invalid_dates.csv'),
        'fund_nav_daily': (fund_nav_daily_repo, '/opt/airflow/data/fund_nav_daily.csv'),
        'set': (set_repo, '/opt/airflow/data/SET.csv'),
        'ytm': (ytm_repo, '/opt/airflow/data/ytm.csv'),
    }
    repo_dict = {
    'nav': fund_nav_daily_repo,
    'set': set_repo,
    'ytm': ytm_repo,
    }
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table_add_default_data_if_not_exists,
        op_args=[database,data_path_repo_dict],
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE
    )
    create_table_task
            