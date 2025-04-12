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
from service.fetch_data_preprocessing_service import FetchDataPreprocessingService

from utils.model_enum import ModelEnum

from utils.model_mapping import model_mapping

from dto.nav_history_dto import NavHistoryDTO


from sklearn.preprocessing import StandardScaler
import pickle
# ตั้งค่า timezone ให้เป็น UTC+7
local_tz = timezone("Asia/Bangkok")

# Default arguments สำหรับ DAG
default_args = {
    'owner': 'mafia',
    'depends_on_past': False,
    'retries': 11,
    'retry_delay': timedelta(hours=2),
}
def create_table_add_default_data_if_not_exists(database, data_path_repo_dict, **kwargs):
    """
    ตรวจสอบและสร้างตารางในฐานข้อมูลหากยังไม่มี และเพิ่มข้อมูล MutualFund
    จากไฟล์ CSV ไปยังฐานข้อมูล
    """
    # ตรวจสอบสถานะผ่าน Variable

    if bool(Variable.get("fund_tables_created", default_var="[]", deserialize_json=True)['is_created']):
        print("Tables already created. Skipping...")
        return

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
     
def fetch_data_invalid_days(data_invalid_dates: DataInvalidDates, 
                            data_invalid_dates_repo: DataInvalidDatesRepository,
                            **kwargs):
    """ดึงข้อมูลตั้งแต่วันที่ invalid_date ถึง execution_date"""
    execution_date = kwargs['execution_date'].replace(tzinfo=None)
    # execution_date = kwargs['data_interval_end'].replace(tzinfo=None)
    fetch_data_service = FetchDataService(data_invalid_dates_repo)
    data = fetch_data_service.fetch_data(data_invalid_dates, execution_date)
    print(f"Data fetched for proj_id ({data_invalid_dates.name})")
    print(f"Data: {data}")
    kwargs['ti'].xcom_push(key=f'{data_invalid_dates.name}', value=data)  # เก็บข้อมูลทั้งหมดใน XCom
    
# ฟังก์ชันสำหรับบันทึกข้อมูลลงในฐานข้อมูล
def save_data_to_db(data_invalid_date: DataInvalidDates, 
                    repo: IRepository,
                    send_predict_invalid_dates_repo: SendPredictInvalidDatesRepository,
                    is_send_predict: bool = False,
                    **kwargs):
    """บันทึกข้อมูลย้อนหลังลงในฐานข้อมูล"""
    data = kwargs['ti'].xcom_pull(key=f'{data_invalid_date.name}')
    

    print(f"Saving data for proj_id ({data_invalid_date.name})")
    print(f"Data: {data}")
    if not data:
        print(f"No data to save for proj_id ({data_invalid_date.name})")
        return  # ไม่มีข้อมูล ไม่ต้องบันทึกอะไร
    print(f"repo: {repo}")
    print(f"data: {data}")
    save_data_servicfe = SaveDataService(repo,send_predict_invalid_dates_repo, is_send_predict)
    dates = save_data_servicfe.save_data(data_invalid_date, data)
    print(f"Data saved for proj_id ({data_invalid_date.name})")
    print(f"Dates: {dates}")
    kwargs['ti'].xcom_push(key=f'save data dates {data_invalid_date.name}', value=dates)  # เก็บวันที่ที่บันทึกไว้ใน XCom
    
def send_nav_to_backend(data_invalid_date: DataInvalidDates,
                        repo: IRepository,
                        config_backend: dict,
                        send_predict_invalid_dates_repo: SendPredictInvalidDatesRepository,
                        **kwargs):
    
    print(f"Sending data to backend for proj_id ({data_invalid_date.name})")
    send_data_service = SendDataService()
    send_data_service.send_data(name=data_invalid_date.name,
                                proj_id=data_invalid_date.proj_id,
                                repo=repo,
                                send_predict_invalid_date_repo=send_predict_invalid_dates_repo,
                                config_backend=config_backend)
    print(f"Data sent to backend for proj_id ({data_invalid_date.name})")
    
def preprocessing_data_and_fetch_data(config,
                                      repo_dict: dict,
                                      send_predict_invalid_dates_repo: SendPredictInvalidDatesRepository,
                                      model_name: str,
                                        **kwargs):
    proj_id = config.get('proj_id')
    repo_list = [repo_dict.get(feature) for feature in config.get('input_features', [])]
    scaler = pickle.load(open(f'./dags/ml_scaler/scaler_{model_name}.pkl', 'rb'))
    fetch_data_preprocessing_service = FetchDataPreprocessingService(config, send_predict_invalid_dates_repo, scaler)
    data_dict = fetch_data_preprocessing_service.fetch_data_preprocessed_data(repo_dict)
    print(f"Data fetched for proj_id for ml ({proj_id}) \n{data_dict}")
    kwargs['ti'].xcom_push(key=f'processed_data_ml {proj_id}', value=data_dict)

def predict_trend(config,
                  model_name,
                  prediction_repo:PredictionMLRepository,
                  send_predict_invalid_dates_repo: SendPredictInvalidDatesRepository,
                  **kwargs):
    proj_id = config.get('proj_id')

    preprocessed_data = kwargs['ti'].xcom_pull(key=f'processed_data_ml {proj_id}')
    
    print(f"Predicting trend for model {model_name}...")
    model_path = f'./dags/ml_model/{model_name}_model.pkl'
    model = pickle.load(open(model_path, 'rb'))
    save_ml_service = SaveDataMLService(prediction_repo, send_predict_invalid_dates_repo)
    for date, data in preprocessed_data.items():
        X = np.array(data)
        y_pred = model.predict(X)
        y_pred_proba = model.predict_proba(X)
        print(f"Predicted trend for {model_name} on {date}: {y_pred[0]}, up_trend_prob: {y_pred_proba[0][1]}, down_trend_prob: {y_pred_proba[0][0]}")
        pred_ml_model = PredictionML(
                proj_id=proj_id,
                name=model_name,
                nav_date=datetime.strptime(date, "%Y-%m-%d"),
                trend=str(y_pred[0]),
                up_trend_prob=float(y_pred_proba[0][1]),
                down_trend_prob=float(y_pred_proba[0][0])
            )
        save_ml_service.save_data(
            pred_ml_model
        )
        print(f"Predicted trend saved for model {model_name} proj_id ({proj_id}, date: {date}")

def send_predict_trend_to_backend(name:str,
                                  proj_id:str,
                                  pred_repo: PredictionMLRepository,
                                  config_backend: dict,
                                  send_predict_invalid_dates_repo: SendPredictInvalidDatesRepository,
                                  **kwargs
                                ):
    print(f"Sending predicted trend to backend...")
    send_data_service = SendDataService()
    send_data_service.send_pred_data(
        name=name,
        proj_id=proj_id,
        repo=pred_repo,
        config_backend=config_backend,
        send_predict_invalid_date_repo=send_predict_invalid_dates_repo
    )
    print(f"Predicted trend sent to backend for proj_id ({name})")

    
with DAG(
    dag_id='fund_daily_send_data_dag',
    default_args=default_args,
    description='Workflow for Fund Daily NAV',
    schedule_interval="0 17 * * *", # 06.00 , 23.00 mon-fri 
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),

    max_active_runs=1,
    catchup=True,
) as dag:
    mysql_uri = "mysql+pymysql://airflow_user:airflow_password@mysql:3306/airflow_db"
    database = MysqlDatabase(uri=mysql_uri)
    
    data_invalid_dates_repo = DataInvalidDatesRepository(database=database)
    fund_nav_daily_repo = FundNavDailyRepository(database=database)
    set_repo = SETRepository(database=database)
    ytm_repo = YTMRepository(database=database)
    send_predict_invalid_dates_repo = SendPredictInvalidDatesRepository(database=database)
    prediction_ml_repo = PredictionMLRepository(database=database)
    
    
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
    # create_table_task
    
    with open('/opt/airflow/config/ml_preprocessing_config_final.yaml', 'r') as f:
        config_ml = yaml.safe_load(f)
    
    with open('/opt/airflow/config/backend_api.json', 'r') as f:
        config_backend = json.load(f)
    fund_api_send_name = [fund['name'] for fund in config_backend['nav_history']['fund_send']]
    records = []
    for model_name, model_config in config_ml["models"].items():
        record = {"model_name": model_name, **model_config}
        records.append(record)

    config_ml_df = pd.DataFrame(records)
    config_ml_df.set_index('model_name', inplace=True)
    
    save_tasks = {}
    
    data_invalid_dates = data_invalid_dates_repo.get_all()
    for proj in data_invalid_dates:
        repo = data_path_repo_dict.get(f'{proj.name}'.lower(), (fund_nav_daily_repo,))[0]
        fetch_data_task = PythonOperator(
            task_id=f'fetch_data_{proj.name}',
            python_callable=fetch_data_invalid_days,
            op_args=[proj, data_invalid_dates_repo],
            provide_context=True,
        )
        save_data_task = PythonOperator(
            task_id=f'save_data_{proj.name}',
            python_callable=save_data_to_db,
            op_args=[proj, repo,send_predict_invalid_dates_repo, proj.name in fund_api_send_name],
            provide_context=True,
        )
        create_table_task >> fetch_data_task >> save_data_task 
        save_tasks[f'{proj.name}'] = save_data_task
        if proj.name in fund_api_send_name:
            send_nav_to_backend_task = PythonOperator(
                task_id=f'send_nav_to_backend_{proj.name}',
                python_callable=send_nav_to_backend,
                op_args=[proj, repo, config_backend,send_predict_invalid_dates_repo],
                provide_context=True,
            )
            save_data_task >> send_nav_to_backend_task
    
    for task_id, task in save_tasks.items():
        if task_id not in config_ml_df.index:
            continue
        
        inputs = config_ml_df.loc[task_id]["input_features"]
        proj_id_ml = config_ml_df.loc[task_id]["proj_id"]
        fetch_data_preprocessing_task = PythonOperator(
            task_id=f'fetch_data_preprocessing_{task_id}',
            python_callable=preprocessing_data_and_fetch_data,
            op_args=[config_ml_df.loc[task_id], repo_dict, send_predict_invalid_dates_repo, task_id],
            provide_context=True,
        )
        predict_trend_task = PythonOperator(
            task_id=f'predict_trend_{task_id}',
            python_callable=predict_trend,
            op_args=[config_ml_df.loc[task_id],task_id,prediction_ml_repo,send_predict_invalid_dates_repo],
            provide_context=True,
        )
        send_predict_trend_to_backend_task = PythonOperator(
            task_id=f'send_predict_trend_to_backend_{task_id}',
            python_callable=send_predict_trend_to_backend,
            op_args=[task_id, proj_id_ml, prediction_ml_repo, config_backend, send_predict_invalid_dates_repo],
            provide_context=True,
        )
            
            
        for input_feature in inputs:
            if input_feature == 'nav':
                pass
            elif input_feature == 'set':
                save_tasks['SET'] >> fetch_data_preprocessing_task >> predict_trend_task >> send_predict_trend_to_backend_task
            elif input_feature == 'ytm':
                save_tasks['YTM'] >> fetch_data_preprocessing_task >> predict_trend_task >> send_predict_trend_to_backend_task
        save_tasks[f'{task_id}'] >> fetch_data_preprocessing_task >> predict_trend_task >> send_predict_trend_to_backend_task