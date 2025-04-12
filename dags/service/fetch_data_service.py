import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json, requests
import yfinance as yf

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

from adapters.exceptions import DatabaseException

import time

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class FetchDataService:
    def __init__(self, data_invalid_dates_repo: DataInvalidDatesRepository):
        self._data_invalid_dates_repository = data_invalid_dates_repo

    def fetch_data(self, data_invalid_date: DataInvalidDates,
                   execution_date: datetime) -> list:
        invalid_date = datetime.combine(data_invalid_date.invalid_date, datetime.min.time())
        data_all = []
        print(f"Fetching data for({data_invalid_date.name}) from {invalid_date} to {execution_date}")
        if f'{data_invalid_date.name}' == 'SET':
            fetch_func = self._fetch_set_data
        elif f'{data_invalid_date.name}' == 'YTM':
            fetch_func = self._fetch_ytm_data
        else:
            fetch_func = self._fetch_fund_nav_daily_data
        
        print(f"Fetching data for {data_invalid_date.name} from {invalid_date} to {execution_date}")
        update_days = 0
        while invalid_date <= execution_date:
            # data_all.extend(fetch_func(data_invalid_date, invalid_date, execution_date))
            daily_data = fetch_func(data_invalid_date, invalid_date, execution_date)
            if daily_data:
                data_all.extend(daily_data)
                update_days += 1
            invalid_date += timedelta(days=1)
        # self._data_invalid_dates_repository.update(data_invalid_date)
        data_invalid_date.invalid_date += timedelta(days=update_days)
        self._data_invalid_dates_repository.update_invalid_date(data_invalid_date.name, "invalid_date", update_days)
        return data_all
     
    def _fetch_fund_nav_daily_data(self,
                    data_invalid_date: DataInvalidDates,
                    current_date: datetime,
                    execute_date: datetime) -> list:
        print(f'json config {data_invalid_date.json_config}')
        api_config = json.loads(data_invalid_date.json_config)
        headers = {
        "Ocp-Apim-Subscription-Key": f"{api_config['Ocp-Apim-Subscription-Key']}"
        }
        nav_date = current_date.strftime('%Y-%m-%d')
        api_url = api_config['url'].format(proj_id=data_invalid_date.proj_id, nav_date=nav_date)
        return self._fetch_via_api(api_url, headers, current_date, data_invalid_date, execute_date)
    
    def _fetch_set_data(self,
                        data_invalid_date: DataInvalidDates,
                        current_date: datetime,
                        execute_date: datetime) -> list:
        df = yf.download('^SET.BK',
                         start=current_date.strftime('%Y-%m-%d'),
                         end=(current_date + timedelta(days=1)).strftime('%Y-%m-%d')
                        )
        if df.empty:
            print(f"No SET data available for {data_invalid_date.name} on {current_date}. Skipping...")
            return []
        data = [{
            'date': current_date.strftime('%Y-%m-%d'),
            'open': df.iloc[0][('Open', '^SET.BK')].item(),
            'high': df.iloc[0][('High', '^SET.BK')].item(),
            'low': df.iloc[0][('Low', '^SET.BK')].item(),
            'close': df.iloc[0][('Close', '^SET.BK')].item(),
        }]
        # data_invalid_date.invalid_date = (current_date + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Data fetched for {data_invalid_date.proj_id} ({data_invalid_date.name}) on {current_date}") 
        return data

    def _fetch_ytm_data(self,
                        data_invalid_date: DataInvalidDates,
                        current_date: datetime,
                        execute_date: datetime) -> list:
        date = current_date.strftime('%Y-%m-%d')
        api_url = data_invalid_date.data_source.format(formatted_date=date)
        data = self._fetch_via_api(api_url, headers={}, current_date=current_date, data_invalid_date=data_invalid_date, execute_date=execute_date)
        if not data:
            return []
        filtered = [item for item in data if item['TtmGroupName'] == "MTM Corporate Bond Index (A- up) "]
        processed = [{'date': item['Asof'], 'ytm': item['AvgYTMIndex']} for item in filtered]
        sorted_data = sorted(
            processed,
            key=lambda x: datetime.strptime(x['date'], '%Y-%m-%dT%H:%M:%S')  # ฟอร์แมตตรงกับข้อมูล
        )
        for item in sorted_data:
            item['date'] = datetime.strptime(item['date'], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')
        return sorted_data

    def _fetch_via_api(self, api_url: str, headers: dict, current_date: datetime, 
                       data_invalid_date, execute_date: datetime) -> list:
        session = requests.Session()
        
        # ตั้งค่า retry policy
        retries = Retry(
            total=10,                # ลองใหม่สูงสุด 10 ครั้ง
            backoff_factor=2,        # รอแบบ exponential: 2, 4, 8, 16 วินาที...
            status_forcelist=[500, 502, 503, 504, 429],  # รีเทรเมื่อพบ error เหล่านี้
            allowed_methods=["GET"]
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))

        attempt = 0
        while attempt < 10:  # จำกัดจำนวน retry
            try:
                response = session.get(api_url, headers=headers, timeout=30)

                if response.status_code == 200 and response.json():
                    data = response.json()
                    print(f"✅ Data fetched for {data_invalid_date.proj_id} ({data_invalid_date.name}) on {current_date}")
                    return data
                
                elif response.status_code == 204:
                    print(f"⚠️ No data available for {data_invalid_date.proj_id} ({data_invalid_date.name}) on {current_date}. Skipping...")
                    return []

                elif response.status_code == 421:  # API rate limit
                    retry_after = int(response.headers.get("Retry-After", 5))  # ค่า default 5 วินาที
                    wait_time = min(60, retry_after * (2 ** attempt))  # จำกัดสูงสุดที่ 60 วินาที
                    print(f"🚨 Rate limit exceeded. Retrying after {wait_time} seconds...")
                    time.sleep(wait_time)

                else:
                    print(f"❌ Unexpected error {response.status_code}: {response.text}")
                    raise Exception(f"Failed to fetch data for {data_invalid_date.proj_id} ({data_invalid_date.name}) on {current_date}")

            except requests.exceptions.RequestException as e:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"🔄 Connection error: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

            attempt += 1
        
        print(f"🚫 Failed to fetch data after {attempt} retries.")
        return []    