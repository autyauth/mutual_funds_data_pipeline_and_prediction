from adapters.models.send_predict_invalid_dates_model import SendPredictInvalidDates
from adapters.repositories.send_predict_invalid_dates_repository import SendPredictInvalidDatesRepository
from dto.nav_history_dto import NavHistoryDTO
from dto.predtiction_trend_funds_dto import PredictionTrendFundsDTO
from adapters.repositories.Irepository import IRepository
from adapters.repositories.fund_nav_daily_repository import FundNavDailyRepository
from adapters.models.fund_nav_daily_model import FundNavDaily

from adapters.repositories.prediction_ml_repository import PredictionMLRepository
from adapters.models.prediction_ml_model import PredictionML



import requests, re
from datetime import datetime, timedelta
class SendDataService:
    def __init__(self):
        pass
    def send_data(self,
                  name:str,
                  proj_id:str,
                  repo: FundNavDailyRepository,
                  send_predict_invalid_date_repo: SendPredictInvalidDatesRepository,
                  config_backend: dict,):
        # send_data_invalid_date เป็น datetime ที่ระบุว่าวันล่าสุดต้องส่งเป็นวันไหน
        # เราต้องดึงตึ้งแต่วัน send_data_invalid_date ถึง execution_date
        data_sned_list = send_predict_invalid_date_repo.get_by_is_send_data(proj_id,
                                                                       False)
        data_list = []
        for data_sned in data_sned_list:
            data = repo.get(FundNavDaily(
                proj_id=proj_id,
                name=name,
                nav_date=data_sned.nav_date
            ))
            if data is not None:
                data_list.append(data.to_dict())

        data_dto_list = []
        
        for entry in sorted(data_list, key=lambda x: x['nav_date']):
            print(f"Sending data: {entry}")
            prev_last_val = repo.prev_last_val(FundNavDaily(
                proj_id=proj_id,
                name=name,
                nav_date=entry['nav_date'],
            )).last_val
            change = float(entry['last_val']) - float(prev_last_val) if prev_last_val is not None else None
            
            if re.search(r"RMF\b", name):  # \b คือ word boundary ป้องกัน False Positive
                fund_type = "RMF"
            elif re.search(r"SSF\b", name):
                fund_type = "SSF"
            elif re.search(r"THAIESG\b", name):
                fund_type = "THAIESG"
            else:
                fund_type = ""
            data_dto = NavHistoryDTO(
                fund_name=name,
                # date=entry['nav_date'].strftime('%Y-%m-%d'),
                date=datetime.strftime(entry['nav_date'], '%Y-%m-%d'),
                nav=entry['last_val'],
                fund_type=fund_type,
                selling_price=entry['sell_price'],
                redemption_price=entry['buy_price'],
                total_net_assets=entry['net_asset'],
                change=change
            )
            data_dto_list.append(data_dto)
        print(f"Data DTOs: {data_dto_list}")
        # ส่งข้อมูลไปยัง backend
        nav_history_config = config_backend['nav_history']
        headers = {
            "Authorization": f"Bearer {self._login(config_backend['login'])}"
        }
        
        for data_dto in data_dto_list:
            try:
                response = requests.post(nav_history_config['url'], json=data_dto.to_dict(), headers=headers)
                response.raise_for_status()
                if response.status_code != 201:
                    print(f"Failed to send data to backend for proj_id ({name})")
                response_data = response.json()
                send_predict_invalid_date_repo.update_is_data_send(proj_id, datetime.strptime(data_dto.date, "%Y-%m-%d") , True)
                print(f"Data sent to backend for proj_id ({name}) response_data: {response_data}")
            except Exception as e:
                print(f"Failed to send data for date {data_dto.date}: {str(e)}")
                raise e
    
    def send_pred_data(self,
                    name: str,
                    proj_id: str,
                    repo: PredictionMLRepository,
                    send_predict_invalid_date_repo: SendPredictInvalidDatesRepository,
                    config_backend: dict,
                  ):
        # 🔹 Dictionary Mapping สำหรับกองทุนที่ต้องการ Duplicate Data
        fund_mapping = {
            "B-INNOTECHRMF": "B-INNOTECHSSF",
        }

        # ดึงข้อมูลของกองทุนหลัก
        data_send_list = send_predict_invalid_date_repo.get_by_multi_is_(proj_id, True, True, False)
        data_list = []
        

        for data_send in data_send_list:
            data = repo.get(PredictionML(
                proj_id=proj_id,
                nav_date=data_send.nav_date
            ))
            print(f'proj_id: {proj_id} ,nav_date: {data_send.nav_date}')
            print(f'data: {data}')
            if data is not None:
                data_list.append(data.to_dict())

        data_dto_list = []

        for entry in sorted(data_list, key=lambda x: x['nav_date']):
            print(f"Sending data: {entry}")
            
            # ✅ สร้าง DTO ของกองทุนหลัก
            main_dto = PredictionTrendFundsDTO(
                fund_name=name,  
                date=datetime.strftime(entry['nav_date'], '%Y-%m-%d'),
                trend=entry['trend'],
                up_trend_prob=entry['up_trend_prob'],
                down_trend_prob=entry['down_trend_prob'],
                reason="",
                indicator="",
            )
            data_dto_list.append(main_dto)

            # ✅ ถ้ากองทุนอยู่ใน `fund_mapping` ให้ Duplicate Data ไปยังกองทุนที่กำหนด
            if name in fund_mapping:
                duplicate_fund = fund_mapping[name]  # ดึงชื่อกองทุนปลายทางจาก mapping

                duplicate_dto = PredictionTrendFundsDTO(
                    fund_name=duplicate_fund,  
                    date=main_dto.date,  # ใช้วันเดียวกัน
                    trend=main_dto.trend,  
                    up_trend_prob=main_dto.up_trend_prob,  
                    down_trend_prob=main_dto.down_trend_prob,  
                    reason="",  
                    indicator="",  
                )
                data_dto_list.append(duplicate_dto)

        print(f"Data DTOs: {data_dto_list}")
        
        # ✅ ส่งข้อมูลไปยัง backend
        pred_send_config = config_backend['prediction_trend_funds']
        headers = {
            "Authorization": f"Bearer {self._login(config_backend['login'])}"
        }
        
        for data_dto in data_dto_list:
            try:
                response = requests.post(pred_send_config['url'], json=data_dto.to_dict(), headers=headers)
                response.raise_for_status()
                if response.status_code != 201:
                    print(f"Failed to send data to backend for fund ({data_dto.fund_name})")
                response_data = response.json()
                send_predict_invalid_date_repo.update_is_predict_send(proj_id, datetime.strptime(data_dto.date, "%Y-%m-%d"), True)
                print(f"Data sent to backend for fund ({data_dto.fund_name}) response_data: {response_data}")
            except Exception as e:
                print(f"Failed to send data for date {data_dto.date}: {str(e)}")
                raise e
        
    def _login(self, login_config: dict):
        login_response = requests.post(login_config['url'], json={
            'username': login_config['username'],
            'password': login_config['password']
        })
        return login_response.json()['access_token']
    
    