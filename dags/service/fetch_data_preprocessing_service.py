from adapters.repositories.Irepository import IRepository
from adapters.repositories.send_predict_invalid_dates_repository import SendPredictInvalidDatesRepository
from adapters.models.send_predict_invalid_dates_model import SendPredictInvalidDates
from adapters.models.fund_nav_daily_model import FundNavDaily
from utils.model_mapping import model_mapping
from utils.indicator_processor_update import IndicatorProcessorUpdate
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


class FetchDataPreprocessingService:
    def __init__(self, config, send_predict_invalid_dates_repository: SendPredictInvalidDatesRepository, scaler):
        self._config = config
        self._send_predict_invalid_dates_repository = send_predict_invalid_dates_repository
        self._scaler = scaler

    def fetch_data_preprocessed_data(self, repo_dict: dict):
        proj_id = str(self._config.get('proj_id'))
        time_step = int(self._config.get('time_step'))
        feature_engineering = self._config.get('feature_engineering')
        is_not_predict_proj = self._send_predict_invalid_dates_repository.get_by_is_predict(proj_id, False)
        print(f'is_not_predict_proj: {is_not_predict_proj}')
        data_dict = {}
        if is_not_predict_proj is None:
            return data_dict
        date_preprocessing_list = [date.nav_date for date in is_not_predict_proj]
        processor = IndicatorProcessorUpdate()
        return_dict = {}
        for date in date_preprocessing_list:
            date_str = datetime.strftime(date, "%Y-%m-%d")
            data_dict[date_str] = {}
            for feature in feature_engineering:
                feature_name = feature["name"]
                input_name = feature["input"]
                repo = repo_dict[input_name]
                
                model_class = model_mapping.get(type(repo))
                if model_class == FundNavDaily:
                    model = FundNavDaily(proj_id=proj_id, nav_date=date)
                else:
                    model = model_class(date=date)
                data_from_todate = repo.get_recent_from_end_date(model,date, time_step)
                # change input_name to nav
                data_from_todate = [{'date': data['date'], feature_name: data[input_name]} for data in data_from_todate]
                ## data_from_todate format 
                ## [{'date': '2021-01-01', input_name: 100}]
                print(f'data_from_todate: {data_from_todate}')
                
                # data_from_todate_df = pd.DataFrame(data_from_todate)
                # print (f'data from todate before indicator: {data_from_todate_df} ')
                # data_from_todate_df.set_index('date', inplace=True)
                for step in feature["chain"]:
                    function_name = step["function"]
                    print(f'function_name: {function_name}')
                    params = step.get("params", {})
                    if function_name == "":
                        continue
                    if function_name == "pct_change":
                        # ดึง data 2 ชุด จุดแรกจำนวน time step จุดที่สองจำนวน time step 
                        print(f'periods: {params["periods"]}, date: {date}, time_step: {time_step}')
                        print(f'type : {type(params["periods"])}, {type(date)}, {type(time_step)}')
                        prev_data = repo.get_recent_from_back_days_and_end_date(model, date, params["periods"], time_step)
                        print(f'prev_data: {prev_data}')
                        if prev_data is None or len(prev_data) != len(data_from_todate):
                            continue
                        # prev_data_df = pd.DataFrame(prev_data)
                        # prev_data_df.set_index('date', inplace=True)
                        # data_from_todate_df = processor.pct_change(data_from_todate_df, prev_data_df)
                        for i in range(len(data_from_todate)):
                            data_from_todate[i][feature_name] = processor.pct_change(data_from_todate[i][feature_name], prev_data[i][input_name])
                        print(f'data after pct_change {data_from_todate}')
                    elif function_name in processor.function_map:
                        # data, date, params, last_values_path, is_update_last_values=False
                        data_list = []
                        for data in data_from_todate:
                            data_ = processor.function_map[function_name](
                                data[feature_name], data['date'], params, step["last_values_path"], data['date'] == date_str
                            )
                            data_list.append({'date': data['date'], feature_name: data_})
                            print(f'data date: is {data["date"]}, {feature_name} is {data_}, {data['date'] == date_str}  ')
                        data_from_todate = data_list
                        # for date_df in data_from_todate_df.index:
                        #     data_list.append({date_df,
                        #         processor.function_map[function_name](
                        #         data_from_todate_df.squeeze()[date_df], date_df, params, step["last_values_path"], date_df == date)})
                        # data_from_todate_df = pd.DataFrame(data_list)
                        # data_from_todate_df.set_index('date', inplace=True)
                        # print(f'data after {function_name} {data_from_todate_df}')
                
                data_dict[date_str][feature_name] = data_from_todate  
            df_new = pd.DataFrame({feature_name: {entry['date']: entry[feature_name] for entry in values} for feature_name, values in data_dict[date_str].items()}).reset_index()
            df_new.rename(columns={'index': 'date'}, inplace=True)
            df_new.set_index('date', inplace=True)
            
            X = df_new.iloc[:, :]
            X_scaled = self._scaler.transform(X)
            data_3d = processor.reshape_data(X_scaled, time_step)
            return_dict[date_str] = data_3d.reshape(data_3d.shape[0], -1).tolist()
            
        return return_dict
            
                
                        