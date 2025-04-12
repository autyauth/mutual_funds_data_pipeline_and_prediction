import pandas_ta as ta
import numpy as np
import pandas as pd
import os
class IndicatorProcessorUpdate:
    def __init__(self):
        """
        selected_indicators: list ของ Indicator ที่ต้องการอัปเดต
        indicator_params: dict ของ parameter ที่กำหนดให้แต่ละ Indicator
        """
        self.function_map = {
            "pct_change": self.pct_change,
            "rsi": self.rsi,
            "cci": self.cci,
            "macd": self.macd,
            "macd_hist": self.macd_hist,
            "roc": self.pct_change,
            "di_minus": self.di_minus,
            "di_plus": self.di_plus,
            "ema": self.ema,
            "ema_diff": self.ema_diff,
            "atr": self.atr,
            "bb_middle": self.BB_middle,
            "ema_diff_sf": self.ema_diff_sf
        }

    def pct_change(self, data, prev_data): 
        pct_change = (data - prev_data) / prev_data
        return pct_change

    def rsi(self, data, date, params, last_values_path, is_update_last_values=False): # ok
        ## ดึง last_values จากไฟล์ CSV แล้วเอาตัวสุดท้ายทำเป็น dict ไปใช้
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        rsi = self.update_rsi(close_price=data, date=date, indicator_params=params, last_values=last_values)
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return rsi["RSI"]

    def cci(self, data, date, params, last_values_path, is_update_last_values=False): # ok
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        ## CCI ต้องใช้ค่า High, Low, Close ของวันก่อนหน้า ทำ tp ปัจจุบัน
        ## ต้องดึง tp ของวันก่อนหน้า period day มาใส่ใน last_values
        ## sma_tp, mean_dev ต้องเป็นของวันก่อนหน้า แต่จริงๆแล้วใน last_values ไม่ได้ใช้
        target_date = pd.to_datetime(date)
        periods = params.get("periods", 20)
        previous_days = df.loc[df["Date"] < target_date].nlargest(periods, "Date").sort_values("Date")

        tp = previous_days["tp"].tolist()
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        last_values_dict = {
            "tp": tp,
            "sma_tp": last_values.get("sma_tp", None),
            "mean_deviation": last_values.get("mean_deviation", None)
        }
        cci = self.update_cci(close_price=data, high_price=data, low_price=data, date=date, 
                              indicator_params=params, last_values=last_values_dict)
        last_values["tp"] = last_values_dict["tp"][-1]
        last_values["CCI"] = last_values_dict["CCI"]
        last_values["Date"] = date
        last_values['sma_tp'] = last_values_dict["sma_tp"]
        last_values['mean_deviation'] = last_values_dict["mean_deviation"]
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return cci["CCI"]

    def macd(self, data, date, params, last_values_path, is_update_last_values=False): # ok
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        macd = self.update_macd(close_price=data, date=date, indicator_params=params, last_values=last_values)
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return macd["MACD"]
        
    def macd_hist(self, data, date, params, last_values_path, is_update_last_values=False): # not use
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        macd = self.update_macd(close_price=data, date=date, indicator_params=params, last_values=last_values)
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return macd["MACD_Hist"]

    def di_minus(self, data, date, params, last_values_path, is_update_last_values=False): # ok
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        di_minus = self.update_di_minus(high_price=data, low_price=data, close_price=data, 
                                        date=date, indicator_params=params, last_values=last_values)
        print(f'lase_values:{last_values}')
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return di_minus["DI_Minus"]
    
    def di_plus(self, data, date, params, last_values_path, is_update_last_values=False): # ok
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        di_plus = self.update_di_plus(high_price=data, low_price=data, close_price=data, 
                                      date=date, indicator_params=params, last_values=last_values)
        print(f'lase_values:{last_values}')
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return di_plus["DI_Plus"]
    
    def ema(self, data, date, params, last_values_path, is_update_last_values=False): # ok
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        ema = self.update_ema(close_price=data, date=date, indicator_params=params, last_values=last_values)
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return ema["EMA"]
    
    def ema_diff(self, data, date, params, last_values_path, is_update_last_values=False):
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        ema = self.update_ema(close_price=data, date=date, indicator_params=params, last_values=last_values)
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return data - ema["EMA"]
    
    def atr(self, data, date, params, last_values_path, is_update_last_values=False): # ok
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        atr = self.update_atr(high_price=data, low_price=data, close_price=data, 
                              date=date, indicator_params=params, last_values=last_values)
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return atr["ATR"]
    
    def BB_middle(self, data, date, params, last_values_path, is_update_last_values=False):
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        periods = params.get("periods", 20)
        previous_days = df.loc[df["Date"] < target_date].nlargest(periods, "Date").sort_values("Date")

        BB_Middle = previous_days["Close"].tolist()
        last_values_dict = {
            "BB_Middle": BB_Middle
        }
        bb_middle = self.update_bollinger_bands(close_price=data, date=date, indicator_params=params, last_values=last_values_dict)
        last_values = {
            "Date": date,
            "Close": data,
        }
        if is_update_last_values:
            self.update_last_values_to_csv(last_values, last_values_path)
        return bb_middle["BB_Middle"]
        
    
    def ema_diff_sf(self, data, date, params, last_values_path, is_update_last_values=False):
        df = pd.read_csv(last_values_path, parse_dates=["Date"])
        df = df.drop_duplicates(keep="first")
        target_date = pd.to_datetime(date)
        last_values = df[df["Date"] == df.loc[df["Date"] < target_date, "Date"].max()].iloc[-1].to_dict()
        # ema = self.update_ema(close_price=data, date=date, indicator_params=params, last_values=last_values)
        # self.update_last_values_to_csv(last_values, last_values_path)
        fast_periods = params.get("fast", 12)
        fast_params = {"periods": fast_periods}
        fast_last_values = {
            "Date": last_values["Date"],
            "EMA": last_values[f"EMA_{fast_periods}"]
        }
        ema_fast = self.update_ema(data, date, fast_params, fast_last_values)['EMA']
        
        slow_periods = params.get("slow", 26)
        slow_params = {"periods": slow_periods}
        slow_last_values = {
            "Date": last_values["Date"],
            "EMA": last_values[f"EMA_{slow_periods}"]
        }
        ema_slow = self.update_ema(data, date, slow_params, slow_last_values)['EMA']
        
        new_last_values = {
            "Date": date,
            f"EMA_{fast_periods}": ema_fast,
            f"EMA_{slow_periods}": ema_slow
        }
        if is_update_last_values:
            self.update_last_values_to_csv(new_last_values, last_values_path)
        return ema_fast - ema_slow
    
    def reshape_data(self, data, time_steps):
        samples = len(data) - time_steps + 1
        reshaped_data = np.zeros((samples, time_steps, data.shape[1]))
        for i in range(samples):
            reshaped_data[i] = data[i:i + time_steps]
        return reshaped_data
    
    def preprocessing(self, df, features, time_step, scaler=None):
        feature_data = {}
        
        for feature in features:
            feature_name = feature["name"]
            processed_data = self.process_chain(df, feature)
            feature_data[feature_name] = processed_data
        combined_data = pd.DataFrame(feature_data)
        combined_data.dropna(inplace=True)
        combined_data.replace([np.inf, -np.inf], np.nan, inplace=True)
        combined_data.dropna(inplace=True)
        data = combined_data.iloc[:, :]
        print(f'data:{data}')
        print(f'data.shape:{data.shape}')
        if scaler is not None:
            scaled_data = scaler.fit_transform(data)
            print(f'scaled_data:{scaled_data}')
            print(f'scaled_data.shape:{scaled_data.shape}')
        else:
            scaled_data = data
        data_3d = self.reshape_data(scaled_data, time_step)
        data_2d = data_3d.reshape(data_3d.shape[0], -1)
        return data_2d

    def process_chain(self, df, feature):
        input_col = feature.get("input", "close")  # Default input column
        data = df[input_col]  # Start with the input column as the base data
        for step in feature["chain"]:
            function_name = step["function"]
            params = step.get("params", {})
            if function_name == "":
                continue
            if function_name in self.function_map:
                data = self.function_map[function_name](data, params)
                print(f'data sharpe after {function_name}: {data.shape}')
            else:
                raise ValueError(f"Function {function_name} is not supported.")
        return data
        
    def update_last_values_to_csv(self, last_values, path_last_values):
        """บันทึกค่า last_values ลงไฟล์ CSV"""
        file_exits = os.path.exists(path_last_values)
        df = pd.DataFrame([last_values])
        df.to_csv(path_last_values, index=False, mode='a', header=not file_exits)

    def update_macd(self, 
                    close_price, 
                    date, 
                    indicator_params,last_values,
                    ema_fast_name="ema_fast", 
                    ema_slow_name="ema_slow", 
                    macd_signal_name="MACD_signal", 
                    macd_name="MACD", 
                    macd_hist_name="MACD_Hist"
                ): #ok
        """คำนวณ MACD"""
        ## รับ last_values มาเพื่อใช้ค่า EMA fast, EMA slow, MACD Signal ก่อนหน้ามา แล้วแก้ไขจะเป็นการอัปเดตค่าล่าสุด
        ## คำนวณ EMA fast, EMA slow, MACD, MACD Signal, MACD Histogram
        ## ema fast, ema slow, macd signal วันปัจจุบันแบบ append ลง csv
        fast_period = indicator_params.get("fast", 12)
        slow_period = indicator_params.get("slow", 26)
        signal_period = indicator_params.get("signal", 9)

        # คำนวณ EMA_Fast
        if ema_fast_name not in last_values:
            last_values[ema_fast_name] = close_price
        else:
            alpha_fast = 2 / (fast_period + 1)
            last_values[ema_fast_name] = (close_price * alpha_fast) + (last_values[ema_fast_name] * (1 - alpha_fast))

        # คำนวณ EMA_Slow
        if ema_slow_name not in last_values:
            last_values[ema_slow_name] = close_price
        else:
            alpha_slow = 2 / (slow_period + 1)
            last_values[ema_slow_name] = (close_price * alpha_slow) + (last_values[ema_slow_name] * (1 - alpha_slow))

        # คำนวณ MACD
        macd = last_values[ema_fast_name] - last_values[ema_slow_name]

        # คำนวณ MACD Signal
        if macd_signal_name not in last_values:
            last_values[macd_signal_name] = macd
        else:
            alpha_signal = 2 / (signal_period + 1)
            last_values[macd_signal_name] = (macd * alpha_signal) + (last_values[macd_signal_name] * (1 - alpha_signal))

        # คำนวณ MACD Histogram
        macd_hist = macd - last_values[macd_signal_name]

        last_values["Date"] = date
        
        # self._save_last_values(self.path_last_values)

        return {"Date": date ,
                macd_name: macd,
                macd_signal_name: last_values[macd_signal_name], 
                macd_hist_name: macd_hist}

    def update_rsi(self,
                   close_price,
                   date,
                   indicator_params,
                   last_values,
                   gain_name='gains',
                   loss_name='losses',
                   prev_close_name='Close',
                   ): # ok
        """คำนวณ RSI โดยใช้ Wilder's Smoothing"""
        """คำนวณ RSI โดยใช้ Wilder's Smoothing และอัปเดตค่าล่าสุด"""
    
        period = indicator_params.get("periods", 14)

        # ถ้าเป็นการคำนวณครั้งแรก ให้ตั้งค่าเริ่มต้น
        if gain_name not in last_values or loss_name not in last_values:
            last_values[gain_name] = 0
            last_values[loss_name] = 0

        # คำนวณการเปลี่ยนแปลงของราคาจากรอบก่อนหน้า
        prev_close = last_values.get(prev_close_name, close_price)  # ใช้ค่าล่าสุด หรือราคาปัจจุบันถ้าไม่มี
        change = close_price - prev_close
        
        gain = max(change, 0)
        loss = abs(min(change, 0))

        # ใช้ Wilder's Smoothing Method
        if last_values[gain_name] == 0 and last_values[loss_name] == 0:
            # กรณีรันครั้งแรก ใช้ค่าเฉลี่ยแบบ SMA
            last_values[gain_name] = gain
            last_values[loss_name] = loss
        else:
            alpha = 1 / period
            last_values[gain_name] = (last_values[gain_name] * (period - 1) + gain) / period
            last_values[loss_name] = (last_values[loss_name] * (period - 1) + loss) / period

        # คำนวณ RSI
        if last_values[loss_name] == 0:
            rsi = 100  # ถ้าไม่มีขาดทุนเลย ให้ RSI เป็น 100
        else:
            rs = last_values[gain_name] / last_values[loss_name]
            rsi = 100 - (100 / (1 + rs))

        # อัปเดตค่าล่าสุดของราคาปิด
        last_values[prev_close_name] = close_price
        last_values["Date"] = date
        return {
            "Date": date,
            "RSI": rsi
        }

    def update_cci(self,
               close_price,
               high_price,
               low_price,
               date,
               indicator_params,
               last_values,
               tp_name="tp",
               sma_tp_name="sma_tp",
               mean_dev_name="mean_deviation",
               cci_name="CCI"): # ok
        """คำนวณ CCI ตาม pandas_ta และรองรับการเริ่มต้น last_values ว่างเปล่า"""
        
        period = indicator_params.get("periods", 20)
        constant = 0.015  # ค่าคงที่มาตรฐานของ CCI

        # ✅ ถ้า last_values เป็น None หรือ ว่างเปล่า ให้สร้าง dict ใหม่
        if not last_values or not isinstance(last_values, dict):
            last_values = {}

        # ✅ ตรวจสอบและกำหนดค่าเริ่มต้นให้กับ last_values
        if tp_name not in last_values or not isinstance(last_values.get(tp_name), list):
            last_values[tp_name] = []
        if sma_tp_name not in last_values or not isinstance(last_values.get(sma_tp_name), (int, float, type(None))):
            last_values[sma_tp_name] = None
        if mean_dev_name not in last_values or not isinstance(last_values.get(mean_dev_name), (int, float, type(None))):
            last_values[mean_dev_name] = None

        # ✅ คำนวณ Typical Price (TP)
        typical_price = (high_price + low_price + close_price) / 3
        last_values[tp_name].append(typical_price)
        print(f'last_values[tp_name]:{last_values[tp_name]}')

        # ✅ จำกัดขนาดของรายการ TP ไม่ให้เกิน period
        if len(last_values[tp_name]) > period:
            last_values[tp_name].pop(0)

        # ✅ คำนวณค่าเฉลี่ยเคลื่อนที่ของ TP (SMA_TP)
        if len(last_values[tp_name]) == period:
            last_values[sma_tp_name] = np.mean(last_values[tp_name])
        else:
            last_values[sma_tp_name] = None

        # ✅ คำนวณ Mean Deviation
        if last_values[sma_tp_name] is not None:
            mean_deviation = np.mean([abs(tp - last_values[sma_tp_name]) for tp in last_values[tp_name]])
            last_values[mean_dev_name] = mean_deviation
        else:
            last_values[mean_dev_name] = None

        # ✅ คำนวณ CCI
        if last_values[sma_tp_name] is not None and last_values[mean_dev_name] is not None and last_values[mean_dev_name] != 0:
            cci = (typical_price - last_values[sma_tp_name]) / (constant * last_values[mean_dev_name])
        else:
            cci = None

        # ✅ บันทึกค่าล่าสุด
        last_values[cci_name] = cci
        
        last_values["Date"] = date

        return {"Date": date, "CCI": cci, "SMA_TP": last_values[sma_tp_name], "Mean_Deviation": last_values[mean_dev_name]}
    
    def update_atr(self, high_price, low_price, close_price, date, indicator_params, last_values,
                   atr_name="ATR", prev_close_name="Close"): #ok
        """คำนวณ ATR (Average True Range) ตาม Wilder’s Smoothing"""
        period = indicator_params.get("periods", 14)

        # ✅ กำหนดค่าเริ่มต้นให้ last_values
        if atr_name not in last_values:
            last_values[atr_name] = 0
        if prev_close_name not in last_values:
            last_values[prev_close_name] = close_price

        # ✅ คำนวณ True Range (TR)
        tr = max(
            high_price - low_price,
            abs(high_price - last_values[prev_close_name]),
            abs(low_price - last_values[prev_close_name])
        )

        # ✅ ใช้ Wilder’s Smoothing ในการคำนวณ ATR
        if last_values[atr_name] == 0:
            last_values[atr_name] = tr  # กรณีค่าเริ่มต้น ใช้ TR แทน
        else:
            last_values[atr_name] = (last_values[atr_name] * (period - 1) + tr) / period

        # ✅ อัปเดตค่าล่าสุด
        last_values[prev_close_name] = close_price
        last_values["Date"] = date

        return {"Date": date, "ATR": last_values[atr_name]}
    
    def update_di_plus(self, high_price, low_price, close_price, date, indicator_params, last_values,
                       dm_plus_name="dm_plus", atr_name="ATR", di_plus_name="DI_Plus",
                       prev_high_name="High", prev_low_name="Low", prev_close_name="Close"):
        """คำนวณ DI+ พร้อมคำนวณ ATR ในตัวเอง"""
        period = indicator_params.get("periods", 14)

        # ✅ กำหนดค่าเริ่มต้นให้ last_values
        if dm_plus_name not in last_values:
            last_values[dm_plus_name] = 0
        if prev_high_name not in last_values:
            last_values[prev_high_name] = high_price
        if prev_low_name not in last_values:
            last_values[prev_low_name] = low_price
        if prev_close_name not in last_values:
            last_values[prev_close_name] = close_price
        if atr_name not in last_values:
            last_values[atr_name] = 0  # ใช้ค่าเริ่มต้นเป็น 0

        # ✅ คำนวณ True Range (TR) และอัปเดต ATR
        tr = max(
            high_price - low_price,
            abs(high_price - last_values[prev_close_name]),
            abs(low_price - last_values[prev_close_name])
        )

        # ใช้ Wilder’s Smoothing
        if last_values[atr_name] == 0:
            last_values[atr_name] = tr
        else:
            last_values[atr_name] = (last_values[atr_name] * (period - 1) + tr) / period

        # ✅ คำนวณ +DM (Directional Movement)
        dm_plus = high_price - last_values[prev_high_name] if (high_price - last_values[prev_high_name]) > (last_values[prev_low_name] - low_price) else 0

        # ✅ ใช้ Wilder’s Smoothing
        last_values[dm_plus_name] = (last_values[dm_plus_name] * (period - 1) + dm_plus) / period

        # ✅ คำนวณ DI+
        di_plus = (last_values[dm_plus_name] / last_values[atr_name]) * 100 if last_values[atr_name] != 0 else None

        last_values[prev_high_name] = high_price
        last_values[prev_low_name] = low_price
        last_values[prev_close_name] = close_price
        last_values["Date"] = date

        return {"Date": date, "DI_Plus": di_plus, "ATR": last_values[atr_name]}

    def update_di_minus(self, high_price, low_price, close_price, date, indicator_params, last_values,
                        dm_minus_name="dm_minus", atr_name="ATR", di_minus_name="DI_Minus",
                        prev_high_name="High", prev_low_name="Low", prev_close_name="Close"):
        # dm,atr ต้องเป็นของวันก่อนหน้า
        """คำนวณ DI- พร้อมคำนวณ ATR ในตัวเอง"""
        period = indicator_params.get("periods", 14)

        # ✅ กำหนดค่าเริ่มต้นให้ last_values
        if dm_minus_name not in last_values:
            last_values[dm_minus_name] = 0
        if prev_high_name not in last_values:
            last_values[prev_high_name] = high_price
        if prev_low_name not in last_values:
            last_values[prev_low_name] = low_price
        if prev_close_name not in last_values:
            last_values[prev_close_name] = close_price
        if atr_name not in last_values:
            last_values[atr_name] = 0  # ใช้ค่าเริ่มต้นเป็น 0

        # ✅ คำนวณ True Range (TR) และอัปเดต ATR
        tr = max(
            high_price - low_price,
            abs(high_price - last_values[prev_close_name]),
            abs(low_price - last_values[prev_close_name])
        )

        # ใช้ Wilder’s Smoothing
        if last_values[atr_name] == 0:
            last_values[atr_name] = tr
        else:
            last_values[atr_name] = (last_values[atr_name] * (period - 1) + tr) / period

        # ✅ คำนวณ -DM (Directional Movement)
        dm_minus = last_values[prev_low_name] - low_price if (last_values[prev_low_name] - low_price) > (high_price - last_values[prev_high_name]) else 0

        # ✅ ใช้ Wilder’s Smoothing
        last_values[dm_minus_name] = (last_values[dm_minus_name] * (period - 1) + dm_minus) / period

        # ✅ คำนวณ DI-
        di_minus = (last_values[dm_minus_name] / last_values[atr_name]) * 100 if last_values[atr_name] != 0 else None

        # ✅ อัปเดตค่าล่าสุด
        last_values[prev_high_name] = high_price
        last_values[prev_low_name] = low_price
        last_values[prev_close_name] = close_price
        last_values["Date"] = date

        return {"Date": date, "DI_Minus": di_minus, "ATR": last_values[atr_name]}
    
    def update_ema(self, close_price, date, indicator_params, last_values,
                   ema_name="EMA"):
        """คำนวณ EMA และอัปเดตค่าล่าสุด"""
        period = indicator_params.get("periods", 14)

        # ✅ ถ้ายังไม่มีค่า EMA เริ่มต้นให้ใช้ค่า Close
        if ema_name not in last_values:
            last_values[ema_name] = close_price
        else:
            alpha = 2 / (period + 1)  # ค่า weight ของ EMA
            last_values[ema_name] = (close_price * alpha) + (last_values[ema_name] * (1 - alpha))
        last_values["Date"] = date

        return {"Date": date, ema_name: last_values[ema_name]}

    def update_bollinger_bands(self, close_price, date, indicator_params, last_values,
                               bb_middle_name="BB_Middle", bb_upper_name="BB_Upper", bb_lower_name="BB_Lower",
                               bb_std_name="BB_STD"):
        """คำนวณ Bollinger Bands และอัปเดตค่าล่าสุด"""
        period = indicator_params.get("BB_period", 20)
        std_factor = indicator_params.get("BB_std_factor", 2)

        # ✅ ถ้ายังไม่มีค่า SMA เริ่มต้น ให้ใช้ list เก็บค่า Close
        if bb_middle_name not in last_values or not isinstance(last_values.get(bb_middle_name), list):
            last_values[bb_middle_name] = []

        last_values[bb_middle_name].append(close_price)

        # ✅ จำกัดขนาดของ list ไม่ให้เกิน period
        if len(last_values[bb_middle_name]) > period:
            last_values[bb_middle_name].pop(0)

        # ✅ คำนวณค่า SMA (BB_Middle)
        if len(last_values[bb_middle_name]) == period:
            bb_middle = np.mean(last_values[bb_middle_name])
            std = np.std(last_values[bb_middle_name])  # คำนวณค่าเบี่ยงเบนมาตรฐาน (Standard Deviation)
            upper_band = bb_middle + (std_factor * std)
            lower_band = bb_middle - (std_factor * std)
        else:
            bb_middle, std, upper_band, lower_band = None, None, None, None

        return {
            "Date": date,
            bb_middle_name: bb_middle,  # เปลี่ยนจาก BB_SMA เป็น BB_Middle
            bb_upper_name: upper_band,
            bb_lower_name: lower_band,
        }