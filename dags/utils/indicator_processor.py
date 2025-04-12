import pandas_ta as ta
import numpy as np
import pandas as pd

class IndicatorProcessor:
    def __init__(self):
        self.function_map = {
            "pct_change": self.pct_change,
            "rsi": self.rsi,
            "cci": self.cci,
            "macd": self.macd,
            "macd_hist": self.macd_hist,
            "roc": self.roc,
            "di_minus": self.di_minus,
            "di_plus": self.di_plus,
            "ema": self.ema,
            "ema_diff": self.ema_diff,
            "atr": self.atr,
            "bb_middle": self.BB_middle,
            "ema_diff_sf": self.ema_diff_sf
        }

    def pct_change(self, data, params): #
        periods = params.get("periods", 1)
        return data.pct_change(periods=periods)

    def rsi(self, data, params): #
        periods = params.get("periods", 14)
        return ta.rsi(data, length=periods)

    def cci(self, data, params): #
        periods = params.get("periods", 20)
        return ta.cci(data, data, data, length=periods)

    def macd(self, data, params): #
        fast = params.get("fast", 12)
        slow = params.get("slow", 26)
        signal = params.get("signal", 9)
        macd = ta.macd(data, fast=fast, slow=slow, signal=signal)
        return macd[f'MACD_{fast}_{slow}_{signal}']

    def macd_hist(self, data, params): #
        fast = params.get("fast", 12)
        slow = params.get("slow", 26)
        signal = params.get("signal", 9)
        macd = ta.macd(data, fast=fast, slow=slow, signal=signal)
        return macd[f'MACDh_{fast}_{slow}_{signal}']

    def roc(self, data, params): #
        periods = params.get("periods", 10)
        return ta.roc(data, length=periods)

    def di_minus(self, data, params): #
        periods = params.get("periods", 14)
        adx = ta.adx(high=data, low=data, close=data, length=periods)
        di_minus = adx[f'DMN_{periods}']
        return di_minus
    
    def di_plus(self, data, params): #
        periods = params.get("periods", 14)
        adx = ta.adx(high=data, low=data, close=data, length=periods)
        return adx[f'DMP_{periods}']
    
    def ema(self, data, params): 
        periods = params.get("periods", 14)
        return ta.ema(data, periods)
    
    def ema_diff(self, data, params):
        periods = params.get("periods", 14)
        ema = ta.ema(data, periods)
        return data - ema
    
    def atr(self, data, params): #
        periods = params.get("periods", 14)
        return ta.atr(high=data, low=data, close=data, length=periods)
    
    def BB_middle(self, data, params):
        periods = params.get("periods", 20)
        bb = ta.bbands(data, length=int(periods*0.5), std=2)
        print(f'{bb[f'BBM_{periods*0.5:.0f}_2.0']}')
        return bb[f'BBM_{periods*0.5:.0f}_2.0']
    
    def ema_diff_sf(self, data, params):
        fast_periods = params.get("fast", 12)
        slow_periods = params.get("slow", 26)
        ema_fast = ta.ema(data, fast_periods)
        ema_slow = ta.ema(data, slow_periods)
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
