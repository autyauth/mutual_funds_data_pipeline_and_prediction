from datetime import date, datetime
from decimal import Decimal

class PredictionTrendFundsDTO:
    def __init__(self, fund_name: str, date: [str, date], trend: [str, int],
                 up_trend_prob: [float, Decimal], down_trend_prob: [float, Decimal],
                 reason: str, indicator: str):
        self.fund_name = fund_name
        self.date = self._convert_date(date)
        self.trend = int(trend)
        self.up_trend_prob = self._convert_decimal(up_trend_prob)
        self.down_trend_prob = self._convert_decimal(down_trend_prob)
        self.reason = reason
        self.indicator = indicator
        
    @staticmethod
    def _convert_decimal(value):
        """ แปลงค่า Decimal เป็น float หรือคืนค่า None ถ้าเป็น None """
        if isinstance(value, Decimal):
            return float(value)
        return value

    @staticmethod
    def _convert_date(value):
        """ แปลงค่า date เป็น string (ISO format) """
        if isinstance(value, date):
            return value.isoformat()
        return value
    
    def to_dict(self):
        """ แปลง DTO เป็น dictionary พร้อมแปลงค่าให้ JSON ใช้ได้ """
        return {
            'fund_name': self.fund_name,
            'date': self.date,
            'trend': self.trend,
            'up_trend_prob': self.up_trend_prob,
            'down_trend_prob': self.down_trend_prob,
            'reason': self.reason,
            'indicator': self.indicator
        }
    
    @staticmethod
    def from_dict(data: dict):
        """ สร้าง DTO จาก dictionary """
        return PredictionTrendFundsDTO(
            fund_name=data.get('fund_name'),
            date=data.get('date'),
            trend=data.get('trend'),
            up_trend_prob=data.get('up_trend_prob'),
            down_trend_prob=data.get('down_trend_prob'),
            reason=data.get('reason'),
            indicator=data.get('indicator')
        )
    
    def __repr__(self):
        """ แสดงข้อมูล DTO เพื่อช่วย Debug """
        return f"PredictionTrendFundsDTO(fund_name={self.fund_name}, date={self.date}, trend={self.trend}, " \
               f"up_trend_prob={self.up_trend_prob}, down_trend_prob={self.down_trend_prob}, " \
               f"reason={self.reason}, indicator={self.indicator})" 
    