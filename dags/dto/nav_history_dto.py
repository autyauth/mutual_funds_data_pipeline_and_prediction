from datetime import date, datetime
from decimal import Decimal

class NavHistoryDTO:
    def __init__(self, fund_name: str, date: [str, date], nav: [float, Decimal], fund_type: str,
                 selling_price: [float, Decimal], redemption_price: [float, Decimal],
                 total_net_assets: [float, Decimal], change: [float, Decimal]):
        self.fund_name = fund_name
        self.date = self._convert_date(date)
        self.nav = self._convert_decimal(nav)
        self.fund_type = fund_type
        self.selling_price = self._convert_decimal(selling_price)
        self.redemption_price = self._convert_decimal(redemption_price)
        self.total_net_assets = self._convert_decimal(total_net_assets)
        self.change = self._convert_decimal(change)

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
            'nav': self.nav,
            'fund_type': self.fund_type,
            'selling_price': self.selling_price,
            'redemption_price': self.redemption_price,
            'total_net_assets': self.total_net_assets,
            'change': self.change
        }

    @staticmethod
    def from_dict(data: dict):
        """ สร้าง DTO จาก dictionary """
        return NavHistoryDTO(
            fund_name=data.get('fund_name'),
            date=data.get('date'),
            nav=data.get('nav'),
            fund_type=data.get('fund_type'),
            selling_price=data.get('selling_price'),
            redemption_price=data.get('redemption_price'),
            total_net_assets=data.get('total_net_assets'),
            change=data.get('change')
        )

    def __repr__(self):
        """ แสดงข้อมูล DTO เพื่อช่วย Debug """
        return f"NavHistoryDTO(fund_name={self.fund_name}, date={self.date}, nav={self.nav}, " \
               f"fund_type={self.fund_type}, selling_price={self.selling_price}, " \
               f"redemption_price={self.redemption_price}, total_net_assets={self.total_net_assets}, " \
               f"change={self.change})"
