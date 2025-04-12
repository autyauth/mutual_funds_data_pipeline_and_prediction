from sqlalchemy import Column, String, Date, DECIMAL, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from adapters.database.mysql_database import Base
from adapters.models.Imodel import IModel
from sqlalchemy.orm import relationship

from abc import ABCMeta

class FundNavDaily(Base, IModel):
    __tablename__ = 'fund_nav_daily'  # ชื่อตารางในฐานข้อมูล
    # __metaclass__ = CombinedMeta

    proj_id = Column(String(50), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    nav_date = Column(Date, primary_key=True, nullable=False)
    last_upd_date = Column(Date, nullable=True)
    class_abbr_name = Column(String(255), nullable=True)
    net_asset = Column(DECIMAL(15, 2), nullable=True)
    last_val = Column(DECIMAL(15, 6), nullable=True)
    previous_val = Column(DECIMAL(15, 6), nullable=True)
    unique_id = Column(String(50), nullable=True)
    sell_price = Column(DECIMAL(15, 6), nullable=True)
    buy_price = Column(DECIMAL(15, 6), nullable=True)
    sell_swap_price = Column(DECIMAL(15, 6), nullable=True)
    buy_swap_price = Column(DECIMAL(15, 6), nullable=True)
    remark_th = Column(Text, nullable=True)
    remark_en = Column(Text, nullable=True)


    def __repr__(self):
        return f"<FundNavDaily(proj_id='{self.proj_id}', nav_date='{self.nav_date}')>"
    
    def to_dict(self):
        return {
            'proj_id': self.proj_id,
            'name': self.name,
            'nav_date': self.nav_date,
            'last_upd_date': self.last_upd_date,
            'class_abbr_name': self.class_abbr_name,
            'net_asset': self.net_asset,
            'last_val': self.last_val,
            'previous_val': self.previous_val,
            'unique_id': self.unique_id,
            'sell_price': self.sell_price,
            'buy_price': self.buy_price,
            'sell_swap_price': self.sell_swap_price,
            'buy_swap_price': self.buy_swap_price,
            'remark_th': self.remark_th,
            'remark_en': self.remark_en
        }
    
    @classmethod
    def to_model(cls, data):
        """
        แปลง dictionary ให้เป็น instance ของ FundNavDaily
        """
        return cls(
            proj_id=data.get('proj_id'),
            name=data.get('name'),
            nav_date=data.get('nav_date'),
            last_upd_date=data.get('last_upd_date'),
            class_abbr_name=data.get('class_abbr_name'),
            net_asset=data.get('net_asset'),
            last_val=data.get('last_val'),
            previous_val=data.get('previous_val'),
            unique_id=data.get('unique_id'),
            sell_price=data.get('sell_price'),
            buy_price=data.get('buy_price'),
            sell_swap_price=data.get('sell_swap_price'),
            buy_swap_price=data.get('buy_swap_price'),
            remark_th=data.get('remark_th'),
            remark_en=data.get('remark_en')
        )