from sqlalchemy import Column, String, Date, Boolean, Float
from adapters.database.mysql_database import Base
from adapters.models.Imodel import IModel
from sqlalchemy.orm import relationship
# from adapters.models.combine_meta import CombinedMeta
from abc import ABCMeta

class PredictionML(Base, IModel):
    __tablename__ = 'prediction_ml'  # ชื่อตารางในฐานข้อมูล
    # __metaclass__ = CombinedMeta

    proj_id = Column(String(255), primary_key=True, nullable=False)
    name = Column(String(255), nullable=True)
    nav_date = Column(Date, primary_key=True, nullable=False)
    trend = Column(String(255), nullable=True)
    up_trend_prob = Column(Float, nullable=True)
    down_trend_prob = Column(Float, nullable=True)
    
    def __repr__(self):
        return f"<prediction_ml(proj_id='{self.proj_id}', name='{self.name}')>"
    
    def to_dict(self):
        return {
            'proj_id': self.proj_id,
            'name': self.name,
            'nav_date': self.nav_date,
            'trend': self.trend,
            'up_trend_prob': self.up_trend_prob,
            'down_trend_prob': self.down_trend_prob
        }

    @classmethod
    def to_model(cls, data):
        """
        แปลง dictionary ให้เป็น instance ของ MutualFund
        """
        return cls(
            proj_id=data.get('proj_id'),
            name=data.get('name'),
            nav_date=data.get('nav_date'),
            trend=data.get('trend')
        )