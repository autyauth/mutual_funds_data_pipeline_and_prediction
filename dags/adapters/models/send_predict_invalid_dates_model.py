from sqlalchemy import Column, String, Date, Boolean, DateTime
from datetime import datetime
from adapters.database.mysql_database import Base
from adapters.models.Imodel import IModel
from sqlalchemy.orm import relationship
# from adapters.models.combine_meta import CombinedMeta
from abc import ABCMeta

class SendPredictInvalidDates(Base, IModel):
    __tablename__ = 'send_predict_invalid_dates'  # ชื่อตารางในฐานข้อมูล
    # __metaclass__ = CombinedMeta

    proj_id = Column(String(255), primary_key=True, nullable=False)
    nav_date = Column(Date, primary_key=True, nullable=False)
    name = Column(String(255), nullable=True)
    is_predict = Column(Boolean, nullable=False, default=False)
    is_data_send = Column(Boolean, nullable=False, default=False)
    is_predict_send = Column(Boolean, nullable=False, default=False)
    last_updated = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

    
    
    def __repr__(self):
        return f"<send_predict_invalid_dates(proj_id='{self.proj_id}', name='{self.name}')>"
    
    def to_dict(self):
        return {
            'proj_id': self.proj_id,
            'name': self.name,
            'nav_date': self.nav_date,
            'is_predict': self.is_predict,
            'is_data_send': self.is_data_send,
            'is_predict_send': self.is_predict_send,
            "last_updated": self.last_updated
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
            is_predict=data.get('is_predict'),
            is_data_send=data.get('is_data_send'),
            is_predict_send=data.get('is_predict_send'),
            last_updated=data.get('last_updated')
        )