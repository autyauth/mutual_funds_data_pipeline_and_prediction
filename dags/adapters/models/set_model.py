from sqlalchemy import Column, Date, Float
from adapters.database.mysql_database import Base
from adapters.models.Imodel import IModel

from abc import ABCMeta

class SET(Base, IModel):
    __tablename__ = 'set'
    # __metaclass__ = CombinedMeta
    
    date = Column(Date, primary_key=True, nullable=False)
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    
    def __repr__(self):
        return f"<SET(date='{self.date}', close='{self.close}')>"
    
    def to_dict(self):
        return {
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
        }
    
    @classmethod
    def to_model(cls, data):
        """
        แปลง dictionary ให้เป็น instance ของ SET
        """
        return cls(
            date=data.get('date'),
            open=data.get('open'),
            high=data.get('high'),
            low=data.get('low'),
            close=data.get('close'),
        )