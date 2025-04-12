from sqlalchemy import Column, Date, Float
from adapters.database.mysql_database import Base
from adapters.models.Imodel import IModel
# from adapters.models.combine_meta import CombinedMeta
from abc import ABCMeta

class YTM(Base, IModel):
    __tablename__ = 'ytm'
    # __metaclass__ = CombinedMeta
    
    date = Column(Date, primary_key=True, nullable=False)
    ytm = Column(Float, nullable=True)
    
    def __repr__(self):
        return f"<YTM(date='{self.date}', ytm='{self.ytm}')>"
    
    def to_dict(self):
        return {
            'date': self.date,
            'ytm': self.ytm
        }
    
    @classmethod
    def to_model(cls, data):
        """
        แปลง dictionary ให้เป็น instance ของ YTM
        """
        return cls(
            date=data.get('date'),
            ytm=data.get('ytm')
        )
    