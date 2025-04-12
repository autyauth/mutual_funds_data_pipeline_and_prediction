from sqlalchemy import Column, String, Date, Boolean
from adapters.database.mysql_database import Base
from adapters.models.Imodel import IModel
from sqlalchemy.orm import relationship
# from adapters.models.combine_meta import CombinedMeta
from abc import ABCMeta

class DataInvalidDates(Base, IModel):
    __tablename__ = 'data_invalid_dates'  # ชื่อตารางในฐานข้อมูล
    # __metaclass__ = CombinedMeta

    proj_id = Column(String(255), nullable=True)
    name = Column(String(255), primary_key=True, nullable=False)
    is_use_api = Column(Boolean, nullable=True)
    data_source = Column(String(255), nullable=True)
    json_config = Column(String(255), nullable=True)
    invalid_date = Column(Date, nullable=True)
    
    
    def __repr__(self):
        return f"<data_invalid_dates(proj_id='{self.proj_id}', name='{self.name}')>"
    
    def to_dict(self):
        return {
            'proj_id': self.proj_id,
            'name': self.name,
            'is_use_api': self.is_use_api,
            'data_source': self.data_source,
            'json_config': self.json_config,
            'invalid_date': self.invalid_date,
        }

    @classmethod
    def to_model(cls, data):
        """
        แปลง dictionary ให้เป็น instance ของ MutualFund
        """
        return cls(
            proj_id=data.get('proj_id'),
            name=data.get('name'),
            is_use_api=data.get('is_use_api'),
            data_source=data.get('data_source'),
            json_config=data.get('json_config'),
            invalid_date=data.get('invalid_date')
        )