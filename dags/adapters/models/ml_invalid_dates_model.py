# from sqlalchemy import Column, String, Date, Boolean
# from adapters.database.mysql_database import Base
# from adapters.models.Imodel import IModel
# from sqlalchemy.orm import relationship

# from abc import ABCMeta

# class MLInvalidDates(Base, IModel):
#     __tablename__ = 'ml_invalid_dates'  # ชื่อตารางในฐานข้อมูล
#     # __metaclass__ = CombinedMeta

#     proj_id = Column(String(255), primary_key=True, nullable=False)
#     name = Column(String(255), nullable=True)
#     invalid_date = Column(Date, nullable=True)
    
    
#     def __repr__(self):
#         return f"<ml_invalid_dates(proj_id='{self.proj_id}', name='{self.name}')>"
    
#     def to_dict(self):
#         return {
#             'proj_id': self.proj_id,
#             'name': self.name,
#             'invalid_date': self.invalid_date
#         }

#     @classmethod
#     def to_model(cls, data):
#         """
#         แปลง dictionary ให้เป็น instance ของ MutualFund
#         """
#         return cls(
#             proj_id=data.get('proj_id'),
#             name=data.get('name'),
#             invalid_date=data.get('invalid_date')
#         )