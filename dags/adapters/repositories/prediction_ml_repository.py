from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from adapters.repositories.Irepository import IDateRepository
from adapters.models.prediction_ml_model import PredictionML
from adapters.exceptions import *
from datetime import datetime, timedelta

class PredictionMLRepository(IDateRepository):
    def __init__(self, database):
        self._database = database
    
    def database(self):
        return self._database
    
    @property
    def model_class(self):
        return PredictionML
    
    def get(self, model: PredictionML):
        try:
            
            session = self._database.get_session()
            fund_nav_daily = session.query(PredictionML).filter(PredictionML.proj_id == model.proj_id, PredictionML.nav_date == model.nav_date).first()
            return fund_nav_daily
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
            
    def create(self, model: PredictionML):
        try:
            session = self._database.get_session()
            session.add(model)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
            
    def create_all(self, models: list[PredictionML]):
        try:
            session = self._database.get_session()
            session.add_all(models)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    # def create_or_update(self, model: PredictionML):
    #     try:
    #         session = self._database.get_session()
    #         existing_fund_nav_daily = session.query(PredictionML).filter(PredictionML.proj_id == model.proj_id, PredictionML.nav_date == model.nav_date).first()
    #         if existing_fund_nav_daily is None:
    #             session.add(model)
    #         else:
    #             existing_fund_nav_daily.name = model.name
    #             existing_fund_nav_daily.trend = model.trend
    #         session.commit()
    #     except SQLAlchemyError as e:
    #         session.rollback()
    #         raise DatabaseException("Failed to create or update mutual fund.", e) from e
    #     finally:
    #         session.close()
    def create_or_update(self, model: PredictionML):
        """ สร้างหรืออัปเดตข้อมูลกองทุนในฐานข้อมูล """
        try:
            session = self._database.get_session()
            existing_fund_nav_daily = session.query(PredictionML).filter(
                PredictionML.proj_id == model.proj_id, 
                PredictionML.nav_date == model.nav_date
            ).first()

            if existing_fund_nav_daily is None:
                session.add(model)
            else:
                # อัปเดตค่าทั้งหมด
                for column in model.__table__.columns.keys():
                    setattr(existing_fund_nav_daily, column, getattr(model, column))

            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update mutual fund.", e) from e
        finally:
            session.close()
    
    def create_or_update_all(self, models: list[PredictionML]):
        try:
            session = self._database.get_session()
            for model in models:
                existing_fund_nav_daily = session.query(PredictionML).filter(PredictionML.proj_id == model.proj_id, PredictionML.nav_date == model.nav_date).first()
                if existing_fund_nav_daily is None:
                    session.add(model)
                else:
                    existing_fund_nav_daily.name = model.name
                    existing_fund_nav_daily.trend = model.trend
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update mutual fund.", e) from e
        finally:
            session.close()
    
    def update(self, model: PredictionML):
        try:
            session = self._database.get_session()
            fund_nav_daily = session.query(PredictionML).filter(PredictionML.proj_id == model.proj_id, PredictionML.nav_date == model.nav_date).first()
            fund_nav_daily.name = model.name
            fund_nav_daily.trend = model.trend
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to update mutual fund.", e) from e
        finally:
            session.close()
    
    def delete(self, model: PredictionML):
        try:
            session = self._database.get_session()
            fund_nav_daily = session.query(PredictionML).filter(PredictionML.proj_id == model.proj_id, PredictionML.nav_date == model.nav_date).first()
            session.delete(fund_nav_daily)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to delete mutual fund.", e) from e
        finally:
            session.close()
    
    def get_recent(self, model: PredictionML, range=150):
        try:
            session = self._database.get_session()
            fund_nav_daily = session.query(PredictionML).filter(PredictionML.proj_id == model.proj_id).order_by(PredictionML.nav_date.desc()).limit(range).all()
            return fund_nav_daily
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()

    def get_recent_from_end_date(self, model: PredictionML, end_date, range=150):
        try:
            session = self._database.get_session()
            
            # ดึงข้อมูลจากฐานข้อมูลโดยเรียงลำดับจากวันที่เก่าสุดไปยังล่าสุด
            fund_nav_daily = (
                session.query(PredictionML)
                .filter(
                    PredictionML.proj_id == model.proj_id,
                    PredictionML.nav_date <= end_date  # เฉพาะข้อมูลถึง end_date
                )
                .order_by(PredictionML.nav_date.desc())  # เรียงจากวันที่เก่าสุดไปล่าสุด
                .limit(range)  # จำกัดจำนวนข้อมูลตาม range
                .all()
            )
            
            # เก็บเฉพาะ nav_date และ last_val
            data = [
                {
                    'Date': fund.nav_date.strftime('%Y-%m-%d'),  # แปลงวันที่เป็น string
                    'trend': fund.trend
                }
                for fund in reversed(fund_nav_daily)  # เรียงกลับให้วันที่เก่าสุดอยู่ข้างหน้า
            ]
            
            return data
        
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        
        finally:
            session.close()