from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from adapters.repositories.Irepository import IDateRepository
from adapters.models.send_predict_invalid_dates_model import SendPredictInvalidDates
from adapters.exceptions import *
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from contextlib import contextmanager
from functools import singledispatchmethod
from sqlalchemy import or_
import time
import random
from datetime import datetime
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from contextlib import contextmanager
from adapters.models.send_predict_invalid_dates_model import SendPredictInvalidDates

class SendPredictInvalidDatesRepository(IDateRepository):
    def __init__(self, database):
        self._database = database
    
    def database(self):
        return self._database
    @property
    def model_class(self):
        return SendPredictInvalidDates
    def get(self, model: SendPredictInvalidDates): #
        try:
            session = self._database.get_session()
            record = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == model.proj_id,
                SendPredictInvalidDates.nav_date == model.nav_date
            ).first()
            return record
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
            
    def create(self, model: SendPredictInvalidDates): #
        try:
            session = self._database.get_session()
            session.add(model)
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()

    def create_all(self, models: list[SendPredictInvalidDates]): #
        try:
            session = self._database.get_session()
            session.add_all(models)
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    def create_or_update(self, model: SendPredictInvalidDates):
        try:
            session = self._database.get_session()
            existing_record = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == model.proj_id,
                SendPredictInvalidDates.nav_date == model.nav_date
            ).first()
            if existing_record:
                for field, value in model.__dict__.items():
                    if hasattr(existing_record, field) and field != '_sa_instance_state':
                        setattr(existing_record, field, value)
                existing_record.last_updated = datetime.now()
            else:
                model.last_updated = datetime.now()
                session.add(model)
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
            
    
    def create_or_update_all(self, models: list[SendPredictInvalidDates]):
        try:
            session = self._database.get_session()
            for model in models:
                existing_record = session.query(SendPredictInvalidDates).filter(
                    SendPredictInvalidDates.proj_id == model.proj_id,
                    SendPredictInvalidDates.nav_date == model.nav_date
                ).first()
                if existing_record:
                    for field, value in model.__dict__.items():
                        if hasattr(existing_record, field) and field != '_sa_instance_state':
                            setattr(existing_record, field, value)
                    existing_record.last_updated = datetime.now()
                else:
                    model.last_updated = datetime.now()
                    session.add(model)
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    
    
    def update(self, model: SendPredictInvalidDates):
        try:
            session = self._database.get_session()
            existing_record = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == model.proj_id,
                SendPredictInvalidDates.nav_date == model.nav_date
            ).first()
            if existing_record:
                for field, value in model.__dict__.items():
                    if hasattr(existing_record, field) and field != '_sa_instance_state':
                        setattr(existing_record, field, value)
                existing_record.last_updated = datetime.now()
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    def update_is_data_send(self, proj_id, nav_date, is_data_send: bool):
        try:
            session = self._database.get_session()
            session.query(SendPredictInvalidDates).filter(
            SendPredictInvalidDates.proj_id == proj_id,
            SendPredictInvalidDates.nav_date == nav_date
            ).update({"is_data_send": is_data_send, "last_updated": datetime.now()}, synchronize_session=False)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to update is_data_send.", e) from e
        finally:
            session.close()
    
    def update_is_predict(self, proj_id, nav_date, is_predict: bool):
        try:
            session = self._database.get_session()
            session.query(SendPredictInvalidDates).filter(
            SendPredictInvalidDates.proj_id == proj_id,
            SendPredictInvalidDates.nav_date == nav_date
            ).update({"is_predict": is_predict, "last_updated": datetime.now()}, synchronize_session=False)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to update is_data_send.", e) from e
        finally:
            session.close()
    
    def update_is_predict_send(self, proj_id, nav_date, is_predict_send: bool):
        try:
            session = self._database.get_session()
            session.query(SendPredictInvalidDates).filter(
            SendPredictInvalidDates.proj_id == proj_id,
            SendPredictInvalidDates.nav_date == nav_date
            ).update({"is_predict_send": is_predict_send, "last_updated": datetime.now()}, synchronize_session=False)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to update is_data_send.", e) from e
        finally:
            session.close()

    def delete(self, model: SendPredictInvalidDates): #
        try:
            session = self._database.get_session()
            existing_record = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == model.proj_id,
                SendPredictInvalidDates.nav_date == model.nav_date
            ).first()
            session.delete(existing_record)
            session.commit()
        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to delete mutual fund.", e) from e
        finally:
            session.close()
    
    ### ✅ get_recent - ดึงข้อมูลล่าสุด ###
    def get_recent(self, model: SendPredictInvalidDates, limit=150): #
        try:
            session = self._database.get_session()
            records = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == model.proj_id
            ).order_by(SendPredictInvalidDates.nav_date.desc()).limit(limit).all()
            return [record.to_dict() for record in records]
        except Exception as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    

    def get_recent_from_end_date(self, model: SendPredictInvalidDates, end_date, limit=150): #
        try:
            session = self._database.get_session()
            records = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == model.proj_id,
                SendPredictInvalidDates.nav_date <= end_date
            ).order_by(SendPredictInvalidDates.nav_date.desc()).limit(limit).all()
            return [record.to_dict() for record in records]
        except Exception as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def get_by_is_send_data(self, proj_id, is_send_data: bool):
        try:
            session = self._database.get_session()
            records = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == proj_id,
                SendPredictInvalidDates.is_data_send == is_send_data
            ).all()
            return records
        except Exception as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def get_by_is_predict(self, proj_id, is_predict: bool):
        try:
            session = self._database.get_session()
            records = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == proj_id,
                SendPredictInvalidDates.is_predict == is_predict
            ).all()
            return records
        except Exception as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def get_by_is_predict_send(self, proj_id, is_predict_send: bool):
        try:
            session = self._database.get_session()
            records = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == proj_id,
                SendPredictInvalidDates.is_predict_send == is_predict_send
            ).all()
            return records
        except Exception as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def get_by_multi_is_(self, proj_id, is_data_send: bool, is_predict: bool, is_predict_send: bool):
        try:
            session = self._database.get_session()
            records = session.query(SendPredictInvalidDates).filter(
                SendPredictInvalidDates.proj_id == proj_id,
                SendPredictInvalidDates.is_data_send == is_data_send,
                SendPredictInvalidDates.is_predict == is_predict,
                SendPredictInvalidDates.is_predict_send == is_predict_send
            ).all()
            return records
        except Exception as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    