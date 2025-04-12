from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from adapters.models.data_invalid_dates_model import DataInvalidDates
from adapters.repositories.Irepository import IRepository
from adapters.exceptions import *
from datetime import datetime, timedelta

class DataInvalidDatesRepository(IRepository):
    def __init__(self, database):
        self._database = database
    
    def database(self):
        return self._database
    
    @property
    def model_class(self):
        return DataInvalidDates
    
    def get(self, model: DataInvalidDates):
        try:
            session = self._database.get_session()
            model_found = session.query(DataInvalidDates).filter(DataInvalidDates.name == model.name).first()
            return model_found
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def get_all(self):
        try:
            session = self._database.get_session()
            models_found = session.query(DataInvalidDates).all()
            return models_found
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual funds.", e) from e
        finally:
            session.close()
    
    def create(self, model: DataInvalidDates):

        try:
            session = self._database.get_session()
            session.add(model)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    def create_all(self, models: list[DataInvalidDates]):
        try:
            session = self._database.get_session()
            session.add_all(models)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    def create_or_update(self, model: DataInvalidDates):
        try:
            session = self._database.get_session()
            existing_model = session.query(DataInvalidDates).filter(DataInvalidDates.name == model.name).first()
            if existing_model is None:
                session.add(model)
            else:
                existing_model.proj_id = model.proj_id
                existing_model.is_use_api = model.is_use_api
                existing_model.data_source = model.data_source
                existing_model.json_config = model.json_config
                existing_model.invalid_date = model.invalid_date
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update mutual fund.", e) from e
        finally:
            session.close()
    
    def create_or_update_all(self, models: list[DataInvalidDates]):
        try:
            session = self._database.get_session()
            for model in models:
                existing_model = session.query(DataInvalidDates).filter(DataInvalidDates.name == model.name).first()
                if existing_model is None:
                    session.add(model)
                else:
                    existing_model.proj_id = model.proj_id
                    existing_model.is_use_api = model.is_use_api
                    existing_model.data_source = model.data_source
                    existing_model.json_config = model.json_config
                    existing_model.invalid_date = model.invalid_date
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update mutual fund.", e) from e
        finally:
            session.close()
    
    def update(self, model: DataInvalidDates):
        try:
            session = self._database.get_session()
            existing_model = session.query(DataInvalidDates).filter(DataInvalidDates.name == model.name).first()
            existing_model.proj_id = model.proj_id
            existing_model.is_use_api = model.is_use_api
            existing_model.data_source = model.data_source
            existing_model.json_config = model.json_config
            existing_model.invalid_date = model.invalid_date
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to update mutual fund.", e) from e
        finally:
            session.close()
    
    def delete(self, model: DataInvalidDates):
        try:
            session = self._database.get_session()
            existing_model = session.query(DataInvalidDates).filter(DataInvalidDates.name == model.name).first()
            session.delete(existing_model)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to delete mutual fund.", e) from e
        finally:
            session.close()
            
    def update_invalid_date(self, name: str, field: str, days: int):
        valid_fields = {"invalid_date", "ml_invalid_date", "send_data_invalid_date", "send_ml_invalid_date"}
        if field not in valid_fields:
            raise ValueError(f"Field '{field}' ไม่ถูกต้อง ต้องเป็นหนึ่งใน {valid_fields}")
        try:
            session = self._database.get_session()
            existing_model = (
            session.query(DataInvalidDates)
                .filter_by(name=name)
                .with_for_update()  
                .first()
            )
            if not existing_model:
                raise DatabaseException(f"ไม่พบข้อมูลสำหรับ '{name}'")
            current_value = getattr(existing_model, field)
            if current_value:
                setattr(existing_model, field, current_value + timedelta(days=days))
            else:
                setattr(existing_model, field, timedelta(days=days))
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to update invalid date.", e) from e
        finally:
            session.close()
