from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from adapters.models.ml_invalid_dates_model import MLInvalidDates
from adapters.repositories.Irepository import IRepository
from adapters.exceptions import *
from datetime import datetime, timedelta

class MLInvalidDatesRepository(IRepository):
    def __init__(self, database):
        self._database = database
    
    def database(self):
        return self._database
    
    @property
    def model_class(self):
        return MLInvalidDates
    
    def get(self, model: MLInvalidDates):
        try:
            session = self._database.get_session()
            model_found = session.query(MLInvalidDates).filter(MLInvalidDates.proj_id == model.proj_id).first()
            return model_found
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by proj_id.", e) from e
        finally:
            session.close()
    
    def get_all(self):
        try:
            session = self._database.get_session()
            models_found = session.query(MLInvalidDates).all()
            return models_found
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual funds.", e) from e
        finally:
            session.close()
    
    def create(self, model: MLInvalidDates):

        try:
            session = self._database.get_session()
            session.add(model)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    def create_all(self, models: list[MLInvalidDates]):
        try:
            session = self._database.get_session()
            session.add_all(models)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    def create_or_update(self, model: MLInvalidDates):
        try:
            session = self._database.get_session()
            existing_model = session.query(MLInvalidDates).filter(MLInvalidDates.proj_id == model.proj_id).first()
            if existing_model is None:
                session.add(model)
            else:
                existing_model.name = model.name
                existing_model.invalid_date = model.invalid_date
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update.", e) from e
        finally:
            session.close()
    
    
    def create_or_update_all(self, models: list[MLInvalidDates]):
        try:
            session = self._database.get_session()
            for model in models:
                existing_model = session.query(MLInvalidDates).filter(MLInvalidDates.proj_id == model.proj_id).first()
                if existing_model is None:
                    session.add(model)
                else:
                    existing_model.name = model.name
                    existing_model.invalid_date = model.invalid_date
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update mutual fund.", e) from e
        finally:
            session.close()
    
    def update(self, model: MLInvalidDates):
        try:
            session = self._database.get_session()
            existing_model = session.query(MLInvalidDates).filter(MLInvalidDates.proj_id == model.proj_id).first()
            existing_model.name = model.name
            existing_model.invalid_date = model.invalid_date
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to update mutual fund.", e) from e
        finally:
            session.close()
    
    def delete(self, model: MLInvalidDates):
        try:
            session = self._database.get_session()
            existing_model = session.query(MLInvalidDates).filter(MLInvalidDates.proj_id == model.proj_id).first()
            session.delete(existing_model)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to delete mutual fund.", e) from e
        finally:
            session.close()