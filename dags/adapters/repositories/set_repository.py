from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from adapters.repositories.Irepository import IDateRepository
from adapters.models.set_model import SET
from adapters.exceptions import *
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session

class SETRepository(IDateRepository):
    def __init__(self, database):
        self._database = database
    
    def database(self):
        return self._database
    
    @property
    def model_class(self):
        return SET
    
    def get(self, model: SET):
        try:
            session = self._database.get_session()
            set = session.query(SET).filter(SET.date == model.date).first()
            return set
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def create(self, model: SET):
        try:
            session = self._database.get_session()
            session.add(model)
            session.commit()
        except IntegrityError as e:
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    def create_all(self, models: list[SET]):
        try:
            session = self._database.get_session()
            session.add_all(models)
            session.commit()
        except IntegrityError as e:
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
            
    def create_or_update(self, model: SET):
        try:
            session = self._database.get_session()
            existing_set = session.query(SET).filter(SET.date == model.date).first()
            if existing_set is None:
                session.add(model)
            else:
                existing_set.open = model.open
                existing_set.high = model.high
                existing_set.low = model.low
                existing_set.close = model.close
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update mutual fund.", e) from e
        finally:
            session.close()
    
    def create_or_update_all(self, models: list[SET]):
        try:
            session = self._database.get_session()
            for model in models:
                existing_set = session.query(SET).filter(SET.date == model.date).first()
                if existing_set is None:
                    session.add(model)
                else:
                    existing_set.open = model.open
                    existing_set.high = model.high
                    existing_set.low = model.low
                    existing_set.close = model.close
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update mutual fund.", e) from e
        finally:
            session.close()
    
    def update(self, model: SET):
        try:
            session = self._database.get_session()
            set = session.query(SET).filter(SET.date == model.date).first()
            set.open = model.open
            set.high = model.high
            set.low = model.low
            set.close = model.close
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to update mutual fund.", e) from e
        finally:
            session.close()
    
    def delete(self, model: SET):
        try:
            session = self._database.get_session()
            set = session.query(SET).filter(SET.date == model.date).first()
            session.delete(set)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to delete mutual fund.", e) from e
        finally:
            session.close()
    
    def get_recent(self, model: SET, range=150):
        try:
            session = self._database.get_session()
            set = session.query(SET).filter(SET.date <= model.date).order_by(SET.date.desc()).limit(range).all()
            return set
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def get_recent_from_end_date(self, model: SET, end_date, range=150):
        try:
            session = self._database.get_session()
            
            sets = (
                session.query(SET)
                .filter(
                    SET.date <= end_date
                )
                .order_by(SET.date.desc())
                .limit(range)
                .all()
            )
            
            data = [
                {
                    'date': set.date.strftime('%Y-%m-%d'),
                    'set': float(set.close) if set.close is not None else None,
                }
                for set in reversed(sets)
            ]
            
            return data
        
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        
        finally:
            session.close()
    
    def get_recent_from_back_days_and_end_date(self, model: SET, end_date, back_days: int, range: int = 150):
        try:
            session: Session = self._database.get_session()

            # แปลง end_date เป็น datetime (ถ้าจำเป็น)
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

            sql = text("""
                WITH ordered_data AS (
                    SELECT 
                        date,
                        close,
                        ROW_NUMBER() OVER (ORDER BY date DESC) AS row_num
                    FROM `set`
                    WHERE date <= :end_date
                ),
                selected_start AS (
                    SELECT MIN(row_num) AS start_row FROM ordered_data WHERE row_num > :back_days
                )
                SELECT date, close
                FROM ordered_data
                WHERE row_num >= (SELECT start_row FROM selected_start)
                ORDER BY date DESC
                LIMIT :range;
            """)

            # ใช้ context manager เพื่อให้ session ปิดอัตโนมัติ
            with session.begin():
                result = session.execute(sql, {
                    "end_date": end_date,
                    "back_days": back_days,
                    "range": range
                }).fetchall()

            # แปลงผลลัพธ์เป็น JSON
            data = [
                {
                    "date": row.date.strftime('%Y-%m-%d'),
                    "set": float(row.close) if row.close is not None else None
                }
                for row in reversed(result)
            ]

            return data

        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to retrieve fund data.", e) from e
        finally:
            session.close()
    # def get_recent_from_back_days_and_end_date(self, model: SET, end_date: str, back_days: int, range: int = 150):
    #     try:
    #         session = self._database.get_session()

    #         sql = text("""
    #             SET @rownum := 0;
    #             WITH ordered_data AS (
    #                 SELECT
    #                     date,
    #                     close,
    #                     (@rownum := @rownum + 1) AS row_num
    #                 FROM (
    #                     SELECT date, close
    #                     FROM `set`
    #                     WHERE date <= :end_date
    #                     ORDER BY date DESC
    #                 ) AS subquery
    #             ),
    #             selected_start AS (
    #                 SELECT MIN(row_num) AS start_row FROM ordered_data WHERE row_num >= :back_days
    #             )
    #             SELECT date, close
    #             FROM ordered_data, selected_start
    #             WHERE ordered_data.row_num >= selected_start.start_row
    #             ORDER BY ordered_data.date DESC
    #             LIMIT :range;
    #         """)

    #         # Execute Query
    #         result = session.execute(sql, {
    #             "end_date": end_date,
    #             "back_days": back_days,
    #             "range": range
    #         }).fetchall()

    #         # แปลงผลลัพธ์เป็น JSON
    #         data = [
    #             {'date': row.date.strftime('%Y-%m-%d'), 'set': float(row.close) if row.close is not None else None}
    #             for row in reversed(result)
    #         ]

    #         return data

    #     except Exception as e:
    #         session.rollback()
    #         raise DatabaseException("Failed to retrieve fund data.", e) from e
    #     finally:
    #         session.close()
    

    
    
    