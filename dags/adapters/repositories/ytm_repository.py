from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from adapters.repositories.Irepository import IDateRepository
from adapters.models.ytm_model import YTM
from adapters.exceptions import *
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session


class YTMRepository(IDateRepository):
    def __init__(self, database):
        self._database = database
    
    def database(self):
        return self._database
    
    @property
    def model_class(self):
        return YTM
    
    def get(self, model: YTM):
        try:
            session = self._database.get_session()
            ytm = session.query(YTM).filter(YTM.date == model.date).first()
            return ytm
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def create(self, model: YTM):
        try:
            session = self._database.get_session()
            session.add(model)
            session.commit()
        except IntegrityError as e:
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    
    def create_all(self, models: list[YTM]):
        try:
            session = self._database.get_session()
            session.add_all(models)
            session.commit()
        except IntegrityError as e:
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
        
    def create_or_update(self, model: YTM):
        try:
            session = self._database.get_session()
            existing_ytm = session.query(YTM).filter(YTM.date == model.date).first()
            if existing_ytm is None:
                session.add(model)
            else:
                existing_ytm.ytm = model.ytm
            session.commit()
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to create or update ytm.", e) from e
        finally:
            session.close()
            
    def create_or_update_all(self, models: list[YTM]):
        try:
            session = self._database.get_session()
            for model in models:
                existing_ytm = session.query(YTM).filter(YTM.date == model.date).first()
                if existing_ytm is None:
                    session.add(model)
                else:
                    existing_ytm.ytm = model.ytm
            session.commit()
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to create or update ytm.", e) from e
        finally:
            session.close()
    
    def update(self, model: YTM):
        try:
            session = self._database.get_session()
            existing_ytm = session.query(YTM).filter(YTM.date == model.date).first()
            if existing_ytm is None:
                raise DatabaseException("Failed to update ytm. YTM not found.")
            existing_ytm.ytm = model.ytm
            session.commit()
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to update ytm.", e) from e
        finally:
            session.close()
    
    def delete(self, model: YTM):
        try:
            session = self._database.get_session()
            existing_ytm = session.query(YTM).filter(YTM.date == model.date).first()
            if existing_ytm is None:
                raise DatabaseException("Failed to delete ytm. YTM not found.")
            session.delete(existing_ytm)
            session.commit()
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to delete ytm.", e) from e
        finally:
            session.close()
    
    def get_recent(self, model: YTM, range=150):
        try:
            session = self._database.get_session()
            ytm = session.query(YTM).filter(YTM.date <= model.date).order_by(YTM.date.desc()).limit(range).all()
            return ytm
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve recent ytm.", e) from e
        finally:
            session.close()
    
    def get_recent_from_end_date(self, model: YTM, end_date, range=150):
        try:
            session = self._database.get_session()
            
            ytms = (
                session.query(YTM)
                .filter(
                    YTM.date <= end_date
                )
                .order_by(YTM.date.desc())
                .limit(range)
                .all()
            )
            
            data = [
                {
                    'date': ytm.date.strftime('%Y-%m-%d'),
                    'ytm': float(ytm.ytm) if ytm.ytm is not None else None,
                }
                for ytm in reversed(ytms)
            ]
            
            return data
        
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        
        finally:
            session.close()
        
    def get_recent_from_back_days_and_end_date(self, model: YTM, end_date, back_days: int, range: int = 150):
        try:
            session: Session = self._database.get_session()

            # แปลง end_date เป็น datetime (ถ้าจำเป็น)
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

            sql = text("""
                WITH ordered_data AS (
                    SELECT 
                        date,
                        ytm,
                        ROW_NUMBER() OVER (ORDER BY date DESC) AS row_num
                    FROM ytm
                    WHERE date <= :end_date
                ),
                selected_start AS (
                    SELECT MIN(row_num) AS start_row FROM ordered_data WHERE row_num > :back_days
                )
                SELECT date, ytm
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
                    "ytm": float(row.ytm) if row.ytm is not None else None
                }
                for row in reversed(result)
            ]

            return data

        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to retrieve fund data.", e) from e
        finally:
            session.close()    
    # def get_recent_from_back_days_and_end_date(self, model: YTM, end_date: str, back_days: int, range: int = 150):
    #     try:
    #         session = self._database.get_session()

    #         sql = text("""
    #             SET @rownum := 0;
    #             WITH ordered_data AS (
    #                 SELECT
    #                     date,
    #                     ytm,
    #                     (@rownum := @rownum + 1) AS row_num
    #                 FROM (
    #                     SELECT date, ytm
    #                     FROM ytm
    #                     WHERE date <= :end_date
    #                     ORDER BY date DESC
    #                 ) AS subquery
    #             ),
    #             selected_start AS (
    #                 SELECT MIN(row_num) AS start_row FROM ordered_data WHERE row_num >= :back_days
    #             )
    #             SELECT date, ytm
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
    #             {'date': row.date.strftime('%Y-%m-%d'), 'ytm': float(row.ytm) if row.ytm is not None else None}
    #             for row in reversed(result)
    #         ]

    #         return data

    #     except Exception as e:
    #         session.rollback()
    #         raise DatabaseException("Failed to retrieve fund data.", e) from e
    #     finally:
    #         session.close()

    