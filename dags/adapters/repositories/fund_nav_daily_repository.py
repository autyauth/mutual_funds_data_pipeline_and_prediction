from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from adapters.repositories.Irepository import IDateRepository
from adapters.models.fund_nav_daily_model import FundNavDaily
from adapters.exceptions import *
from datetime import datetime, timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session
import numpy as np
from sqlalchemy.exc import SQLAlchemyError

class FundNavDailyRepository(IDateRepository):
    def __init__(self, database):
        self._database = database
    
    def database(self):
        return self._database
    def _clean_nan_values(self, model: FundNavDaily):
        """ แปลงค่า NaN หรือ None ใน model ให้เป็น None """
        for column in model.__table__.columns.keys():
            value = getattr(model, column)
            if isinstance(value, float) and np.isnan(value):  # ถ้าเป็น NaN ให้แปลงเป็น None
                setattr(model, column, None)
        return model
    
    @property
    def model_class(self):
        return FundNavDaily
    
    def get(self, model: FundNavDaily):
        try:
            
            session = self._database.get_session()
            fund_nav_daily = session.query(FundNavDaily).filter(FundNavDaily.proj_id == model.proj_id, FundNavDaily.nav_date == model.nav_date).first()
            return fund_nav_daily
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
            
    def create(self, model: FundNavDaily):
        try:
            session = self._database.get_session()
            session.add(model)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
            
    def create_all(self, models: list[FundNavDaily]):
        try:
            session = self._database.get_session()
            session.add_all(models)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise DatabaseException("Failed to create mutual fund.", e) from e
        finally:
            session.close()
    def create_or_update(self, model: FundNavDaily):
        """ สร้างหรืออัปเดตข้อมูลกองทุนในฐานข้อมูล """
        try:
            session = self._database.get_session()
            model = self._clean_nan_values(model)  # แปลง NaN เป็น None

            existing_fund_nav_daily = session.query(FundNavDaily).filter(
                FundNavDaily.proj_id == model.proj_id, 
                FundNavDaily.nav_date == model.nav_date
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

    def create_or_update_all(self, models: list[FundNavDaily]):
        """ สร้างหรืออัปเดตข้อมูลกองทุนหลายตัวในฐานข้อมูล """
        try:
            session = self._database.get_session()
            cleaned_models = [self._clean_nan_values(model) for model in models]  # แปลง NaN เป็น None

            existing_data = session.query(FundNavDaily).filter(
                FundNavDaily.proj_id.in_([model.proj_id for model in models]),
                FundNavDaily.nav_date.in_([model.nav_date for model in models])
            ).all()

            existing_dict = {(item.proj_id, item.nav_date): item for item in existing_data}

            for model in cleaned_models:
                key = (model.proj_id, model.nav_date)
                if key in existing_dict:
                    # อัปเดตค่าที่มีอยู่แล้ว
                    for column in model.__table__.columns.keys():
                        setattr(existing_dict[key], column, getattr(model, column))
                else:
                    session.add(model)

            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to create or update mutual fund.", e) from e
        finally:
            session.close()
    # def create_or_update(self, model: FundNavDaily):
    #     try:
    #         session = self._database.get_session()
    #         existing_fund_nav_daily = session.query(FundNavDaily).filter(FundNavDaily.proj_id == model.proj_id, FundNavDaily.nav_date == model.nav_date).first()
    #         if existing_fund_nav_daily is None:
    #             session.add(model)
    #         else:
    #             existing_fund_nav_daily.name = model.name
    #             existing_fund_nav_daily.last_upd_date = model.last_upd_date
    #             existing_fund_nav_daily.class_abbr_name = model.class_abbr_name
    #             existing_fund_nav_daily.net_asset = model.net_asset
    #             existing_fund_nav_daily.last_val = model.last_val
    #             existing_fund_nav_daily.previous_val = model.previous_val
    #             existing_fund_nav_daily.unique_id = model.unique_id
    #             existing_fund_nav_daily.sell_price = model.sell_price
    #             existing_fund_nav_daily.buy_price = model.buy_price
    #             existing_fund_nav_daily.sell_swap_price = model.sell_swap_price
    #             existing_fund_nav_daily.buy_swap_price = model.buy_swap_price
    #             existing_fund_nav_daily.remark_th = model.remark_th
    #             existing_fund_nav_daily.remark_en = model.remark_en
    #         session.commit()
    #     except SQLAlchemyError as e:
    #         session.rollback()
    #         raise DatabaseException("Failed to create or update mutual fund.", e) from e
    #     finally:
    #         session.close()
    
    # def create_or_update_all(self, models: list[FundNavDaily]):
    #     try:
    #         session = self._database.get_session()
    #         for model in models:
    #             existing_fund_nav_daily = session.query(FundNavDaily).filter(FundNavDaily.proj_id == model.proj_id, FundNavDaily.nav_date == model.nav_date).first()
    #             if existing_fund_nav_daily is None:
    #                 session.add(model)
    #             else:
    #                 existing_fund_nav_daily.name = model.name
    #                 existing_fund_nav_daily.last_upd_date = model.last_upd_date
    #                 existing_fund_nav_daily.class_abbr_name = model.class_abbr_name
    #                 existing_fund_nav_daily.net_asset = model.net_asset
    #                 existing_fund_nav_daily.last_val = model.last_val
    #                 existing_fund_nav_daily.previous_val = model.previous_val
    #                 existing_fund_nav_daily.unique_id = model.unique_id
    #                 existing_fund_nav_daily.sell_price = model.sell_price
    #                 existing_fund_nav_daily.buy_price = model.buy_price
    #                 existing_fund_nav_daily.sell_swap_price = model.sell_swap_price
    #                 existing_fund_nav_daily.buy_swap_price = model.buy_swap_price
    #                 existing_fund_nav_daily.remark_th = model.remark_th
    #                 existing_fund_nav_daily.remark_en = model.remark_en
    #         session.commit()
    #     except SQLAlchemyError as e:
    #         session.rollback()
    #         raise DatabaseException("Failed to create or update mutual fund.", e) from e
    #     finally:
    #         session.close()
    
    def update(self, model: FundNavDaily):
        try:
            session = self._database.get_session()
            fund_nav_daily = session.query(FundNavDaily).filter(FundNavDaily.proj_id == model.proj_id, FundNavDaily.nav_date == model.nav_date).first()
            fund_nav_daily.name = model.name
            fund_nav_daily.last_upd_date = model.last_upd_date
            fund_nav_daily.class_abbr_name = model.class_abbr_name
            fund_nav_daily.net_asset = model.net_asset
            fund_nav_daily.last_val = model.last_val
            fund_nav_daily.previous_val = model.previous_val
            fund_nav_daily.unique_id = model.unique_id
            fund_nav_daily.sell_price = model.sell_price
            fund_nav_daily.buy_price = model.buy_price
            fund_nav_daily.sell_swap_price = model.sell_swap_price
            fund_nav_daily.buy_swap_price = model.buy_swap_price
            fund_nav_daily.remark_th = model.remark_th
            fund_nav_daily.remark_en = model.remark_en
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to update mutual fund.", e) from e
        finally:
            session.close()
    
    def delete(self, model: FundNavDaily):
        try:
            session = self._database.get_session()
            fund_nav_daily = session.query(FundNavDaily).filter(FundNavDaily.proj_id == model.proj_id, FundNavDaily.nav_date == model.nav_date).first()
            session.delete(fund_nav_daily)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise DatabaseException("Failed to delete mutual fund.", e) from e
        finally:
            session.close()
    
    def get_recent(self, model: FundNavDaily, range=150):
        try:
            session = self._database.get_session()
            fund_nav_daily = session.query(FundNavDaily).filter(FundNavDaily.proj_id == model.proj_id).order_by(FundNavDaily.nav_date.desc()).limit(range).all()
            return fund_nav_daily
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()
    
    def get_by_range(self, proj_id, start_date, end_date):
        try:
            session = self._database.get_session()
            fund_nav_daily = session.query(FundNavDaily).filter(FundNavDaily.proj_id == proj_id, 
                                                                FundNavDaily.nav_date >= start_date, 
                                                                FundNavDaily.nav_date <= end_date) \
                                                        .order_by(FundNavDaily.nav_date.desc()).all()
            return fund_nav_daily
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()

    def get_recent_from_end_date(self, model: FundNavDaily, end_date, range=150):
        try:
            session = self._database.get_session()
            
            # ดึงข้อมูลจากฐานข้อมูลโดยเรียงลำดับจากวันที่เก่าสุดไปยังล่าสุด
            fund_nav_daily = (
                session.query(FundNavDaily)
                .filter(
                    FundNavDaily.proj_id == model.proj_id,
                    FundNavDaily.nav_date <= end_date  # เฉพาะข้อมูลถึง end_date
                )
                .order_by(FundNavDaily.nav_date.desc())  # เรียงจากวันที่เก่าสุดไปล่าสุด
                .limit(range)  # จำกัดจำนวนข้อมูลตาม range
                .all()
            )
            
            # เก็บเฉพาะ nav_date และ last_val
            data = [
                {
                    'date': fund.nav_date.strftime('%Y-%m-%d'),  # แปลงวันที่เป็น string
                    'nav': float(fund.last_val) if fund.last_val is not None else None  # แปลง last_val เป็น float
                }
                for fund in reversed(fund_nav_daily)  # เรียงกลับให้วันที่เก่าสุดอยู่ข้างหน้า
            ]
            
            return data
        
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        
        finally:
            session.close()
            
    # def get_recent_from_back_days_and_end_date(self, model: FundNavDaily, end_date: str, back_days: int, range: int = 150):
    #     try:
    #         session = self._database.get_session()

    #         sql = text("""
    #             SET @rownum := 0;
    #             WITH ordered_data AS (
    #                 SELECT 
    #                     proj_id,
    #                     nav_date,
    #                     last_val,
    #                     name,
    #                     (@rownum := @rownum + 1) AS row_num
    #                 FROM (
    #                     SELECT proj_id, nav_date, last_val, name
    #                     FROM fund_nav_daily
    #                     WHERE proj_id = :proj_id AND nav_date <= :end_date
    #                     ORDER BY nav_date DESC
    #                 ) AS subquery
    #             ),
    #             selected_start AS (
    #                 SELECT MIN(row_num) AS start_row FROM ordered_data WHERE row_num >= :back_days
    #             )
    #             SELECT nav_date, last_val
    #             FROM ordered_data, selected_start
    #             WHERE ordered_data.row_num >= selected_start.start_row
    #             ORDER BY ordered_data.nav_date DESC
    #             LIMIT :range;
    #         """)
    #         # Execute Query
    #         result = session.execute(sql, {
    #             "proj_id": model.proj_id,
    #             "end_date": end_date,
    #             "back_days": back_days,
    #             "range": range
    #         }).fetchall()

    #         # แปลงผลลัพธ์เป็น JSON
    #         data = [
    #             {"date": row.nav_date.strftime('%Y-%m-%d'), "nav": float(row.last_val) if row.last_val is not None else None}
    #             for row in reversed(result)
    #         ]

    #         return data

    #     except Exception as e:
    #         session.rollback()
    #         raise DatabaseException("Failed to retrieve fund data.", e) from e
    #     finally:
    #         session.close()

    def get_recent_from_back_days_and_end_date(self, model: FundNavDaily, end_date, back_days: int, range: int = 150):
        try:
            session: Session = self._database.get_session()

            # แปลง end_date เป็น datetime (ถ้าจำเป็น)
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

            sql = text("""
                WITH ordered_data AS (
                    SELECT 
                        proj_id,
                        nav_date,
                        last_val,
                        name,
                        ROW_NUMBER() OVER (ORDER BY nav_date DESC) AS row_num
                    FROM fund_nav_daily
                    WHERE proj_id = :proj_id AND nav_date <= :end_date
                ),
                selected_start AS (
                    SELECT MIN(row_num) AS start_row FROM ordered_data WHERE row_num > :back_days
                )
                SELECT nav_date, last_val
                FROM ordered_data
                WHERE row_num >= (SELECT start_row FROM selected_start)
                ORDER BY nav_date DESC
                LIMIT :range;
            """)

            # ใช้ context manager เพื่อให้ session ปิดอัตโนมัติ
            with session.begin():
                result = session.execute(sql, {
                    "proj_id": model.proj_id,
                    "end_date": end_date,
                    "back_days": back_days,
                    "range": range
                }).fetchall()

            # แปลงผลลัพธ์เป็น JSON
            data = [
                {
                    "date": row.nav_date.strftime('%Y-%m-%d'),
                    "nav": float(row.last_val) if row.last_val is not None else None
                }
                for row in reversed(result)
            ]

            return data

        except Exception as e:
            session.rollback()
            raise DatabaseException("Failed to retrieve fund data.", e) from e
        finally:
            session.close()


    def prev_last_val(self, model: FundNavDaily):
        try:
            session = self._database.get_session()
            fund_nav_daily = session.query(FundNavDaily).filter(FundNavDaily.proj_id == model.proj_id, FundNavDaily.nav_date < model.nav_date).order_by(FundNavDaily.nav_date.desc()).first()
            return fund_nav_daily
        except SQLAlchemyError as e:
            raise DatabaseException("Failed to retrieve mutual fund by name.", e) from e
        finally:
            session.close()