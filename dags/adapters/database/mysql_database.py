from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from adapters.exceptions import DatabaseException
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from abc import ABCMeta, abstractmethod

Base = declarative_base()

class MysqlDatabase:
    def __init__(self, uri):
        self.uri = uri
        try:
            # พยายามสร้าง engine และเชื่อมต่อ
            self.engine = create_engine(uri, echo=True)
            # ทดสอบการเชื่อมต่อด้วยการสร้าง session
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))  # Query เบื้องต้นเพื่อทดสอบการเชื่อมต่อ
            self.Session = sessionmaker(bind=self.engine)
        except OperationalError as e:
            raise DatabaseException(text(f"Error connecting to the database: {e}"), e)
        except Exception as e:
            raise DatabaseException(text(f"An error occurred: {e}"), e)

    def get_session(self):
        try:
            session = self.Session()
            return session
        except Exception as e:
            raise DatabaseException(error=e)
    
    def create_tables_if_not_exists(self):
        """ตรวจสอบและสร้างตารางใหม่ในฐานข้อมูล"""
        try:
            inspector = inspect(self.engine)
            existing_tables = inspector.get_table_names()
            print(f"Existing tables: {existing_tables}")

            Base.metadata.create_all(self.engine)  # ตรวจสอบและสร้างตารางใหม่
            print("Tables created successfully!")
        except SQLAlchemyError as e:
            print(f"Error creating tables: {e}")

        
        

