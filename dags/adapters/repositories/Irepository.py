from abc import ABC, abstractmethod
from adapters.models.Imodel import IModel
class IRepository(ABC):
    @property
    @abstractmethod
    def database(self):
        pass
    
    @property
    @abstractmethod
    def model_class(self):
        """
        กำหนดให้แต่ละ Repository ระบุโมเดลที่เกี่ยวข้อง เช่น `FundNavDaily`
        """
        pass
    
    @abstractmethod
    def get(self, model: IModel):
        pass

    @abstractmethod
    def create(self, model: IModel):
        pass
    
    @abstractmethod
    def create_all(self, models: list[IModel]):
        pass
    @abstractmethod
    def create_or_update(self, model: IModel):
        pass
    @abstractmethod
    def create_or_update_all(self, models: list[IModel]):
        pass
    @abstractmethod
    def update(self, model: IModel):
        pass

    @abstractmethod
    def delete(self, model: IModel):
        pass

class IDateRepository(IRepository):
    @abstractmethod
    def get_recent(self, model: IModel, range):
        pass
    
    @abstractmethod
    def get_recent_from_end_date(self, model: IModel, end_date, range):
        pass