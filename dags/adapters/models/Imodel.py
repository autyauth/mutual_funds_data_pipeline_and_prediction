from abc import ABC, abstractmethod

class IModel():
    # @abstractmethod
    def __repr__(self):
        pass

    # @abstractmethod
    def to_dict(self):
        pass
    
    # @classmethod
    @abstractmethod
    def to_model(cls, data: dict):
        """
        แปลง dictionary ให้กลายเป็น instance ของ model
        """
        pass