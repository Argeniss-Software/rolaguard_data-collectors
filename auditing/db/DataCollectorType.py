from sqlalchemy import Column, String, BigInteger
from auditing.db import session
from sqlalchemy.dialects import postgresql, sqlite

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


BigIntegerType = BigInteger().with_variant(postgresql.BIGINT(), 'postgresql')

class DataCollectorType(Base):
    __tablename__ = "data_collector_type"
    id = Column(BigIntegerType, primary_key=True, autoincrement=True)
    type = Column(String(30), nullable=False, unique=True)
    name = Column(String(50), nullable=False)
    
    @classmethod
    def find_one_by_type(cls, type):
        return session.query(cls).filter(cls.type == type).first()

    @classmethod
    def find_type_by_id(cls, id):
        return session.query(cls).filter(cls.id == id).first().type

    def save(self):
        session.add(self)
        session.flush()
