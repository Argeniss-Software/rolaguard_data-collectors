from sqlalchemy import Column, String, BigInteger
from auditing.db import session
from sqlalchemy.dialects import postgresql, sqlite

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


BigIntegerType = BigInteger().with_variant(postgresql.BIGINT(), 'postgresql')

class TTNRegion(Base):
    __tablename__ = "ttn_region"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    region = Column(String(30), nullable=False, unique=True)
    name = Column(String(30), nullable=False, unique=True)

    @classmethod
    def find_one_by_region(cls, region):
        return session.query(cls).filter(cls.region == region).first()

    @classmethod
    def find_region_by_id(cls, id):
        return session.query(cls).filter(cls.id == id).first().region

    def save(self):
        session.add(self)
        session.flush()
