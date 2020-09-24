from sqlalchemy import Column, String, BigInteger, func
from auditing.db import session
from sqlalchemy.dialects import postgresql, sqlite

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

BigIntegerType = BigInteger().with_variant(postgresql.BIGINT(), 'postgresql')

class Organization(Base):
    __tablename__ = "organization"
    id = Column(BigIntegerType, primary_key=True)
    name = Column(String(120), unique=True)

    @classmethod
    def find_one(cls, id=None):
        query = session.query(cls)
        if id:
            query = query.filter(cls.id == id)
        return query.first()

    @classmethod
    def count(cls):
        return session.query(func.count(cls.id)).scalar()

    def save(self):
        session.add(self)
        session.flush()
        session.commit()
