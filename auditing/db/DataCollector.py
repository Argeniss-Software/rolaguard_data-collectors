from enum import Enum

from sqlalchemy import Column, DateTime, String, BigInteger, Boolean, ForeignKey, func, Enum as SQLEnum
from auditing.db import session
from sqlalchemy.dialects import postgresql, sqlite

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

BigIntegerType = BigInteger().with_variant(postgresql.BIGINT(), 'postgresql')


class DataCollectorStatus(Enum):
    CONNECTED = 'CONNECTED'
    DISCONNECTED = 'DISCONNECTED'
    DISABLED = 'DISABLED'

class DataCollector(Base):
    __tablename__ = "data_collector"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    data_collector_type_id = Column(BigInteger, ForeignKey("data_collector_type.id"), nullable=False)
    name = Column(String(120), nullable=False)
    description = Column(String(1000), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    ip = Column(String(120), nullable=True)
    port = Column(String(120), nullable=True)
    user = Column(String(120), nullable=False)
    password = Column(String(120), nullable=False)
    ssl = Column(Boolean, nullable=True)
    gateway_id = Column(String(100), nullable=True)
    organization_id = Column(BigInteger, ForeignKey("organization.id"), nullable=False)
    policy_id = Column(BigInteger, ForeignKey("policy.id"), nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(SQLEnum(DataCollectorStatus))
    verified = Column(Boolean, nullable=False, default=False)

    @classmethod
    def find_one_by_ip_port_and_dctype_id(cls, dctype_id, ip, port):
        return session.query(cls).filter(cls.ip == ip).filter(cls.data_collector_type_id == dctype_id).filter(cls.port == port).first()
    
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
