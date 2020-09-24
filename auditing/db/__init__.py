from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os


DB_HOST = os.environ["DB_HOST"] 
DB_NAME = os.environ["DB_NAME"] 
DB_USERNAME = os.environ["DB_USERNAME"] 
DB_PASSWORD = os.environ["DB_PASSWORD"] 
DB_PORT = os.environ["DB_PORT"] 

engine = create_engine('postgresql+psycopg2://{user}:{pw}@{url}:{port}/{db}'.\
    format(user=DB_USERNAME, pw=DB_PASSWORD, url=DB_HOST, port=DB_PORT, db=DB_NAME))

sessionBuilder = sessionmaker()
sessionBuilder.configure(bind=engine)
session = sessionBuilder()

def commit():
    session.commit()

def begin():
    session.begin()

def rollback():
    session.rollback()


from .DataCollector import DataCollector
from .DataCollectorType import DataCollectorType
from .Organization import Organization