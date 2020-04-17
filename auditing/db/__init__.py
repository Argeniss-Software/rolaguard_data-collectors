from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os


DB_HOST = os.environ["DB_HOST"] 
DB_NAME = os.environ["DB_NAME"] 
DB_USERNAME = os.environ["DB_USERNAME"] 
DB_PASSWORD = os.environ["DB_PASSWORD"] 
DB_PORT = os.environ["DB_PORT"] 

engine = create_engine('postgresql+psycopg2://{user}:{pw}@{url}:{port}/{db}'.format(user=DB_USERNAME, pw=DB_PASSWORD, url=DB_HOST, port= DB_PORT, db=DB_NAME))
# If you'd like to use sqlite <---


# Uncomment these lines if you want to work with sqlite instead of postgres
# engine = create_engine('sqlite:///orm_in_detail.sqlite')
# os.environ["ENVIRONMENT"] = "DEV"

Base = declarative_base()
sessionBuilder = sessionmaker()
sessionBuilder.configure(bind=engine)
session = sessionBuilder()

from auditing.db.Models import AlertType, RowProcessed, rollback
import logging

if os.environ.get("ENVIRONMENT") == "DEV":
    logging.getLogger().setLevel(logging.DEBUG)
else:
    logging.getLogger().setLevel(logging.INFO)

try:
    if RowProcessed.count() == 0:
        RowProcessed(last_row= 0, analyzer= 'bruteforcer').save_and_flush()
        RowProcessed(last_row= 0, analyzer= 'packet_analyzer').save_and_flush()
        RowProcessed(last_row= 0, analyzer= 'printer').save_and_flush()

except Exception as exc:
    logging.error(f'Error at commit when initializing: {exc}')
    logging.info('Rolling back the session')
    rollback()