import os

import cx_Oracle

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def connect_ugo():
    os.environ["NLS_LANG"] = ".UTF8"
    # dsn_tns = cx_Oracle.makedsn('IP', 1521, service_name='sml')  # 172.17.0.1
    dsn_tns = cx_Oracle.makedsn('10.64.25.41', 15120, service_name='dbtasy')  # 172.17.0.1
    return cx_Oracle.connect('HDATA', 'HDATATS2022', dsn_tns)

def connect_hdata():
    os.environ["NLS_LANG"] = ".UTF8"
    dsn_tns = cx_Oracle.makedsn('orclstage-1.cxp7emb18yqw.us-east-2.rds.amazonaws.com', 61521, service_name='orcl')
    return cx_Oracle.connect('UNIMED_GYN', 'UNIMEDGYN', dsn_tns)

def connect():
    connect_hdata_2 = 'oracle+cx_oracle://' + 'UNIMED_GYN' + ':' + 'UNIMEDGYN' + '@' + '/ORCL'
    return connect_hdata_2

def engine():
    engine = create_engine(connect(), max_identifier_length=128)
    return engine

def engine():
    engine = create_engine(connect_hdata(), max_identifier_length=128)
    return engine

Session = sessionmaker(bind=engine)
session = Session()

Session_engine = sessionmaker(bind=engine)
session_engine = Session_engine()