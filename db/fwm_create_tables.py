from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import fwm_config as fwm_config

# Configuration containing the DB name and login credentials
config = fwm_config.Config()

print config.FWM_DB
# Initialize
config.initDB()
# create the tables
config.create_tables()
