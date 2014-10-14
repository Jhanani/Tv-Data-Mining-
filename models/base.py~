from sqlalchemy import Column, Boolean, Integer, BigInteger, String, DateTime, TIMESTAMP, Index, BINARY, Date, func, Time, Text, Boolean, Table, ForeignKey, Float, Numeric
from sqlalchemy import and_, or_, event, text
from sqlalchemy.dialects.mysql.base import LONGTEXT
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.schema import UniqueConstraint, ForeignKeyConstraint

import base64
import re

# Class for FWM Data for the month of April
def FourthWallMediaTV_04_2014(base):
    class FourthWallMediaTV_04_2014(base):
        __bind_key__ = 'fwm'
        __tablename__ = 'fwm_tv_04_2014'
        
        id = Column(Integer, primary_key=True)
        # By default numeric would be 10 digits, Decimal(M,D) where M=5, and D=0 would result in 5 digit MSO
        ##mso = Column(Numeric(precision=5), nullable=True)
        mso = Column(Integer, nullable=True)
        device_id = Column(String(24), nullable=False)
        event_date = Column(Date, nullable=False)           # This is LOCAL date
        event_day = Column(String(12), nullable=True)
        event_time = Column(Time, nullable=False)           # This is LOCAL time
        event_type = Column(String(1), nullable=True)       # Toggle, Key Press, etc..
        event_value = Column(Numeric(precision=5), nullable=True)        # Channel Number
        event_name = Column(String(32), nullable=True)      # Station Name
        event_id = Column(Integer, nullable=True)          # Station ID, This field will not be available if the device is OFF

        # Program information
        program_tms_id = Column(String(14), nullable=True)  # Program TMS ID
        program_title = Column(String(1024), nullable=True)
        program_start_time = Column(Time, nullable=True)    # Start datetime of the program, This comes from SONAR database, IMP: This is always UTC
        program_duration = Column(Float, nullable=True)     # Program duration (I guess sonar database has this in minutes), This comes from SONAR database
        program_viewtime = Column(Float, nullable=True)     # Program viewtime in seconds
        program_genre = Column(Text, nullable=True)         # Comma seperated list of genres, e.g., ",sitcom,drama,". Notice the ',' at the beginning and end

    return FourthWallMediaTV_04_2014

# Class for FWM Data for the month of May
def FourthWallMediaTV_05_2014(base):
    class FourthWallMediaTV_05_2014(base):
        __bind_key__ = 'fwm'
        __tablename__ = 'fwm_tv_05_2014'
        
        id = Column(Integer, primary_key=True)
        # By default numeric would be 10 digits, Decimal(M,D) where M=5, and D=0 would result in 5 digit MSO
        mso = Column(Integer, nullable=True)
        device_id = Column(String(24), nullable=False)
        event_date = Column(Date, nullable=False)           # This is LOCAL date
        event_day = Column(String(12), nullable=True)
        event_time = Column(Time, nullable=False)           # This is LOCAL time
        event_type = Column(String(1), nullable=True)       # Toggle, Key Press, etc..
        event_value = Column(Numeric(precision=5), nullable=True)        # Channel Number
        event_name = Column(String(32), nullable=True)      # Station Name
        event_id = Column(Integer, nullable=True)          # Station ID, This field will not be available if the device is OFF

        # Program information
        program_tms_id = Column(String(14), nullable=True)  # Program TMS ID
        program_title = Column(String(1024), nullable=True)
        program_start_time = Column(Time, nullable=True)    # Start datetime of the program, This comes from SONAR database, IMP: This is always UTC
        program_duration = Column(Float, nullable=True)     # Program duration (I guess sonar database has this in minutes), This comes from SONAR database
        program_viewtime = Column(Float, nullable=True)     # Program viewtime in seconds
        program_genre = Column(Text, nullable=True)         # Comma seperated list of genres, e.g., ",sitcom,drama,". Notice the ',' at the beginning and end

    return FourthWallMediaTV_05_2014

# Class for FWM Data for the month of June
def FourthWallMediaTV_06_2014(base):
    class FourthWallMediaTV_06_2014(base):
        __bind_key__ = 'fwm'
        __tablename__ = 'fwm_tv_06_2014'
        
        id = Column(Integer, primary_key=True)
        # By default numeric would be 10 digits, Decimal(M,D) where M=5, and D=0 would result in 5 digit MSO
        mso = Column(Integer, nullable=True)
        device_id = Column(String(24), nullable=False)
        event_date = Column(Date, nullable=False)           # This is LOCAL date
        event_day = Column(String(12), nullable=True)
        event_time = Column(Time, nullable=False)           # This is LOCAL time
        event_type = Column(String(1), nullable=True)       # Toggle, Key Press, etc..
        event_value = Column(Numeric(precision=5), nullable=True)        # Channel Number
        event_name = Column(String(32), nullable=True)      # Station Name
        event_id = Column(Integer, nullable=True)          # Station ID, This field will not be available if the device is OFF

        # Program information
        program_tms_id = Column(String(14), nullable=True)  # Program TMS ID
        program_title = Column(String(1024), nullable=True)
        program_start_time = Column(Time, nullable=True)    # Start datetime of the program, This comes from SONAR database, IMP: This is always UTC
        program_duration = Column(Float, nullable=True)     # Program duration (I guess sonar database has this in minutes), This comes from SONAR database
        program_viewtime = Column(Float, nullable=True)     # Program viewtime in seconds
        program_genre = Column(Text, nullable=True)         # Comma seperated list of genres, e.g., ",sitcom,drama,". Notice the ',' at the beginning and end

    return FourthWallMediaTV_06_2014

## The reason I am not using this is because: It is possible to have same program on the same day but at different times
##  In such a scenario, I can populate air_date in one pass and start_datetime on the second pass because, for the same air_date
##  there can be two start_datetime
## Class for FWM Programs Data
#def FourthWallMediaPrograms(base):
#    class FourthWallMediaPrograms(base):
#        __bind_key__ = 'fwm_programs'
#        __tablename__ = 'fwm_programs'
#        
#        # Program information
#        id = Column(Integer, primary_key=True)
#        tms_id = Column(String(14), nullable=True)          # Program TMS ID
#        title = Column(String(1024), nullable=True)
#        genre = Column(TEXT, nullable=True)                 # Comma seperated list of genres, e.g., ",sitcom,drama,". Notice the ',' at the beginning and end
#        #?? program_air_date = Column(Date, nullable=True)  # Date when program was aired or viewed
#        start_datetime = Column(DateTime, nullable=True)    # Start datetime of the program
#        duration = Column(Float, nullable=True)             # Program duration (I guess sonar database has this in minutes)
#
#    return FourthWallMediaPrograms

## Class for FWM Household data
#def FourthWallMediaHH(base):
#    class FourthWallMediaHH(base):
#        __bind_key__ = 'fwm_hh'
#        __tablename__ = 'fwm_hh'
#
#    return FourthWallMediaHH
