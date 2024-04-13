from sqlalchemy import Float, Column, Integer, String, DateTime, Time, Boolean, func, PrimaryKeyConstraint, ForeignKeyConstraint
from database import Base
from datetime import datetime, timedelta

class StoreTimeZone(Base):
    __tablename__ = "Store_Time_Zone"
    store_id = Column(Integer, primary_key=True, index=True)
    timezone_str = Column(String, nullable=False)

class StoreStatus(Base):
    __tablename__ = "Store_Status"
    store_id = Column(Integer, index=True)
    timestamp_utc = Column(DateTime, index=True)
    status = Column(String, index=True)
    __table_args__ = (
        PrimaryKeyConstraint('store_id', 'timestamp_utc'),
        ForeignKeyConstraint(['store_id'], ['Stores.id'])
    )

class StoreActivity(Base):
    __tablename__ = "Store_Activity"
    store_id = Column(Integer, primary_key=True,  index=True)
    day = Column(Integer)
    start_time_local = Column(Time)
    end_time_local = Column(Time)

class Report(Base):
    __tablename__ = "reports"
    store_id = Column(Integer, primary_key=True)
    uptime_last_hour_minutes = Column(Integer)
    uptime_last_day_hours = Column(Float)
    uptime_last_week = Column(Float)
    downtime_last_hour_minutes = Column(Integer)
    downtime_last_day_hours = Column(Float)
    downtime_last_week = Column(Float)