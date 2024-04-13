from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql://postgres:lavkar@localhost/postgres"
Base = declarative_base()


engine = create_engine(DATABASE_URL)

try:
  # Attempt a connection
  engine.connect()
  print("Database connection successful!")
except Exception as e:
  print("Database connection error:", e)
  exit(1)  # Exit the application on connection failure

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)