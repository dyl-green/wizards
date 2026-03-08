from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine, Boolean
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import declarative_base
from datetime import datetime

# 1. Connect to the default 'postgres' DB to create the new one
base_url = "postgresql://postgres:Drosser101@localhost:5432/"
new_db_name = "wizards"
engine = create_engine(base_url + "postgres")

# 2. Use sqlalchemy-utils to create the database if it doesn't exist
# pip install sqlalchemy-utils
target_url = base_url + new_db_name
if not database_exists(target_url):
    create_database(target_url)
    print(f"Database '{new_db_name}' created.")

