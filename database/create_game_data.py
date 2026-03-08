import json

from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine, Boolean
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import Session, declarative_base
from datetime import datetime

base_url = "postgresql://postgres:Drosser101@localhost:5432/wizards"
engine = create_engine(base_url)



Base = declarative_base()
class Data(Base):
    __tablename__ = 'wizards_game_data'

    game_id = Column(Integer, primary_key=True, autoincrement=True)
    game_date = Column(String(20))
    game_time = Column(String(20))
    game_location = Column(String(100))
    game_attandence = Column(String(20))
    game_refs = Column(String(200))


# Recreate table in the 'wizards' database (drops existing table)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Load your JSON
with open('data/wizards_game_data.json', 'r') as f:
    data = json.load(f)



with Session(engine) as session:
    game_count = 1
    for i in range(1, len(data)+1):
        row = Data(
            game_date = data[f"game_{game_count}"]["game_date"],
            game_time = data[f"game_{game_count}"]["game_time"],
            game_location = data[f"game_{game_count}"]["game_location"],
            game_attandence = data[f"game_{game_count}"]["game_attandence"],
            game_refs = data[f"game_{game_count}"]["game_refs"]
        )
        session.add(row)
        print(f"Added data for game_{game_count}")
        game_count += 1
    session.commit()
    print(f"Inserted {len(data)} rows successfully.")
