import json

from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine, Boolean
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import Session, declarative_base
from datetime import datetime

base_url = "postgresql://postgres:Drosser101@localhost:5432/wizards"
engine = create_engine(base_url)



Base = declarative_base()
class GameData(Base):
    __tablename__ = 'wizards_box_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(Integer)
    name = Column(String(100))
    minutes = Column(Integer)
    points = Column(Integer)
    field_goal_percentage = Column(String(20))
    three_point_percentage = Column(String(20))
    free_throw_percentage = Column(String(20))
    rebounds = Column(Integer)
    assists = Column(Integer)
    turnovers = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    o_rebounds = Column(Integer)
    d_rebounds = Column(Integer)
    fouls = Column(Integer)
    plus_minus = Column(String(20))
    Starter = Column(Boolean)
    home_or_away = Column(String(10))


# Recreate table in the 'wizards' database (drops existing table)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Load your JSON
with open('wizards_data_2026-05-03.json', 'r') as f:
    data = json.load(f)



with Session(engine) as session:
    game_count = 1

    for i in range(1, len(data)):
        for player in range(len(data[f"game_{game_count}"]["home_team"])):
            row = GameData(
                game_id = game_count,
                name=data[f"game_{game_count}"]["home_team"][player]["player"],
                minutes=data[f"game_{game_count}"]["home_team"][player]["minutes"],
                points=data[f"game_{game_count}"]["home_team"][player]["points"],
                field_goal_percentage=data[f"game_{game_count}"]["home_team"][player]["field_goal_percentage"],
                three_point_percentage=data[f"game_{game_count}"]["home_team"][player]["three_point_percentage"],
                free_throw_percentage=data[f"game_{game_count}"]["home_team"][player]["free_throw_percentage"],
                rebounds=data[f"game_{game_count}"]["home_team"][player]["rebounds"],
                assists=data[f"game_{game_count}"]["home_team"][player]["assists"],
                turnovers=data[f"game_{game_count}"]["home_team"][player]["turnovers"],
                steals=data[f"game_{game_count}"]["home_team"][player]["steals"],
                blocks=data[f"game_{game_count}"]["home_team"][player]["blocks"],
                o_rebounds=data[f"game_{game_count}"]["home_team"][player]["o_rebounds"],
                d_rebounds=data[f"game_{game_count}"]["home_team"][player]["d_rebounds"],
                fouls=data[f"game_{game_count}"]["home_team"][player]["fouls"],
                plus_minus=data[f"game_{game_count}"]["home_team"][player]["plus_minus"],
                Starter=data[f"game_{game_count}"]["home_team"][player]["Starter"],
                home_or_away="home"
            )
            session.add(row)
        for player in range(len(data[f"game_{game_count}"]["away_team"])):
            row = GameData(
                game_id = game_count,
                name=data[f"game_{game_count}"]["away_team"][player]["player"],
                minutes=data[f"game_{game_count}"]["away_team"][player]["minutes"],
                points=data[f"game_{game_count}"]["away_team"][player]["points"],
                field_goal_percentage=data[f"game_{game_count}"]["away_team"][player]["field_goal_percentage"],
                three_point_percentage=data[f"game_{game_count}"]["away_team"][player]["three_point_percentage"],
                free_throw_percentage=data[f"game_{game_count}"]["away_team"][player]["free_throw_percentage"],
                rebounds=data[f"game_{game_count}"]["away_team"][player]["rebounds"],
                assists=data[f"game_{game_count}"]["away_team"][player]["assists"],
                turnovers=data[f"game_{game_count}"]["away_team"][player]["turnovers"],
                steals=data[f"game_{game_count}"]["away_team"][player]["steals"],
                blocks=data[f"game_{game_count}"]["away_team"][player]["blocks"],
                o_rebounds=data[f"game_{game_count}"]["away_team"][player]["o_rebounds"],
                d_rebounds=data[f"game_{game_count}"]["away_team"][player]["d_rebounds"],
                fouls=data[f"game_{game_count}"]["away_team"][player]["fouls"],
                plus_minus=data[f"game_{game_count}"]["away_team"][player]["plus_minus"],
                Starter=data[f"game_{game_count}"]["away_team"][player]["Starter"],
                home_or_away="away"

            )
            session.add(row)
        print(f"Added data for game_{game_count}")
        game_count += 1
    session.commit()
    print(f"Inserted {len(data)} rows successfully.")
