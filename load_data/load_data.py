from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import json
import pandas as pd



with open('wizards_data_2026-05-03.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 2. Define the connection string (replace with your details)
# Format: 'postgresql://username:password@host:port/database_name'
base_url = "postgresql://postgres:Drosser101@localhost:5432/wizards_games"
engine = create_engine(base_url)

# Insert rows
with Session(engine) as session:
    for player in data:
        row = GameData(
            name=player['name'],
            jersey=player['jersey'],
            minutes=player['minutes'],
            points=player['points'],
            field_goal_percentage=player['field_goal_percentage'],
            three_point_percentage=player['three_point_percentage'],
            free_throw_percentage=player['free_throw_percentage'],
            rebounds=player['rebounds'],
            assists=player['assists'],
            turnovers=player['turnovers'],
            steals=player['steals'],
            blocks=player['blocks'],
            o_rebounds=player['o_rebounds'],
            d_rebounds=player['d_rebounds'],
            fouls=player['fouls'],
            plus_minus=player['plus_minus'],
            Starter=player['Starter']
        )
        session.add(row)
    session.commit()
    print(f"Inserted {len(data)} rows successfully.")