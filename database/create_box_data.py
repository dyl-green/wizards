import json

from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine, Boolean
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import Session, declarative_base
from datetime import datetime

base_url = "postgresql://postgres:Drosser101@localhost:5432/nba_stats"
engine = create_engine(base_url)

teams = [
    "atlanta-hawks",
    "boston-celtics",
    "brooklyn-nets",
    "charlotte-hornets",
    "chicago-bulls",
    "cleveland-cavaliers",
    "dallas-mavericks",
    "denver-nuggets",
    "detroit-pistons",
    "golden-state-warriors",
    "houston-rockets",
    "indiana-pacers",
    "los-angeles-clippers",
    "los-angeles-lakers",
    "memphis-grizzlies",
    "miami-heat",
    "milwaukee-bucks",
    "minnesota-timberwolves",
    "new-orleans-pelicans",
    "new-york-knicks",
    "oklahoma-city-thunder",
    "orlando-magic",
    "philadelphia-76ers",
    "phoenix-suns",
    "portland-trail-blazers",
    "sacramento-kings",
    "san-antonio-spurs",
    "toronto-raptors",
    "utah-jazz",
    "washington-wizards"
]

Base = declarative_base()

def parse_int(value):
    if isinstance(value, int):
        return value
    if value in (None, "", "--", "N/A", "n/a"):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value = value.strip().lower()
        if value in ("true", "yes", "1", "starter", "s"):
            return True
        if value in ("false", "no", "0", "bench", "b"):
            return False
    return None


def parse_string(value):
    if value in (None, "--"):
        return None
    return str(value)


def create_game_data_model(team_name):
    safe_name = team_name.replace("-", "_").replace(" ", "_")

    return type(
        f"GameData_{safe_name}",
        (Base,),
        {
            "__tablename__": f"{team_name}_box_data",
            "id": Column(Integer, primary_key=True, autoincrement=True),
            "game_id": Column(Integer),
            "name": Column(String(100)),
            "team": Column(String(50)),
            "minutes": Column(Integer),
            "points": Column(Integer),
            "field_goal_percentage": Column(String(20)),
            "three_point_percentage": Column(String(20)),
            "free_throw_percentage": Column(String(20)),
            "rebounds": Column(Integer),
            "assists": Column(Integer),
            "turnovers": Column(Integer),
            "steals": Column(Integer),
            "blocks": Column(Integer),
            "o_rebounds": Column(Integer),
            "d_rebounds": Column(Integer),
            "fouls": Column(Integer),
            "plus_minus": Column(String(20)),
            "Starter": Column(Boolean),
            "home_or_away": Column(String(10)),
        },
    )

models = {team: create_game_data_model(team) for team in teams}
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Load your JSON
with open('data/nba_player_data.json', 'r') as f:
    data = json.load(f)



teams = [
    "atlanta-hawks",
    "boston-celtics"
]

true_count = 0
for team in teams:
    GameData = models[team]

    for i in range(1, len(data[team]) + 1):
        true_count += 1
        game_key = f"game_{true_count}"
        print(f"Processing {team} {game_key}")

        for player in data[team][game_key]["home_team"]:
            with Session(engine) as session:
                row = GameData(
                    game_id=true_count,
                    name=player["player"],
                    team=player["team"],
                    minutes=parse_int(player.get("minutes")),
                    points=parse_int(player.get("points")),
                    field_goal_percentage=parse_string(player.get("field_goal_percentage")),
                    three_point_percentage=parse_string(player.get("three_point_percentage")),
                    free_throw_percentage=parse_string(player.get("free_throw_percentage")),
                    rebounds=parse_int(player.get("rebounds")),
                    assists=parse_int(player.get("assists")),
                    turnovers=parse_int(player.get("turnovers")),
                    steals=parse_int(player.get("steals")),
                    blocks=parse_int(player.get("blocks")),
                    o_rebounds=parse_int(player.get("o_rebounds")),
                    d_rebounds=parse_int(player.get("d_rebounds")),
                    fouls=parse_int(player.get("fouls")),
                    plus_minus=parse_string(player.get("plus_minus")),
                    Starter=parse_bool(player.get("Starter")),
                    home_or_away="home"
                )
                session.add(row)
                session.commit()

        for player in data[team][game_key]["away_team"]:
            with Session(engine) as session:
                row = GameData(
                    game_id=true_count,
                    name=player["player"],
                    team=player["team"],
                    minutes=parse_int(player.get("minutes")),
                    points=parse_int(player.get("points")),
                    field_goal_percentage=parse_string(player.get("field_goal_percentage")),
                    three_point_percentage=parse_string(player.get("three_point_percentage")),
                    free_throw_percentage=parse_string(player.get("free_throw_percentage")),
                    rebounds=parse_int(player.get("rebounds")),
                    assists=parse_int(player.get("assists")),
                    turnovers=parse_int(player.get("turnovers")),
                    steals=parse_int(player.get("steals")),
                    blocks=parse_int(player.get("blocks")),
                    o_rebounds=parse_int(player.get("o_rebounds")),
                    d_rebounds=parse_int(player.get("d_rebounds")),
                    fouls=parse_int(player.get("fouls")),
                    plus_minus=parse_string(player.get("plus_minus")),
                    Starter=parse_bool(player.get("Starter")),
                    home_or_away="away"
                )
                session.add(row)
                session.commit()

        print(f"Added data for {team} {game_key}")

print(f"Finished processing {true_count} games.")

            
        
# with Session(engine) as session:
#     game_count = 1

#     for i in range(1, len(data)+1):
#         for player in range(len(data[f"game_{game_count}"]["home_team"])):
#             row = GameData(
#                 game_id = game_count,
#                 name=data[f"game_{game_count}"]["home_team"][player]["player"],
#                 team=data[f"game_{game_count}"]["home_team"][player]["team"],
#                 minutes=data[f"game_{game_count}"]["home_team"][player]["minutes"],
#                 points=data[f"game_{game_count}"]["home_team"][player]["points"],
#                 field_goal_percentage=data[f"game_{game_count}"]["home_team"][player]["field_goal_percentage"],
#                 three_point_percentage=data[f"game_{game_count}"]["home_team"][player]["three_point_percentage"],
#                 free_throw_percentage=data[f"game_{game_count}"]["home_team"][player]["free_throw_percentage"],
#                 rebounds=data[f"game_{game_count}"]["home_team"][player]["rebounds"],
#                 assists=data[f"game_{game_count}"]["home_team"][player]["assists"],
#                 turnovers=data[f"game_{game_count}"]["home_team"][player]["turnovers"],
#                 steals=data[f"game_{game_count}"]["home_team"][player]["steals"],
#                 blocks=data[f"game_{game_count}"]["home_team"][player]["blocks"],
#                 o_rebounds=data[f"game_{game_count}"]["home_team"][player]["o_rebounds"],
#                 d_rebounds=data[f"game_{game_count}"]["home_team"][player]["d_rebounds"],
#                 fouls=data[f"game_{game_count}"]["home_team"][player]["fouls"],
#                 plus_minus=data[f"game_{game_count}"]["home_team"][player]["plus_minus"],
#                 Starter=data[f"game_{game_count}"]["home_team"][player]["Starter"],
#                 home_or_away="home"
#             )
#             session.add(row)
#         for player in range(len(data[f"game_{game_count}"]["away_team"])):
#             row = GameData(
#                 game_id = game_count,
#                 name=data[f"game_{game_count}"]["away_team"][player]["player"],
#                 team=data[f"game_{game_count}"]["away_team"][player]["team"],
#                 minutes=data[f"game_{game_count}"]["away_team"][player]["minutes"],
#                 points=data[f"game_{game_count}"]["away_team"][player]["points"],
#                 field_goal_percentage=data[f"game_{game_count}"]["away_team"][player]["field_goal_percentage"],
#                 three_point_percentage=data[f"game_{game_count}"]["away_team"][player]["three_point_percentage"],
#                 free_throw_percentage=data[f"game_{game_count}"]["away_team"][player]["free_throw_percentage"],
#                 rebounds=data[f"game_{game_count}"]["away_team"][player]["rebounds"],
#                 assists=data[f"game_{game_count}"]["away_team"][player]["assists"],
#                 turnovers=data[f"game_{game_count}"]["away_team"][player]["turnovers"],
#                 steals=data[f"game_{game_count}"]["away_team"][player]["steals"],
#                 blocks=data[f"game_{game_count}"]["away_team"][player]["blocks"],
#                 o_rebounds=data[f"game_{game_count}"]["away_team"][player]["o_rebounds"],
#                 d_rebounds=data[f"game_{game_count}"]["away_team"][player]["d_rebounds"],
#                 fouls=data[f"game_{game_count}"]["away_team"][player]["fouls"],
#                 plus_minus=data[f"game_{game_count}"]["away_team"][player]["plus_minus"],
#                 Starter=data[f"game_{game_count}"]["away_team"][player]["Starter"],
#                 home_or_away="away"

#             )
#             session.add(row)
#         print(f"Added data for game_{game_count}")
#         game_count += 1
#     session.commit()
#     print(f"Inserted {len(data)} rows successfully.")
