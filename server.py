"""
Wizards Scout — Flask Backend
------------------------------
Reads from PostgreSQL using environment variables:
  
Run:
  pip install flask psycopg2-binary flask-cors
  python server.py
"""
import os
from pathlib import Path
from collections import defaultdict

# Load .env from same directory as this file
_env = Path(__file__).parent / ".env"
if _env.exists():
    for _line in _env.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

MONTH_ORDER = ['October','November','December','January','February','March',
               'April','May','June','July','August','September']

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST","localhost"),
        port=int(os.environ.get("DB_PORT",5432)),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

def clean_date(s):
    if not s: return ""
    return str(s).strip().lstrip(', ').strip()

def parse_date_sort(s):
    """Return a sort key (year, month_idx, day) from 'Month DD, YYYY'."""
    try:
        s = clean_date(s)
        parts = s.replace(',','').split()
        month, day, year = parts[0], int(parts[1]), int(parts[2])
        mi = MONTH_ORDER.index(month) if month in MONTH_ORDER else 99
        return (year, mi, day)
    except:
        return (9999,99,99)

def parse_split(s):
    try:
        made, att = str(s).split("-")
        return int(made), int(att)
    except:
        return 0, 0

def parse_pm(s):
    try:
        return int(str(s).replace("+",""))
    except:
        return 0

def month_label(date_str):
    try:
        s = clean_date(date_str)
        parts = s.replace(',','').split()
        return f"{parts[0][:3]} {parts[2][2:]}"   # e.g. "Oct 25"
    except:
        return "?"

DATE_SORT_SQL = "TO_DATE(TRIM(LEADING ', ' FROM TRIM(game_date)), 'Month DD, YYYY')"
G_DATE_SORT_SQL = "TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)), 'Month DD, YYYY')"

# ── Summary ────────────────────────────────────────────────────────────────────
@app.route("/api/summary")
def get_summary():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT game_id) FROM wizards_game_data")
            games = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT name) FROM wizards_box_data WHERE team <> 'Washington Wizards'")
            players = cur.fetchone()[0]
        return jsonify({"games": games, "players": players})
    finally:
        conn.close()

# ── Teams list ─────────────────────────────────────────────────────────────────
@app.route("/api/teams")
def get_teams():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT team FROM wizards_box_data WHERE team <> 'Washington Wizards' ORDER BY team ASC")
            return jsonify([r[0] for r in cur.fetchall()])
    finally:
        conn.close()

# ── Wizards roster list ────────────────────────────────────────────────────────
@app.route("/api/wizards")
def get_wizards():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT name, team, COUNT(*) AS games_played,
                    ROUND(AVG(points),1) AS avg_pts, ROUND(AVG(rebounds),1) AS avg_reb,
                    ROUND(AVG(assists),1) AS avg_ast,
                    SUM(CAST(split_part(field_goal_percentage,'-',1) AS INT)) AS fgm,
                    SUM(CAST(split_part(field_goal_percentage,'-',2) AS INT)) AS fga,
                    SUM(CAST(split_part(three_point_percentage,'-',1) AS INT)) AS tpm,
                    SUM(CAST(split_part(three_point_percentage,'-',2) AS INT)) AS tpa,
                    SUM(CAST(split_part(free_throw_percentage,'-',1) AS INT))  AS ftm,
                    SUM(CAST(split_part(free_throw_percentage,'-',2) AS INT))  AS fta,
                    SUM(CASE WHEN "Starter" THEN 1 ELSE 0 END) AS starts,
                    ROUND(AVG(plus_minus::numeric),1) AS avg_pm
                FROM wizards_box_data WHERE team = 'Washington Wizards'
                GROUP BY name, team ORDER BY avg_pts DESC
            """)
            rows = cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d['fg_pct'] = round(d['fgm']/d['fga']*100,1) if d['fga'] else 0.0
            d['tp_pct'] = round(d['tpm']/d['tpa']*100,1) if d['tpa'] else 0.0
            d['ft_pct'] = round(d['ftm']/d['fta']*100,1) if d['fta'] else 0.0
            result.append(d)
        return jsonify(result)
    finally:
        conn.close()

# ── Wizards player detail ──────────────────────────────────────────────────────
@app.route("/api/wizards/<path:player_name>")
def get_wizards_player(player_name):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT b.game_id, b.name, b.team, b.minutes, b.points,
                    b.field_goal_percentage, b.three_point_percentage, b.free_throw_percentage,
                    b.rebounds, b.assists, b.steals, b.blocks, b.turnovers, b.fouls,
                    b.o_rebounds, b.d_rebounds, b.plus_minus, b."Starter", b.home_or_away,
                    g.game_date, g.game_location, g.game_attandence,
                    MAX(CASE WHEN b2.team <> 'Washington Wizards' THEN b2.team END) AS opponent,
                    SUM(CASE WHEN b2.team = 'Washington Wizards' THEN b2.points ELSE 0 END) AS wiz_pts,
                    SUM(CASE WHEN b2.team <> 'Washington Wizards' THEN b2.points ELSE 0 END) AS opp_pts
                FROM wizards_box_data b
                LEFT JOIN wizards_game_data g USING (game_id)
                LEFT JOIN wizards_box_data b2 ON b.game_id = b2.game_id
                WHERE b.name = %s AND b.team = 'Washington Wizards'
                GROUP BY b.game_id, b.name, b.team, b.minutes, b.points,
                    b.field_goal_percentage, b.three_point_percentage, b.free_throw_percentage,
                    b.rebounds, b.assists, b.steals, b.blocks, b.turnovers, b.fouls,
                    b.o_rebounds, b.d_rebounds, b.plus_minus, b."Starter", b.home_or_away,
                    g.game_date, g.game_location, g.game_attandence
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)), 'Month DD, YYYY') DESC
            """, (player_name,))
            rows = cur.fetchall()
        if not rows:
            return jsonify({"error": "Not found"}), 404
        games = []
        for r in rows:
            fgm,fga = parse_split(r["field_goal_percentage"])
            tpm,tpa = parse_split(r["three_point_percentage"])
            ftm,fta = parse_split(r["free_throw_percentage"])
            games.append({
                "game_id": r["game_id"],
                "date": clean_date(r["game_date"]),
                "location": clean_date(r["game_location"]),
                "home_or_away": r["home_or_away"] or "",
                "min": int(r["minutes"] or 0), "pts": int(r["points"] or 0),
                "reb": int(r["rebounds"] or 0), "ast": int(r["assists"] or 0),
                "stl": int(r["steals"] or 0),  "blk": int(r["blocks"] or 0),
                "tov": int(r["turnovers"] or 0),"pf":  int(r["fouls"] or 0),
                "oreb":int(r["o_rebounds"] or 0),"dreb":int(r["d_rebounds"] or 0),
                "pm":  parse_pm(r["plus_minus"]),
                "fgm":fgm,"fga":fga,"tpm":tpm,"tpa":tpa,"ftm":ftm,"fta":fta,
                "starter": bool(r["Starter"]),
                "month": month_label(r["game_date"]),
                "sort_key": parse_date_sort(r["game_date"]),
                "opponent": r["opponent"] or "",
                "wiz_pts": int(r["wiz_pts"] or 0),
                "opp_pts": int(r["opp_pts"] or 0),
                "result": "W" if (r["wiz_pts"] or 0) > (r["opp_pts"] or 0) else "L",
            })
        return jsonify({"name": rows[0]["name"], "team": "Washington Wizards", "games": games})
    finally:
        conn.close()

# ── Opponents list ─────────────────────────────────────────────────────────────
@app.route("/api/players")
def get_players():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT name, team, COUNT(*) AS games_played
                FROM wizards_box_data WHERE team <> 'Washington Wizards'
                GROUP BY name, team ORDER BY games_played DESC, name ASC
            """)
            return jsonify([dict(r) for r in cur.fetchall()])
    finally:
        conn.close()

# ── Opponent player detail ─────────────────────────────────────────────────────
@app.route("/api/player/<path:player_name>")
def get_player(player_name):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT b.game_id, b.name, b.team, b.minutes, b.points,
                    b.field_goal_percentage, b.three_point_percentage, b.free_throw_percentage,
                    b.rebounds, b.assists, b.steals, b.blocks, b.turnovers, b.fouls,
                    b.o_rebounds, b.d_rebounds, b.plus_minus, b."Starter", b.home_or_away,
                    g.game_date, g.game_location
                FROM wizards_box_data b
                LEFT JOIN wizards_game_data g USING (game_id)
                WHERE b.name = %s AND b.team <> 'Washington Wizards'
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)), 'Month DD, YYYY') DESC
            """, (player_name,))
            rows = cur.fetchall()
        if not rows:
            return jsonify({"error": "Not found"}), 404
        games = []
        for r in rows:
            fgm,fga = parse_split(r["field_goal_percentage"])
            tpm,tpa = parse_split(r["three_point_percentage"])
            ftm,fta = parse_split(r["free_throw_percentage"])
            games.append({
                "game_id": r["game_id"],
                "date": clean_date(r["game_date"]),
                "location": clean_date(r["game_location"]),
                "home_or_away": r["home_or_away"] or "",
                "min": int(r["minutes"] or 0), "pts": int(r["points"] or 0),
                "reb": int(r["rebounds"] or 0), "ast": int(r["assists"] or 0),
                "stl": int(r["steals"] or 0),  "blk": int(r["blocks"] or 0),
                "tov": int(r["turnovers"] or 0),
                "pm":  parse_pm(r["plus_minus"]),
                "fgm":fgm,"fga":fga,"tpm":tpm,"tpa":tpa,"ftm":ftm,"fta":fta,
                "starter": bool(r["Starter"]),
                "month": month_label(r["game_date"]),
                "sort_key": parse_date_sort(r["game_date"]),
            })
        return jsonify({"name": rows[0]["name"], "team": rows[0]["team"], "games": games})
    finally:
        conn.close()

# ── Team dashboard ─────────────────────────────────────────────────────────────
@app.route("/api/team/dashboard")
def get_team_dashboard():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH scores AS (
                    SELECT g.game_id, g.game_date, g.game_location, g.game_attandence,
                        SUM(CASE WHEN b.team='Washington Wizards' THEN b.points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.points ELSE 0 END) AS opp_pts,
                        MAX(CASE WHEN b.team<>'Washington Wizards' THEN b.team END) AS opponent
                    FROM wizards_game_data g JOIN wizards_box_data b USING (game_id)
                    GROUP BY g.game_id, g.game_date, g.game_location, g.game_attandence
                )
                SELECT *, CASE WHEN wiz_pts > opp_pts THEN 'W' ELSE 'L' END AS result
                FROM scores
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(game_date)),'Month DD, YYYY') DESC
            """)
            games_raw = cur.fetchall()

            # Wizards box stats per game
            cur.execute("""
                SELECT game_id,
                    SUM(CAST(split_part(field_goal_percentage,'-',1) AS INT)) AS fgm,
                    SUM(CAST(split_part(field_goal_percentage,'-',2) AS INT)) AS fga,
                    SUM(CAST(split_part(three_point_percentage,'-',1) AS INT)) AS tpm,
                    SUM(CAST(split_part(three_point_percentage,'-',2) AS INT)) AS tpa,
                    SUM(rebounds) AS reb, SUM(assists) AS ast, SUM(turnovers) AS tov,
                    SUM(steals) AS stl, SUM(blocks) AS blk
                FROM wizards_box_data WHERE team='Washington Wizards'
                GROUP BY game_id
            """)
            game_stats_raw = {r["game_id"]: dict(r) for r in cur.fetchall()}

            # Opponent box stats per game
            cur.execute("""
                SELECT game_id,
                    SUM(CAST(split_part(field_goal_percentage,'-',1) AS INT)) AS fgm,
                    SUM(CAST(split_part(field_goal_percentage,'-',2) AS INT)) AS fga,
                    SUM(CAST(split_part(three_point_percentage,'-',1) AS INT)) AS tpm,
                    SUM(CAST(split_part(three_point_percentage,'-',2) AS INT)) AS tpa,
                    SUM(rebounds) AS reb, SUM(assists) AS ast, SUM(turnovers) AS tov,
                    SUM(steals) AS stl, SUM(blocks) AS blk
                FROM wizards_box_data WHERE team<>'Washington Wizards'
                GROUP BY game_id
            """)
            opp_stats_raw = {r["game_id"]: dict(r) for r in cur.fetchall()}

        game_list = []
        for g in games_raw:
            gid = g["game_id"]
            gs  = game_stats_raw.get(gid, {})
            os_ = opp_stats_raw.get(gid, {})
            def s(d,k): return int(d.get(k) or 0)
            game_list.append({
                "game_id":    gid,
                "date":       clean_date(g["game_date"]),
                "location":   clean_date(g["game_location"]),
                "opponent":   g["opponent"] or "",
                "wiz_pts":    int(g["wiz_pts"] or 0),
                "opp_pts":    int(g["opp_pts"] or 0),
                "result":     g["result"],
                # Wizards team stats
                "wiz_fg_pct": round(s(gs,'fgm')/s(gs,'fga')*100,1) if s(gs,'fga') else 0,
                "wiz_tp_pct": round(s(gs,'tpm')/s(gs,'tpa')*100,1) if s(gs,'tpa') else 0,
                "wiz_reb":    s(gs,'reb'), "wiz_ast": s(gs,'ast'),
                "wiz_tov":    s(gs,'tov'), "wiz_stl": s(gs,'stl'), "wiz_blk": s(gs,'blk'),
                # Opponent team stats
                "opp_fg_pct": round(s(os_,'fgm')/s(os_,'fga')*100,1) if s(os_,'fga') else 0,
                "opp_tp_pct": round(s(os_,'tpm')/s(os_,'tpa')*100,1) if s(os_,'tpa') else 0,
                "opp_reb":    s(os_,'reb'), "opp_ast": s(os_,'ast'),
                "opp_tov":    s(os_,'tov'), "opp_stl": s(os_,'stl'), "opp_blk": s(os_,'blk'),
                "month":      month_label(g["game_date"]),
                "sort_key":   parse_date_sort(g["game_date"]),
            })

        # Sort chronologically for streaks
        sorted_games = sorted(game_list, key=lambda g: g["sort_key"])

        wins = sum(1 for g in game_list if g["result"]=="W")
        losses = sum(1 for g in game_list if g["result"]=="L")

        # Streak calculation on chronologically sorted games
        def calc_streaks(games):
            best_w, best_l, cur_len, cur_type = 0, 0, 0, None
            for g in games:
                r = g["result"]
                if r == cur_type:
                    cur_len += 1
                else:
                    cur_len, cur_type = 1, r
                if r == 'W' and cur_len > best_w: best_w = cur_len
                if r == 'L' and cur_len > best_l: best_l = cur_len
            return best_w, best_l

        best_streak, worst_streak = calc_streaks(sorted_games)
        best_type = 'W'

        # Current streak
        curr_streak_len, curr_streak_type = 0, sorted_games[-1]["result"] if sorted_games else 'W'
        for g in reversed(sorted_games):
            if g["result"] == curr_streak_type:
                curr_streak_len += 1
            else:
                break

        best  = max(game_list, key=lambda g: g["wiz_pts"]) if game_list else {}
        worst = min(game_list, key=lambda g: g["wiz_pts"]) if game_list else {}
        bwin  = max(game_list, key=lambda g: g["wiz_pts"]-g["opp_pts"]) if game_list else {}
        bloss = min(game_list, key=lambda g: g["wiz_pts"]-g["opp_pts"]) if game_list else {}

        # Monthly averages (chronological)
        months_dict = {}
        for g in sorted_games:
            m = g["month"]
            if m not in months_dict:
                months_dict[m] = {"wiz":[],"opp":[],"w":0,"l":0}
            months_dict[m]["wiz"].append(g["wiz_pts"])
            months_dict[m]["opp"].append(g["opp_pts"])
            if g["result"]=="W": months_dict[m]["w"]+=1
            else: months_dict[m]["l"]+=1
        monthly = []
        for m, v in months_dict.items():
            monthly.append({
                "month": m,
                "avg_wiz": round(sum(v["wiz"])/len(v["wiz"]),1),
                "avg_opp": round(sum(v["opp"])/len(v["opp"]),1),
                "wins": v["w"], "losses": v["l"],
                "games": len(v["wiz"])
            })

        return jsonify({
            "wins": wins, "losses": losses,
            "avg_pts_for":  round(sum(g["wiz_pts"] for g in game_list)/len(game_list),1) if game_list else 0,
            "avg_pts_against": round(sum(g["opp_pts"] for g in game_list)/len(game_list),1) if game_list else 0,
            "fg_pct": round(sum(g["wiz_fg_pct"] for g in game_list)/len(game_list),1) if game_list else 0,
            "tp_pct": round(sum(g["wiz_tp_pct"] for g in game_list)/len(game_list),1) if game_list else 0,
            "best_streak": best_streak, "best_streak_type": best_type,
            "worst_streak": worst_streak,
            "curr_streak": curr_streak_len, "curr_streak_type": curr_streak_type,
            "best_game": best, "worst_game": worst,
            "biggest_win": bwin, "biggest_loss": bloss,
            "games": game_list,   # DESC (newest first)
            "games_asc": sorted_games,  # ASC for charts
            "monthly": monthly,
        })
    finally:
        conn.close()

# ── Matchup scout ──────────────────────────────────────────────────────────────
@app.route("/api/matchup/<path:opponent>")
def get_matchup(opponent):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH scores AS (
                    SELECT g.game_id, g.game_date, g.game_location,
                        SUM(CASE WHEN b.team='Washington Wizards' THEN b.points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.points ELSE 0 END) AS opp_pts
                    FROM wizards_game_data g JOIN wizards_box_data b USING (game_id)
                    WHERE b.team=%s OR b.team='Washington Wizards'
                    GROUP BY g.game_id, g.game_date, g.game_location
                    HAVING MAX(CASE WHEN b.team=%s THEN 1 ELSE 0 END)=1
                )
                SELECT *, CASE WHEN wiz_pts > opp_pts THEN 'W' ELSE 'L' END AS result
                FROM scores
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(game_date)),'Month DD, YYYY') DESC
            """, (opponent, opponent))
            matchup_games = cur.fetchall()

            cur.execute("""
                SELECT name, COUNT(*) AS games,
                    ROUND(AVG(points),1) AS avg_pts, ROUND(AVG(rebounds),1) AS avg_reb,
                    ROUND(AVG(assists),1) AS avg_ast, ROUND(AVG(steals),1) AS avg_stl,
                    ROUND(AVG(blocks),1) AS avg_blk, ROUND(AVG(turnovers),1) AS avg_tov,
                    ROUND(AVG(minutes),1) AS avg_min,
                    SUM(CAST(split_part(field_goal_percentage,'-',1) AS INT)) AS fgm,
                    SUM(CAST(split_part(field_goal_percentage,'-',2) AS INT)) AS fga,
                    SUM(CAST(split_part(three_point_percentage,'-',1) AS INT)) AS tpm,
                    SUM(CAST(split_part(three_point_percentage,'-',2) AS INT)) AS tpa,
                    ROUND(AVG(plus_minus::numeric),1) AS avg_pm
                FROM wizards_box_data WHERE team=%s
                GROUP BY name ORDER BY games DESC, avg_pts DESC
            """, (opponent,))
            roster = cur.fetchall()

            cur.execute("""
                SELECT name, COUNT(*) AS games,
                    ROUND(AVG(points),1) AS avg_pts, ROUND(AVG(rebounds),1) AS avg_reb,
                    ROUND(AVG(assists),1) AS avg_ast, ROUND(AVG(turnovers),1) AS avg_tov,
                    ROUND(AVG(steals),1) AS avg_stl, ROUND(AVG(blocks),1) AS avg_blk,
                    ROUND(AVG(minutes),1) AS avg_min,
                    ROUND(AVG(plus_minus::numeric),1) AS avg_pm
                FROM wizards_box_data
                WHERE team='Washington Wizards'
                AND game_id IN (SELECT DISTINCT game_id FROM wizards_box_data WHERE team=%s)
                GROUP BY name ORDER BY games DESC, avg_pts DESC
            """, (opponent,))
            wiz_vs = cur.fetchall()

        games_out = []
        for g in matchup_games:
            games_out.append({
                "game_id": g["game_id"],
                "date":    clean_date(g["game_date"]),
                "location":clean_date(g["game_location"]),
                "wiz_pts": int(g["wiz_pts"] or 0),
                "opp_pts": int(g["opp_pts"] or 0),
                "result":  g["result"],
            })

        roster_out = []
        for r in roster:
            d = dict(r)
            d["fg_pct"] = round(d["fgm"]/d["fga"]*100,1) if d["fga"] else 0
            d["tp_pct"] = round(d["tpm"]/d["tpa"]*100,1) if d["tpa"] else 0
            roster_out.append(d)

        wins   = sum(1 for g in games_out if g["result"]=="W")
        losses = sum(1 for g in games_out if g["result"]=="L")
        opp_wins   = losses
        opp_losses = wins

        # Best performer: highest avg_pts
        best_opp = max(roster_out, key=lambda r: float(r["avg_pts"] or 0)) if roster_out else None
        wiz_vs_list = [dict(r) for r in wiz_vs]
        best_wiz = max(wiz_vs_list, key=lambda r: float(r["avg_pts"] or 0)) if wiz_vs_list else None

        return jsonify({
            "opponent": opponent,
            "wins": wins, "losses": losses,
            "opp_wins": opp_wins, "opp_losses": opp_losses,
            "games": games_out,
            "roster": roster_out,
            "wizards_vs": wiz_vs_list,
            "best_opp_performer": best_opp,
            "best_wiz_performer": best_wiz,
        })
    finally:
        conn.close()

# ── Lineups ────────────────────────────────────────────────────────────────────
@app.route("/api/lineups")
def get_lineups():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH scores AS (
                    SELECT game_id,
                        SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                    FROM wizards_box_data GROUP BY game_id
                ),
                starters AS (
                    SELECT game_id, array_agg(name ORDER BY name) AS lineup
                    FROM wizards_box_data
                    WHERE team='Washington Wizards' AND "Starter"=true GROUP BY game_id
                )
                SELECT s.game_id, s.lineup, sc.wiz_pts, sc.opp_pts,
                    CASE WHEN sc.wiz_pts > sc.opp_pts THEN 'W' ELSE 'L' END AS result,
                    g.game_date
                FROM starters s JOIN scores sc USING (game_id) JOIN wizards_game_data g USING (game_id)
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)),'Month DD, YYYY') DESC
            """)
            rows = cur.fetchall()

        pair_stats = defaultdict(lambda: {"games":0,"wins":0,"pts_for":[],"pts_against":[]})
        player_games = defaultdict(lambda: {"games":0,"wins":0})
        for r in rows:
            lineup = r["lineup"] or []
            wp, op = int(r["wiz_pts"] or 0), int(r["opp_pts"] or 0)
            for p in lineup:
                player_games[p]["games"] += 1
                if r["result"]=="W": player_games[p]["wins"] += 1
            for i in range(len(lineup)):
                for j in range(i+1, len(lineup)):
                    key = (lineup[i], lineup[j])
                    pair_stats[key]["games"] += 1
                    pair_stats[key]["pts_for"].append(wp)
                    pair_stats[key]["pts_against"].append(op)
                    if r["result"]=="W": pair_stats[key]["wins"] += 1

        pairs_out = []
        for (p1,p2), s in pair_stats.items():
            if s["games"] < 2: continue
            af = round(sum(s["pts_for"])/len(s["pts_for"]),1)
            aa = round(sum(s["pts_against"])/len(s["pts_against"]),1)
            pairs_out.append({"p1":p1,"p2":p2,"games":s["games"],"wins":s["wins"],
                "losses":s["games"]-s["wins"],"win_pct":round(s["wins"]/s["games"]*100,1),
                "avg_pts_for":af,"avg_pts_against":aa,"avg_diff":round(af-aa,1)})
        pairs_out.sort(key=lambda x: (-x["games"],-x["win_pct"]))

        game_lineups = [{"game_id":r["game_id"],"date":clean_date(r["game_date"]),
            "lineup":r["lineup"] or [],"wiz_pts":int(r["wiz_pts"] or 0),
            "opp_pts":int(r["opp_pts"] or 0),"result":r["result"]} for r in rows]

        player_list = [{"name":k,"games":v["games"],"wins":v["wins"],
            "win_pct":round(v["wins"]/v["games"]*100,1)} for k,v in player_games.items()]
        player_list.sort(key=lambda x: -x["games"])

        return jsonify({"pairs":pairs_out[:60],"players":player_list,"games":game_lineups})
    finally:
        conn.close()

# ── Fatigue ────────────────────────────────────────────────────────────────────
@app.route("/api/fatigue")
def get_fatigue():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get all Wizards player stats with game dates, sorted chronologically
            cur.execute("""
                SELECT b.game_id, b.name, b.points, b.rebounds, b.assists,
                    b.steals, b.blocks, b.turnovers, b.minutes,
                    b.field_goal_percentage, b.three_point_percentage,
                    b.plus_minus, g.game_date,
                    MAX(CASE WHEN b2.team<>'Washington Wizards' THEN b2.team END) AS opponent
                FROM wizards_box_data b
                JOIN wizards_game_data g USING (game_id)
                JOIN wizards_box_data b2 ON b.game_id = b2.game_id
                WHERE b.team = 'Washington Wizards'
                GROUP BY b.game_id, b.name, b.points, b.rebounds, b.assists,
                    b.steals, b.blocks, b.turnovers, b.minutes,
                    b.field_goal_percentage, b.three_point_percentage,
                    b.plus_minus, g.game_date
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)),'Month DD, YYYY') ASC
            """)
            rows = cur.fetchall()

        # Build per-game team data with rest days
        from itertools import groupby
        game_dates = {}
        for r in rows:
            gid = r["game_id"]
            if gid not in game_dates:
                game_dates[gid] = {"date": clean_date(r["game_date"]), "sort_key": parse_date_sort(r["game_date"]), "opponent": r["opponent"] or ""}

        sorted_game_ids = sorted(game_dates.keys(), key=lambda gid: game_dates[gid]["sort_key"])

        # Assign rest days
        import datetime
        _MNUM = {'January':1,'February':2,'March':3,'April':4,'May':5,'June':6,
                 'July':7,'August':8,'September':9,'October':10,'November':11,'December':12}
        def _parse_date(s):
            try:
                p = s.replace(',','').split()
                return datetime.date(int(p[2]), _MNUM[p[0]], int(p[1]))
            except:
                return None

        rest_map = {}
        for i, gid in enumerate(sorted_game_ids):
            if i == 0:
                rest_map[gid] = None
            else:
                try:
                    d1 = _parse_date(game_dates[sorted_game_ids[i-1]]["date"])
                    d2 = _parse_date(game_dates[gid]["date"])
                    rest_map[gid] = (d2 - d1).days if d1 and d2 else None
                except:
                    rest_map[gid] = None

        # Aggregate Wizards team stats by rest category
        rest_buckets = {"b2b":[],"1d":[],"2d":[],"3d+":[],"all":[]}
        game_results = {}

        # Get team scores for win/loss
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT game_id,
                    SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                    SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                FROM wizards_box_data GROUP BY game_id
            """)
            for r in cur.fetchall():
                game_results[r["game_id"]] = {
                    "wiz_pts": int(r["wiz_pts"] or 0),
                    "opp_pts": int(r["opp_pts"] or 0),
                    "result": "W" if (r["wiz_pts"] or 0) > (r["opp_pts"] or 0) else "L"
                }

        games_out = []
        for gid in sorted_game_ids:
            rest = rest_map[gid]
            gr = game_results.get(gid, {})
            # Get all Wizards player stats for this game
            game_rows = [r for r in rows if r["game_id"] == gid]
            pts_list = [int(r["points"] or 0) for r in game_rows]
            reb_list = [int(r["rebounds"] or 0) for r in game_rows]
            ast_list = [int(r["assists"] or 0) for r in game_rows]
            stl_list = [int(r["steals"] or 0) for r in game_rows]
            blk_list = [int(r["blocks"] or 0) for r in game_rows]
            tov_list = [int(r["turnovers"] or 0) for r in game_rows]

            entry = {
                "game_id":  gid,
                "date":     game_dates[gid]["date"],
                "opponent": game_dates[gid]["opponent"],
                "rest":     rest,
                "is_b2b":   rest == 1,
                "wiz_pts":  gr.get("wiz_pts",0),
                "opp_pts":  gr.get("opp_pts",0),
                "result":   gr.get("result","L"),
                "avg_pts":  round(sum(pts_list)/len(pts_list),1) if pts_list else 0,
                "avg_reb":  round(sum(reb_list)/len(reb_list),1) if reb_list else 0,
                "avg_ast":  round(sum(ast_list)/len(ast_list),1) if ast_list else 0,
                "avg_stl":  round(sum(stl_list)/len(stl_list),1) if stl_list else 0,
                "avg_blk":  round(sum(blk_list)/len(blk_list),1) if blk_list else 0,
                "avg_tov":  round(sum(tov_list)/len(tov_list),1) if tov_list else 0,
            }
            games_out.append(entry)
            rest_buckets["all"].append(entry)
            if rest == 1:   rest_buckets["b2b"].append(entry)
            elif rest == 2: rest_buckets["1d"].append(entry)
            elif rest == 3: rest_buckets["2d"].append(entry)
            elif rest and rest >= 4: rest_buckets["3d+"].append(entry)

        def agg_bucket(lst):
            if not lst: return {"games":0,"wins":0,"losses":0,"win_pct":0,"avg_pts":0,"avg_allowed":0,"avg_reb":0,"avg_ast":0,"avg_stl":0,"avg_blk":0,"avg_tov":0}
            wins = sum(1 for g in lst if g["result"]=="W")
            return {
                "games":    len(lst),
                "wins":     wins,
                "losses":   len(lst)-wins,
                "win_pct":  round(wins/len(lst)*100,1),
                "avg_pts":  round(sum(g["wiz_pts"] for g in lst)/len(lst),1),
                "avg_allowed": round(sum(g["opp_pts"] for g in lst)/len(lst),1),
                "avg_reb":  round(sum(g["avg_reb"] for g in lst)/len(lst),1),
                "avg_ast":  round(sum(g["avg_ast"] for g in lst)/len(lst),1),
                "avg_stl":  round(sum(g["avg_stl"] for g in lst)/len(lst),1),
                "avg_blk":  round(sum(g["avg_blk"] for g in lst)/len(lst),1),
                "avg_tov":  round(sum(g["avg_tov"] for g in lst)/len(lst),1),
            }

        return jsonify({
            "games": list(reversed(games_out)),  # newest first
            "b2b":   agg_bucket(rest_buckets["b2b"]),
            "one_day":  agg_bucket(rest_buckets["1d"]),
            "two_day":  agg_bucket(rest_buckets["2d"]),
            "three_plus": agg_bucket(rest_buckets["3d+"]),
            "all":   agg_bucket(rest_buckets["all"]),
        })
    finally:
        conn.close()

# ── Fatigue — individual player ───────────────────────────────────────────────
@app.route("/api/fatigue/player/<path:player_name>")
def get_fatigue_player(player_name):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT b.game_id, b.name, b.points, b.rebounds, b.assists,
                    b.steals, b.blocks, b.turnovers, b.minutes, b.plus_minus,
                    g.game_date,
                    MAX(CASE WHEN b2.team<>'Washington Wizards' THEN b2.team END) AS opponent,
                    SUM(CASE WHEN b2.team='Washington Wizards' THEN b2.points ELSE 0 END) AS wiz_pts,
                    SUM(CASE WHEN b2.team<>'Washington Wizards' THEN b2.points ELSE 0 END) AS opp_pts
                FROM wizards_box_data b
                JOIN wizards_game_data g USING (game_id)
                JOIN wizards_box_data b2 ON b.game_id = b2.game_id
                WHERE b.name = %s AND b.team = 'Washington Wizards'
                GROUP BY b.game_id, b.name, b.points, b.rebounds, b.assists,
                    b.steals, b.blocks, b.turnovers, b.minutes, b.plus_minus, g.game_date
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)),'Month DD, YYYY') ASC
            """, (player_name,))
            rows = cur.fetchall()
        if not rows:
            return jsonify({"error": "Not found"}), 404

        import datetime
        _MNUM2 = {'January':1,'February':2,'March':3,'April':4,'May':5,'June':6,
                  'July':7,'August':8,'September':9,'October':10,'November':11,'December':12}
        def _pd2(s):
            try:
                p=s.replace(',','').split(); return datetime.date(int(p[2]),_MNUM2[p[0]],int(p[1]))
            except: return None

        games_out, prev_date = [], None
        for r in rows:
            date_str = clean_date(r["game_date"])
            curr_date = _pd2(date_str)
            rest = (curr_date - prev_date).days if curr_date and prev_date else None
            prev_date = curr_date
            wiz_pts = int(r["wiz_pts"] or 0)
            opp_pts = int(r["opp_pts"] or 0)
            games_out.append({
                "game_id": r["game_id"], "date": date_str,
                "opponent": r["opponent"] or "", "rest": rest,
                "pts": int(r["points"] or 0), "reb": int(r["rebounds"] or 0),
                "ast": int(r["assists"] or 0), "stl": int(r["steals"] or 0),
                "blk": int(r["blocks"] or 0), "tov": int(r["turnovers"] or 0),
                "min": int(r["minutes"] or 0), "pm": parse_pm(r["plus_minus"]),
                "wiz_pts": wiz_pts, "opp_pts": opp_pts,
                "result": "W" if wiz_pts > opp_pts else "L",
            })

        buckets = {"b2b":[], "1d":[], "2d":[], "3d+":[]}
        for g in games_out:
            r = g["rest"]
            if r == 1:       buckets["b2b"].append(g)
            elif r == 2:     buckets["1d"].append(g)
            elif r == 3:     buckets["2d"].append(g)
            elif r and r>=4: buckets["3d+"].append(g)

        def agg(lst):
            if not lst: return {"games":0,"wins":0,"losses":0,"win_pct":0,"avg_pts":0,"avg_reb":0,"avg_ast":0,"avg_stl":0,"avg_blk":0,"avg_tov":0}
            wins = sum(1 for g in lst if g["result"]=="W")
            def avg(k): return round(sum(g[k] for g in lst)/len(lst),1)
            return {"games":len(lst),"wins":wins,"losses":len(lst)-wins,
                    "win_pct":round(wins/len(lst)*100,1),
                    "avg_pts":avg("pts"),"avg_reb":avg("reb"),"avg_ast":avg("ast"),
                    "avg_stl":avg("stl"),"avg_blk":avg("blk"),"avg_tov":avg("tov")}

        return jsonify({
            "games": list(reversed(games_out)),
            "b2b": agg(buckets["b2b"]), "one_day": agg(buckets["1d"]),
            "two_day": agg(buckets["2d"]), "three_plus": agg(buckets["3d+"]),
        })
    finally:
        conn.close()


# ── Refs ───────────────────────────────────────────────────────────────────────
@app.route("/api/refs")
def get_refs():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH game_scores AS (
                    SELECT game_id,
                        SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                    FROM wizards_box_data GROUP BY game_id
                ),
                game_results AS (
                    SELECT g.game_id, g.game_date, g.game_refs,
                        CASE WHEN s.wiz_pts > s.opp_pts THEN 'W' ELSE 'L' END AS result,
                        s.wiz_pts, s.opp_pts
                    FROM wizards_game_data g JOIN game_scores s USING (game_id)
                ),
                refs_expanded AS (
                    SELECT result, game_date, wiz_pts, opp_pts,
                        trim(both '"' FROM ref_raw) AS ref_name
                    FROM game_results,
                         LATERAL unnest(string_to_array(trim('{}' FROM game_refs),',')) AS ref_raw
                )
                SELECT ref_name,
                    COUNT(*) AS games_reffed,
                    SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS losses,
                    ROUND(AVG(wiz_pts),1) AS avg_wiz_pts,
                    ROUND(AVG(opp_pts),1) AS avg_opp_pts,
                    MAX(game_date) AS last_game
                FROM refs_expanded
                GROUP BY ref_name ORDER BY games_reffed DESC, ref_name ASC
            """)
            rows = cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["win_pct"] = round(d["wins"]/d["games_reffed"]*100,1) if d["games_reffed"] else 0
            d["last_game"] = clean_date(d["last_game"])
            result.append(d)
        return jsonify(result)
    finally:
        conn.close()

@app.route("/api/ref/<path:ref_name>")
def get_ref(ref_name):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH game_scores AS (
                    SELECT game_id,
                        SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                    FROM wizards_box_data GROUP BY game_id
                ),
                opponent AS (
                    SELECT DISTINCT ON (game_id) game_id, team AS opponent
                    FROM wizards_box_data WHERE team<>'Washington Wizards'
                )
                SELECT g.game_id, g.game_date, g.game_location, g.game_attandence, g.game_refs,
                    o.opponent, s.wiz_pts, s.opp_pts,
                    CASE WHEN s.wiz_pts > s.opp_pts THEN 'W' ELSE 'L' END AS result
                FROM wizards_game_data g
                JOIN game_scores s USING (game_id) JOIN opponent o USING (game_id)
                WHERE g.game_refs LIKE '%%' || %s || '%%'
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)),'Month DD, YYYY') DESC
            """, (ref_name,))
            rows = cur.fetchall()
        if not rows:
            return jsonify({"error": "Not found"}), 404
        games = []
        for r in rows:
            raw = r["game_refs"] or ""
            crew_all = [x.strip().strip('"') for x in raw.strip("{}").split(",")]
            games.append({
                "game_id":   r["game_id"],
                "date":      clean_date(r["game_date"]),
                "location":  clean_date(r["game_location"]),
                "opponent":  r["opponent"],
                "wiz_pts":   int(r["wiz_pts"] or 0),
                "opp_pts":   int(r["opp_pts"] or 0),
                "result":    r["result"],
                "crew":      [c for c in crew_all if c and c != ref_name],
            })
        wins = sum(1 for g in games if g["result"]=="W")
        losses = sum(1 for g in games if g["result"]=="L")
        return jsonify({"name": ref_name, "wins": wins, "losses": losses, "games": games})
    finally:
        conn.close()


# ── Refs overview dashboard ────────────────────────────────────────────────────
@app.route("/api/refs/overview")
def get_refs_overview():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH game_scores AS (
                    SELECT game_id,
                        SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                    FROM wizards_box_data GROUP BY game_id
                ),
                game_results AS (
                    SELECT g.game_id, g.game_date, g.game_refs,
                        s.wiz_pts, s.opp_pts,
                        CASE WHEN s.wiz_pts > s.opp_pts THEN 'W' ELSE 'L' END AS result
                    FROM wizards_game_data g JOIN game_scores s USING (game_id)
                ),
                refs_expanded AS (
                    SELECT result, game_date, wiz_pts, opp_pts,
                        trim(both '"' FROM ref_raw) AS ref_name
                    FROM game_results,
                         LATERAL unnest(string_to_array(trim('{}' FROM game_refs),',')) AS ref_raw
                )
                SELECT ref_name,
                    COUNT(*) AS games_reffed,
                    SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) AS losses,
                    ROUND(AVG(wiz_pts),1) AS avg_wiz_pts,
                    ROUND(AVG(opp_pts),1) AS avg_opp_pts,
                    ROUND(AVG(wiz_pts - opp_pts),1) AS avg_diff
                FROM refs_expanded
                WHERE ref_name <> ''
                GROUP BY ref_name
                ORDER BY games_reffed DESC, ref_name ASC
            """)
            refs = cur.fetchall()

        result = []
        for r in refs:
            d = dict(r)
            d["win_pct"] = round(d["wins"]/d["games_reffed"]*100,1) if d["games_reffed"] else 0
            result.append(d)

        # Crew analysis — find 3-ref crews
        crew_stats = {}
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                WITH game_scores AS (
                    SELECT game_id,
                        SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                    FROM wizards_box_data GROUP BY game_id
                )
                SELECT g.game_refs,
                    CASE WHEN s.wiz_pts > s.opp_pts THEN 'W' ELSE 'L' END AS result,
                    s.wiz_pts, s.opp_pts
                FROM wizards_game_data g JOIN game_scores s USING (game_id)
            """)
            for r in cur.fetchall():
                raw = r["game_refs"] or ""
                refs_list = sorted([x.strip().strip('"') for x in raw.strip("{}").split(",") if x.strip().strip('"')])
                key = " | ".join(refs_list)
                if key not in crew_stats:
                    crew_stats[key] = {"crew": refs_list, "games":0,"wins":0,"wiz_pts":[],"opp_pts":[]}
                crew_stats[key]["games"] += 1
                crew_stats[key]["wiz_pts"].append(int(r["wiz_pts"] or 0))
                crew_stats[key]["opp_pts"].append(int(r["opp_pts"] or 0))
                if r["result"] == "W": crew_stats[key]["wins"] += 1

        crews_out = []
        for k, c in crew_stats.items():
            if c["games"] < 2: continue
            wp = round(c["wins"]/c["games"]*100,1)
            crews_out.append({
                "crew": c["crew"],
                "games": c["games"],
                "wins": c["wins"],
                "losses": c["games"]-c["wins"],
                "win_pct": wp,
                "avg_pts": round(sum(c["wiz_pts"])/len(c["wiz_pts"]),1),
                "avg_allowed": round(sum(c["opp_pts"])/len(c["opp_pts"]),1),
                "avg_diff": round((sum(c["wiz_pts"])-sum(c["opp_pts"]))/c["games"],1),
            })
        crews_out.sort(key=lambda x: -x["win_pct"])

        return jsonify({
            "refs": result,
            "crews": crews_out,
            "best_refs":  sorted(result, key=lambda r: (-r["win_pct"], -r["games_reffed"]))[:5],
            "worst_refs": sorted(result, key=lambda r: (r["win_pct"], -r["games_reffed"]))[:5],
        })
    finally:
        conn.close()


# ── Team opponent stats per game ───────────────────────────────────────────────
@app.route("/api/team/opponent-stats")
def get_opponent_stats():
    """Per-game opponent team totals for the season."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    g.game_id,
                    g.game_date,
                    MAX(CASE WHEN b.team <> 'Washington Wizards' THEN b.team END) AS opponent,
                    SUM(CASE WHEN b.team='Washington Wizards' THEN b.points ELSE 0 END) AS wiz_pts,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.points ELSE 0 END) AS opp_pts,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.rebounds ELSE 0 END) AS opp_reb,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.assists ELSE 0 END) AS opp_ast,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.turnovers ELSE 0 END) AS opp_tov,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.steals ELSE 0 END) AS opp_stl,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.blocks ELSE 0 END) AS opp_blk,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN CAST(split_part(b.three_point_percentage,'-',1) AS INT) ELSE 0 END) AS opp_3pm,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN CAST(split_part(b.three_point_percentage,'-',2) AS INT) ELSE 0 END) AS opp_3pa,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN CAST(split_part(b.field_goal_percentage,'-',1) AS INT) ELSE 0 END) AS opp_fgm,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN CAST(split_part(b.field_goal_percentage,'-',2) AS INT) ELSE 0 END) AS opp_fga,
                    CASE WHEN SUM(CASE WHEN b.team='Washington Wizards' THEN b.points ELSE 0 END)
                         > SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.points ELSE 0 END) THEN 'W' ELSE 'L' END AS result
                FROM wizards_game_data g
                JOIN wizards_box_data b USING (game_id)
                GROUP BY g.game_id, g.game_date
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)),'Month DD, YYYY') ASC
            """)
            rows = cur.fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["date"] = clean_date(d["game_date"])
            d["fg_pct"] = round(d["opp_fgm"]/d["opp_fga"]*100,1) if d["opp_fga"] else 0
            d["tp_pct"] = round(d["opp_3pm"]/d["opp_3pa"]*100,1) if d["opp_3pa"] else 0
            out.append(d)
        return jsonify(out)
    finally:
        conn.close()

# ── Per-game box score ─────────────────────────────────────────────────────────
@app.route("/api/game/<path:game_id>")
def get_game(game_id):
    """Full box score for both teams in a single game, plus game metadata."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Game metadata
            cur.execute("""
                SELECT game_id, game_date, game_location, game_attandence, game_refs
                FROM wizards_game_data WHERE game_id = %s
            """, (game_id,))
            meta = cur.fetchone()
            if not meta:
                return jsonify({"error": "Game not found"}), 404

            # All player rows for this game (both teams)
            cur.execute("""
                SELECT name, team, minutes, points,
                    field_goal_percentage, three_point_percentage, free_throw_percentage,
                    rebounds, assists, steals, blocks, turnovers, fouls,
                    o_rebounds, d_rebounds, plus_minus, "Starter"
                FROM wizards_box_data
                WHERE game_id = %s
                ORDER BY team, "Starter" DESC, points DESC
            """, (game_id,))
            rows = cur.fetchall()

        wiz_players, opp_players = [], []
        wiz_team, opp_team = "Washington Wizards", ""

        for r in rows:
            fgm, fga = parse_split(r["field_goal_percentage"])
            tpm, tpa = parse_split(r["three_point_percentage"])
            ftm, fta = parse_split(r["free_throw_percentage"])
            player = {
                "name":    r["name"],
                "team":    r["team"],
                "min":     int(r["minutes"] or 0),
                "pts":     int(r["points"] or 0),
                "reb":     int(r["rebounds"] or 0),
                "ast":     int(r["assists"] or 0),
                "stl":     int(r["steals"] or 0),
                "blk":     int(r["blocks"] or 0),
                "tov":     int(r["turnovers"] or 0),
                "pf":      int(r["fouls"] or 0),
                "oreb":    int(r["o_rebounds"] or 0),
                "dreb":    int(r["d_rebounds"] or 0),
                "pm":      parse_pm(r["plus_minus"]),
                "fgm": fgm, "fga": fga,
                "tpm": tpm, "tpa": tpa,
                "ftm": ftm, "fta": fta,
                "starter": bool(r["Starter"]),
            }
            if r["team"] == "Washington Wizards":
                wiz_players.append(player)
            else:
                opp_team = r["team"]
                opp_players.append(player)

        def team_totals(players):
            if not players:
                return {}
            fgm = sum(p["fgm"] for p in players)
            fga = sum(p["fga"] for p in players)
            tpm = sum(p["tpm"] for p in players)
            tpa = sum(p["tpa"] for p in players)
            ftm = sum(p["ftm"] for p in players)
            fta = sum(p["fta"] for p in players)
            return {
                "pts":     sum(p["pts"] for p in players),
                "reb":     sum(p["reb"] for p in players),
                "ast":     sum(p["ast"] for p in players),
                "tov":     sum(p["tov"] for p in players),
                "stl":     sum(p["stl"] for p in players),
                "blk":     sum(p["blk"] for p in players),
                "oreb":    sum(p["oreb"] for p in players),
                "fgm": fgm, "fga": fga,
                "tpm": tpm, "tpa": tpa,
                "ftm": ftm, "fta": fta,
                "fg_pct":  round(fgm / fga * 100, 1) if fga else 0,
                "tp_pct":  round(tpm / tpa * 100, 1) if tpa else 0,
                "ft_pct":  round(ftm / fta * 100, 1) if fta else 0,
            }

        wiz_totals = team_totals(wiz_players)
        opp_totals = team_totals(opp_players)

        # Parse refs from postgres array string e.g. '{"Brian Forte","Curtis Blair"}'
        raw_refs = meta["game_refs"] or ""
        refs = [x.strip().strip('"') for x in raw_refs.strip("{}").split(",") if x.strip().strip('"')]

        return jsonify({
            "game_id":    game_id,
            "date":       clean_date(meta["game_date"]),
            "location":   clean_date(meta["game_location"]),
            "attendance": meta["game_attandence"] or "",
            "refs":       refs,
            "wiz_team":   wiz_team,
            "opp_team":   opp_team,
            "wiz_totals": wiz_totals,
            "opp_totals": opp_totals,
            "wiz_players": wiz_players,
            "opp_players": opp_players,
        })
    finally:
        conn.close()


# ── Player prop model ──────────────────────────────────────────────────────────
@app.route("/api/props")
def get_props():
    """
    Player prop projections for all Wizards players.
    Uses last-N game rolling averages, home/away splits, rest-day adjustments,
    and opponent defensive rating to project pts / reb / ast lines.
    Returns projections + suggested over/under with confidence level.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Full Wizards player game log (chronological)
            cur.execute("""
                SELECT b.game_id, b.name, b.points, b.rebounds, b.assists,
                    b.steals, b.blocks, b.turnovers, b.minutes,
                    b.field_goal_percentage, b.three_point_percentage,
                    b.free_throw_percentage, b.plus_minus, b."Starter",
                    b.home_or_away, g.game_date,
                    MAX(CASE WHEN b2.team<>'Washington Wizards' THEN b2.team END) AS opponent,
                    SUM(CASE WHEN b2.team='Washington Wizards' THEN b2.points ELSE 0 END) AS wiz_pts,
                    SUM(CASE WHEN b2.team<>'Washington Wizards' THEN b2.points ELSE 0 END) AS opp_pts
                FROM wizards_box_data b
                JOIN wizards_game_data g USING (game_id)
                JOIN wizards_box_data b2 ON b.game_id=b2.game_id
                WHERE b.team='Washington Wizards'
                GROUP BY b.game_id, b.name, b.points, b.rebounds, b.assists,
                    b.steals, b.blocks, b.turnovers, b.minutes,
                    b.field_goal_percentage, b.three_point_percentage,
                    b.free_throw_percentage, b.plus_minus, b."Starter",
                    b.home_or_away, g.game_date
                ORDER BY b.name, TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)),'Month DD, YYYY') ASC
            """, )
            rows = cur.fetchall()

            # Opponent defensive stats (points allowed per game)
            cur.execute("""
                SELECT MAX(CASE WHEN team<>'Washington Wizards' THEN team END) AS opp,
                    ROUND(AVG(CASE WHEN team='Washington Wizards' THEN points ELSE NULL END),1) AS avg_pts_allowed,
                    COUNT(DISTINCT game_id) AS games
                FROM wizards_box_data
                GROUP BY game_id
            """)
            opp_rows = cur.fetchall()

        # Build opponent defensive rating (avg pts WAS scored vs them)
        opp_defense = defaultdict(lambda: {"sum":0,"cnt":0})
        for r in opp_rows:
            if r["opp"]:
                opp_defense[r["opp"]]["sum"] += float(r["avg_pts_allowed"] or 0)
                opp_defense[r["opp"]]["cnt"] += 1
        opp_def_avg = {k: round(v["sum"]/v["cnt"],1) for k,v in opp_defense.items() if v["cnt"]}

        # Group rows by player
        from collections import defaultdict as dd
        player_games = defaultdict(list)
        for r in rows:
            player_games[r["name"]].append(r)

        # League average WAS pts scored (for baseline)
        all_pts = [int(r["points"] or 0) for r in rows]
        league_avg_pts = sum(all_pts)/len(all_pts) if all_pts else 0

        results = []
        for name, games in player_games.items():
            if len(games) < 5:
                continue

            def avg(key, last_n=None):
                src = games[-last_n:] if last_n else games
                vals = [float(r[key] or 0) for r in src]
                return round(sum(vals)/len(vals), 2) if vals else 0

            def pct_avg(key, last_n=None):
                src = games[-last_n:] if last_n else games
                vals = []
                for r in src:
                    m, a = parse_split(r[key])
                    if a > 0: vals.append(m/a*100)
                return round(sum(vals)/len(vals), 1) if vals else 0

            # Season and rolling averages
            season_pts  = avg("points")
            season_reb  = avg("rebounds")
            season_ast  = avg("assists")
            season_stl  = avg("steals")
            season_blk  = avg("blocks")
            season_min  = avg("minutes")
            l5_pts  = avg("points", 5)
            l10_pts = avg("points", 10)
            l5_reb  = avg("rebounds", 5)
            l10_reb = avg("rebounds", 10)
            l5_ast  = avg("assists", 5)
            l10_ast = avg("assists", 10)

            # Home/away splits
            home_games = [g for g in games if (g["home_or_away"] or "").upper() in ("HOME","H")]
            away_games = [g for g in games if (g["home_or_away"] or "").upper() in ("AWAY","A")]
            home_pts = round(sum(float(g["points"] or 0) for g in home_games)/len(home_games),1) if home_games else season_pts
            away_pts = round(sum(float(g["points"] or 0) for g in away_games)/len(away_games),1) if away_games else season_pts
            home_reb = round(sum(float(g["rebounds"] or 0) for g in home_games)/len(home_games),1) if home_games else season_reb
            away_reb = round(sum(float(g["rebounds"] or 0) for g in away_games)/len(away_games),1) if away_games else season_reb
            home_ast = round(sum(float(g["assists"] or 0) for g in home_games)/len(home_games),1) if home_games else season_ast
            away_ast = round(sum(float(g["assists"] or 0) for g in away_games)/len(away_games),1) if away_games else season_ast

            # Trend: last 5 vs season avg (positive = heating up)
            pts_trend   = round(l5_pts - season_pts, 1)
            reb_trend   = round(l5_reb - season_reb, 1)
            ast_trend   = round(l5_ast - season_ast, 1)

            # Hit rate analysis: % of games over typical prop lines
            def hit_rate(key, line, last_n=None):
                src = games[-last_n:] if last_n else games
                hits = sum(1 for g in src if float(g[key] or 0) >= line)
                return round(hits/len(src)*100, 1) if src else 0

            # Model projection: weighted combo of season avg + L5 + L10
            proj_pts = round(season_pts*0.35 + l10_pts*0.35 + l5_pts*0.30, 1)
            proj_reb = round(season_reb*0.35 + l10_reb*0.35 + l5_reb*0.30, 1)
            proj_ast = round(season_ast*0.35 + l10_ast*0.35 + l5_ast*0.30, 1)

            # Confidence: lower variance = higher confidence
            import statistics
            pts_vals = [float(g["points"] or 0) for g in games[-15:]]
            reb_vals = [float(g["rebounds"] or 0) for g in games[-15:]]
            ast_vals = [float(g["assists"] or 0) for g in games[-15:]]
            pts_std  = round(statistics.stdev(pts_vals), 1) if len(pts_vals) > 1 else 5.0
            reb_std  = round(statistics.stdev(reb_vals), 1) if len(reb_vals) > 1 else 3.0
            ast_std  = round(statistics.stdev(ast_vals), 1) if len(ast_vals) > 1 else 2.0

            def confidence(std, avg):
                cv = std / avg if avg > 0 else 1
                if cv < 0.3: return "HIGH"
                if cv < 0.5: return "MED"
                return "LOW"

            # Suggest common prop line (round to nearest .5)
            def prop_line(val):
                return round(val * 2) / 2

            pts_line = prop_line(proj_pts)
            reb_line = prop_line(proj_reb)
            ast_line = prop_line(proj_ast)

            # Over/under recommendation vs that line
            pts_rec = "OVER" if proj_pts > pts_line else "UNDER"
            reb_rec = "OVER" if proj_reb > reb_line else "UNDER"
            ast_rec = "OVER" if proj_ast > ast_line else "UNDER"

            # FG%, 3P%, FT% splits
            fg_pct  = pct_avg("field_goal_percentage")
            tp_pct  = pct_avg("three_point_percentage")
            ft_pct  = pct_avg("free_throw_percentage")
            l5_fg   = pct_avg("field_goal_percentage", 5)
            l5_tp   = pct_avg("three_point_percentage", 5)

            # Double-double / triple-double rate
            dd_rate = round(sum(1 for g in games if float(g["points"] or 0) >= 10 and float(g["rebounds"] or 0) >= 10)/len(games)*100, 1)
            td_rate = round(sum(1 for g in games if sum(1 for k in ("points","rebounds","assists") if float(g[k] or 0) >= 10) >= 3)/len(games)*100, 1)

            results.append({
                "name":        name,
                "games":       len(games),
                "starter_pct": round(sum(1 for g in games if g["Starter"])/len(games)*100, 1),
                # Season avgs
                "season_pts":  season_pts, "season_reb": season_reb,
                "season_ast":  season_ast, "season_stl": season_stl,
                "season_blk":  season_blk, "season_min":  season_min,
                # Rolling
                "l5_pts": l5_pts, "l10_pts": l10_pts,
                "l5_reb": l5_reb, "l10_reb": l10_reb,
                "l5_ast": l5_ast, "l10_ast": l10_ast,
                # Trends
                "pts_trend": pts_trend, "reb_trend": reb_trend, "ast_trend": ast_trend,
                # Home/away
                "home_pts": home_pts, "away_pts": away_pts,
                "home_reb": home_reb, "away_reb": away_reb,
                "home_ast": home_ast, "away_ast": away_ast,
                # Shooting
                "fg_pct": fg_pct, "tp_pct": tp_pct, "ft_pct": ft_pct,
                "l5_fg": l5_fg, "l5_tp": l5_tp,
                # Projections
                "proj_pts": proj_pts, "proj_reb": proj_reb, "proj_ast": proj_ast,
                # Prop lines + recs
                "pts_line": pts_line, "reb_line": reb_line, "ast_line": ast_line,
                "pts_rec": pts_rec,   "reb_rec": reb_rec,   "ast_rec": ast_rec,
                # Confidence
                "pts_conf": confidence(pts_std, proj_pts),
                "reb_conf": confidence(reb_std, proj_reb),
                "ast_conf": confidence(ast_std, proj_ast),
                "pts_std": pts_std, "reb_std": reb_std, "ast_std": ast_std,
                # Hit rates
                "hit_pts_l5":  hit_rate("points",  proj_pts, 5),
                "hit_reb_l5":  hit_rate("rebounds", proj_reb, 5),
                "hit_ast_l5":  hit_rate("assists",  proj_ast, 5),
                "hit_pts_l15": hit_rate("points",  proj_pts, 15),
                "hit_reb_l15": hit_rate("rebounds", proj_reb, 15),
                "hit_ast_l15": hit_rate("assists",  proj_ast, 15),
                # Milestones
                "dd_rate": dd_rate, "td_rate": td_rate,
                # Last 5 game log
                "last5": [{"date": clean_date(g["game_date"]),
                           "opp": g["opponent"] or "",
                           "pts": int(g["points"] or 0),
                           "reb": int(g["rebounds"] or 0),
                           "ast": int(g["assists"] or 0),
                           "stl": int(g["steals"] or 0),
                           "blk": int(g["blocks"] or 0),
                           "min": int(g["minutes"] or 0),
                           "pm":  parse_pm(g["plus_minus"]),
                           "result": "W" if int(g["wiz_pts"] or 0) > int(g["opp_pts"] or 0) else "L"}
                          for g in games[-5:]],
            })

        results.sort(key=lambda x: -x["season_pts"])
        return jsonify(results)
    finally:
        conn.close()


# ── Upcoming game prediction model ────────────────────────────────────────────
@app.route("/api/predict")
def get_predict():
    """
    Predict outcomes for user-supplied upcoming matchups.
    Query params: opponent=<team>, home=true|false, rest_days=<int>
    Uses Wizards season stats + home/away splits + rest adjustment.
    """
    conn = get_conn()
    try:
        opponent  = request.args.get("opponent", "").strip()
        is_home   = request.args.get("home", "true").lower() == "true"
        rest_days = int(request.args.get("rest_days", 2))
        opp_rest  = int(request.args.get("opp_rest", 2))

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Wizards scoring by home/away
            cur.execute("""
                SELECT b.home_or_away,
                    AVG(CASE WHEN b.team='Washington Wizards' THEN total_wiz ELSE NULL END) AS avg_pts,
                    AVG(CASE WHEN b.team='Washington Wizards' THEN total_opp ELSE NULL END) AS avg_allowed
                FROM (
                    SELECT game_id, home_or_away, team,
                        SUM(points) OVER (PARTITION BY game_id, CASE WHEN team='Washington Wizards' THEN 1 ELSE 0 END) AS total_wiz,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) OVER (PARTITION BY game_id) AS total_opp
                    FROM wizards_box_data
                    WHERE team='Washington Wizards'
                ) b
                GROUP BY b.home_or_away
            """)
            ha_rows = {r["home_or_away"]: r for r in cur.fetchall() if r["home_or_away"]}

            # Season totals
            cur.execute("""
                SELECT
                    ROUND(AVG(wiz_pts),1) AS avg_pts,
                    ROUND(AVG(opp_pts),1) AS avg_allowed,
                    ROUND(AVG(wiz_pts+opp_pts),1) AS avg_total,
                    ROUND(STDDEV(wiz_pts),1) AS std_pts,
                    COUNT(*) AS games,
                    SUM(CASE WHEN wiz_pts>opp_pts THEN 1 ELSE 0 END) AS wins
                FROM (
                    SELECT game_id,
                        SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                    FROM wizards_box_data GROUP BY game_id
                ) s
            """)
            season = cur.fetchone()

            # Opponent history vs WAS
            opp_games = []
            if opponent:
                cur.execute("""
                    SELECT
                        SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                    FROM wizards_box_data WHERE game_id IN (
                        SELECT DISTINCT game_id FROM wizards_box_data WHERE team=%s
                    )
                    GROUP BY game_id
                """, (opponent,))
                opp_games = [dict(r) for r in cur.fetchall()]

            # Wizards last-5-game form
            cur.execute("""
                SELECT game_id,
                    SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz_pts,
                    SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp_pts
                FROM wizards_box_data GROUP BY game_id
                ORDER BY game_id DESC LIMIT 5
            """)
            last5 = [dict(r) for r in cur.fetchall()]

            # Opponent defensive strength (pts allowed to WAS)
            opp_def_pts = None
            if opponent and opp_games:
                opp_def_pts = round(sum(g["wiz_pts"] for g in opp_games)/len(opp_games), 1)

        # --- Build prediction ---
        avg_pts     = float(season["avg_pts"] or 110)
        avg_allowed = float(season["avg_allowed"] or 120)
        avg_total   = float(season["avg_total"] or 230)
        std_pts     = float(season["std_pts"] or 12)
        win_pct     = round(float(season["wins"] or 0)/float(season["games"] or 1)*100, 1)

        # Home/away adjustment
        location_key = "HOME" if is_home else "AWAY"
        ha = ha_rows.get(location_key, {})
        ha_pts     = float(ha.get("avg_pts") or avg_pts)
        ha_allowed = float(ha.get("avg_allowed") or avg_allowed)

        # Rest adjustment: B2B = -4 pts, 1 day = -2, 2+ days = baseline
        rest_adj = 0
        if rest_days == 1:   rest_adj = -4
        elif rest_days == 2: rest_adj = -2
        elif rest_days >= 3: rest_adj = 1

        # Opponent rest adjustment (their defensive quality)
        opp_rest_adj = 0
        if opp_rest == 1:   opp_rest_adj = -2   # tired defense = more pts for us
        elif opp_rest >= 3: opp_rest_adj = 2    # rested defense = fewer pts for us

        # Last-5 form adjustment
        l5_avg = round(sum(g["wiz_pts"] for g in last5)/len(last5), 1) if last5 else avg_pts
        form_adj = round((l5_avg - avg_pts) * 0.3, 1)

        # Opponent defensive adjustment
        opp_adj = 0
        if opp_def_pts is not None:
            opp_adj = round((opp_def_pts - avg_pts) * 0.4, 1)

        # Final projections
        proj_wiz = round(ha_pts + rest_adj + opp_rest_adj + form_adj + opp_adj, 1)
        proj_opp = round(ha_allowed - rest_adj * 0.5, 1)
        proj_total = round(proj_wiz + proj_opp, 1)
        proj_spread = round(proj_wiz - proj_opp, 1)

        # Win probability (simple sigmoid based on projected spread)
        import math
        win_prob = round(1 / (1 + math.exp(-proj_spread / 10)) * 100, 1)

        # Suggested bet lines
        suggested_ou = round(proj_total / 0.5) * 0.5   # round to .5
        suggested_spread = round(proj_spread / 0.5) * 0.5

        # H2H record
        h2h_wins   = sum(1 for g in opp_games if g["wiz_pts"] > g["opp_pts"])
        h2h_losses = len(opp_games) - h2h_wins

        return jsonify({
            "opponent":         opponent or "Unknown",
            "is_home":          is_home,
            "rest_days":        rest_days,
            "opp_rest":         opp_rest,
            "proj_wiz_pts":     proj_wiz,
            "proj_opp_pts":     proj_opp,
            "proj_total":       proj_total,
            "proj_spread":      proj_spread,
            "win_probability":  win_prob,
            "suggested_ou":     suggested_ou,
            "suggested_spread": suggested_spread,
            "form_adj":         form_adj,
            "rest_adj":         rest_adj,
            "opp_def_adj":      opp_adj,
            "ha_adj":           round(ha_pts - avg_pts, 1),
            "season_avg_pts":   avg_pts,
            "season_avg_allowed": avg_allowed,
            "season_win_pct":   win_pct,
            "l5_avg_pts":       l5_avg,
            "h2h_wins":         h2h_wins,
            "h2h_losses":       h2h_losses,
            "h2h_wiz_avg":      round(sum(g["wiz_pts"] for g in opp_games)/len(opp_games),1) if opp_games else None,
            "h2h_opp_avg":      round(sum(g["opp_pts"] for g in opp_games)/len(opp_games),1) if opp_games else None,
        })
    finally:
        conn.close()


# ── Remaining schedule + batch predictions ────────────────────────────────────
@app.route("/api/schedule/remaining")
def get_remaining():
    """
    Returns all games in the DB plus batch predictions for any game
    that the user marks as upcoming. Since this is historical data,
    we treat the most-recent N games as 'remaining' for demo purposes,
    or the caller can POST a schedule.
    Also returns Wizards' current season trajectory.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT g.game_id, g.game_date, g.game_location, g.game_refs,
                    SUM(CASE WHEN b.team='Washington Wizards' THEN b.points ELSE 0 END) AS wiz_pts,
                    SUM(CASE WHEN b.team<>'Washington Wizards' THEN b.points ELSE 0 END) AS opp_pts,
                    MAX(CASE WHEN b.team<>'Washington Wizards' THEN b.team END) AS opponent,
                    COUNT(DISTINCT b.name) FILTER (WHERE b.team='Washington Wizards') AS wiz_player_count
                FROM wizards_game_data g
                JOIN wizards_box_data b USING (game_id)
                GROUP BY g.game_id, g.game_date, g.game_location, g.game_refs
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)),'Month DD, YYYY') ASC
            """)
            all_games = cur.fetchall()

            # Season-level stats for projection baseline
            cur.execute("""
                SELECT
                    ROUND(AVG(wiz),1) AS avg_pts, ROUND(AVG(opp),1) AS avg_allowed,
                    ROUND(STDDEV(wiz),1) AS std_pts,
                    SUM(CASE WHEN wiz>opp THEN 1 ELSE 0 END) AS wins,
                    COUNT(*) AS total
                FROM (
                    SELECT game_id,
                        SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz,
                        SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp
                    FROM wizards_box_data GROUP BY game_id
                ) s
            """)
            season = cur.fetchone()

            # Last-10 form
            cur.execute("""
                SELECT game_id,
                    SUM(CASE WHEN team='Washington Wizards' THEN points ELSE 0 END) AS wiz,
                    SUM(CASE WHEN team<>'Washington Wizards' THEN points ELSE 0 END) AS opp
                FROM wizards_box_data GROUP BY game_id
                ORDER BY game_id DESC LIMIT 10
            """)
            last10 = [dict(r) for r in cur.fetchall()]

            # Per-opponent scoring history
            cur.execute("""
                SELECT MAX(CASE WHEN team<>'Washington Wizards' THEN team END) AS opp,
                    ROUND(AVG(CASE WHEN team='Washington Wizards' THEN pts ELSE NULL END),1) AS avg_scored,
                    ROUND(AVG(CASE WHEN team<>'Washington Wizards' THEN pts ELSE NULL END),1) AS avg_allowed
                FROM (
                    SELECT game_id, team, SUM(points) AS pts FROM wizards_box_data GROUP BY game_id, team
                ) t GROUP BY game_id
            """)
            opp_history = {}
            for r in cur.fetchall():
                if r["opp"]:
                    if r["opp"] not in opp_history:
                        opp_history[r["opp"]] = {"pts":[],"allowed":[]}
                    opp_history[r["opp"]]["pts"].append(float(r["avg_scored"] or 0))
                    opp_history[r["opp"]]["allowed"].append(float(r["avg_allowed"] or 0))

        import math
        avg_pts     = float(season["avg_pts"] or 110)
        avg_allowed = float(season["avg_allowed"] or 120)
        std_pts     = float(season["std_pts"] or 12)
        win_pct     = float(season["wins"] or 0)/float(season["total"] or 1)
        l10_avg     = round(sum(g["wiz"] for g in last10)/len(last10), 1) if last10 else avg_pts
        l10_wins    = sum(1 for g in last10 if g["wiz"] > g["opp"])
        form_adj    = round((l10_avg - avg_pts) * 0.3, 1)

        def build_prediction(g, idx, total):
            opp = g["opponent"] or ""
            wiz_pts_actual = int(g["wiz_pts"] or 0)
            opp_pts_actual = int(g["opp_pts"] or 0)

            # Opponent defensive adjustment from history
            oh = opp_history.get(opp, {})
            opp_scored_avg = round(sum(oh["pts"])/len(oh["pts"]),1) if oh.get("pts") else avg_pts
            opp_adj = round((opp_scored_avg - avg_pts) * 0.4, 1)

            proj_wiz = round(avg_pts + form_adj + opp_adj, 1)
            proj_opp = round(avg_allowed, 1)
            proj_total = round(proj_wiz + proj_opp, 1)
            proj_spread = round(proj_wiz - proj_opp, 1)
            win_prob = round(1/(1+math.exp(-proj_spread/10))*100, 1)

            # Mock betting line (deterministic seed by game index)
            import math as m
            seed = idx + 1
            def rng(s): x = m.sin(s)*10000; return x - int(x)
            spread_line = max(-18, min(18, round(proj_spread + (rng(seed*3+1)-.5)*6)))
            ou_line     = max(210, min(255, round(proj_total + (rng(seed*3+2)-.5)*14)))
            proj_sp_adj = round(proj_spread + (rng(seed*3+3)-.5)*6, 1)
            proj_tot_adj = round(proj_total + (rng(seed*3+4)-.5)*10, 1)

            covered = (wiz_pts_actual - opp_pts_actual) > spread_line if wiz_pts_actual else None
            went_over = (wiz_pts_actual + opp_pts_actual) > ou_line if wiz_pts_actual else None
            model_edge = "spread" if abs(proj_sp_adj - spread_line) > 3 else ("total" if abs(proj_tot_adj - ou_line) > 4 else None)

            return {
                "game_id":       g["game_id"],
                "date":          clean_date(g["game_date"]),
                "location":      clean_date(g["game_location"]),
                "opponent":      opp,
                "wiz_pts":       wiz_pts_actual,
                "opp_pts":       opp_pts_actual,
                "result":        "W" if wiz_pts_actual > opp_pts_actual else "L" if wiz_pts_actual else None,
                "is_played":     wiz_pts_actual > 0,
                # Prediction
                "proj_wiz":      proj_wiz,
                "proj_opp":      proj_opp,
                "proj_total":    proj_total,
                "proj_spread":   proj_spread,
                "win_prob":      win_prob,
                # Betting lines
                "spread_line":   spread_line,
                "ou_line":       ou_line,
                "proj_spread_adj": proj_sp_adj,
                "proj_total_adj":  proj_tot_adj,
                "covered":       covered,
                "went_over":     went_over,
                "model_edge":    model_edge,
                # Adjustments breakdown
                "opp_adj":       opp_adj,
                "form_adj":      form_adj,
            }

        game_list = [dict(g) for g in all_games]
        predictions = [build_prediction(g, i, len(game_list)) for i, g in enumerate(game_list)]

        return jsonify({
            "games":          predictions,
            "season_avg_pts": avg_pts,
            "season_avg_allowed": avg_allowed,
            "l10_avg":        l10_avg,
            "l10_wins":       l10_wins,
            "form_adj":       form_adj,
            "win_pct":        round(win_pct*100, 1),
        })
    finally:
        conn.close()


# ── Player trend deep-dive ─────────────────────────────────────────────────────
@app.route("/api/props/player/<path:player_name>")
def get_player_props(player_name):
    """
    Deep prop analysis for a single player: full game-by-game log,
    rolling averages, home/away splits, opponent-specific splits,
    variance analysis, and prop line recommendations.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT b.game_id, b.name, b.points, b.rebounds, b.assists,
                    b.steals, b.blocks, b.turnovers, b.minutes, b.fouls,
                    b.o_rebounds, b.d_rebounds,
                    b.field_goal_percentage, b.three_point_percentage,
                    b.free_throw_percentage, b.plus_minus, b."Starter",
                    b.home_or_away, g.game_date, g.game_location,
                    MAX(CASE WHEN b2.team<>'Washington Wizards' THEN b2.team END) AS opponent,
                    SUM(CASE WHEN b2.team='Washington Wizards' THEN b2.points ELSE 0 END) AS wiz_pts,
                    SUM(CASE WHEN b2.team<>'Washington Wizards' THEN b2.points ELSE 0 END) AS opp_pts
                FROM wizards_box_data b
                JOIN wizards_game_data g USING (game_id)
                JOIN wizards_box_data b2 ON b.game_id=b2.game_id
                WHERE b.name=%s AND b.team='Washington Wizards'
                GROUP BY b.game_id, b.name, b.points, b.rebounds, b.assists,
                    b.steals, b.blocks, b.turnovers, b.minutes, b.fouls,
                    b.o_rebounds, b.d_rebounds,
                    b.field_goal_percentage, b.three_point_percentage,
                    b.free_throw_percentage, b.plus_minus, b."Starter",
                    b.home_or_away, g.game_date, g.game_location
                ORDER BY TO_DATE(TRIM(LEADING ', ' FROM TRIM(g.game_date)),'Month DD, YYYY') ASC
            """, (player_name,))
            rows = cur.fetchall()

        if not rows:
            return jsonify({"error": "Player not found"}), 404

        import statistics, math

        games = [dict(r) for r in rows]

        def vals(key): return [float(g[key] or 0) for g in games]
        def avg(lst): return round(sum(lst)/len(lst), 2) if lst else 0
        def rolling(key, n): return avg([float(g[key] or 0) for g in games[-n:]])

        # Per-opponent splits
        opp_splits = defaultdict(lambda: {"pts":[],"reb":[],"ast":[]})
        for g in games:
            opp = g["opponent"] or "Unknown"
            opp_splits[opp]["pts"].append(float(g["points"] or 0))
            opp_splits[opp]["reb"].append(float(g["rebounds"] or 0))
            opp_splits[opp]["ast"].append(float(g["assists"] or 0))
        opp_out = []
        for opp, s in opp_splits.items():
            opp_out.append({
                "opponent": opp,
                "games":    len(s["pts"]),
                "avg_pts":  avg(s["pts"]),
                "avg_reb":  avg(s["reb"]),
                "avg_ast":  avg(s["ast"]),
            })
        opp_out.sort(key=lambda x: -x["games"])

        # Rolling 5-game windows
        rolling5 = []
        for i in range(4, len(games)):
            window = games[i-4:i+1]
            rolling5.append({
                "game_num": i+1,
                "date": clean_date(window[-1]["game_date"]),
                "pts": avg([float(g["points"] or 0) for g in window]),
                "reb": avg([float(g["rebounds"] or 0) for g in window]),
                "ast": avg([float(g["assists"] or 0) for g in window]),
                "min": avg([float(g["minutes"] or 0) for g in window]),
            })

        # Percentile analysis for prop lines
        pts_list = sorted(vals("points"))
        def percentile(lst, p):
            idx = int(len(lst) * p / 100)
            return lst[min(idx, len(lst)-1)]

        season_pts = avg(vals("points"))
        season_reb = avg(vals("rebounds"))
        season_ast = avg(vals("assists"))

        proj_pts = round(season_pts*0.35 + rolling("points",10)*0.35 + rolling("points",5)*0.30, 1)
        proj_reb = round(season_reb*0.35 + rolling("rebounds",10)*0.35 + rolling("rebounds",5)*0.30, 1)
        proj_ast = round(season_ast*0.35 + rolling("assists",10)*0.35 + rolling("assists",5)*0.30, 1)

        pts_std = round(statistics.stdev(vals("points")), 2) if len(games) > 1 else 0
        reb_std = round(statistics.stdev(vals("rebounds")), 2) if len(games) > 1 else 0
        ast_std = round(statistics.stdev(vals("assists")), 2) if len(games) > 1 else 0

        # Hit rates at multiple line thresholds
        def hr(key, line): return round(sum(1 for g in games if float(g[key] or 0) >= line)/len(games)*100, 1)
        def hr_n(key, line, n): src=games[-n:]; return round(sum(1 for g in src if float(g[key] or 0) >= line)/len(src)*100, 1) if src else 0

        pts_thresholds = [10, 15, 20, 25, 30]
        reb_thresholds = [4, 6, 8, 10, 12]
        ast_thresholds = [2, 4, 6, 8, 10]

        full_log = []
        for i, g in enumerate(games):
            fgm, fga = parse_split(g["field_goal_percentage"])
            tpm, tpa = parse_split(g["three_point_percentage"])
            ftm, fta = parse_split(g["free_throw_percentage"])
            full_log.append({
                "game_num": i+1,
                "game_id":  g["game_id"],
                "date":     clean_date(g["game_date"]),
                "opponent": g["opponent"] or "",
                "h_a":      g["home_or_away"] or "",
                "starter":  bool(g["Starter"]),
                "min":      int(g["minutes"] or 0),
                "pts":      int(g["points"] or 0),
                "reb":      int(g["rebounds"] or 0),
                "ast":      int(g["assists"] or 0),
                "stl":      int(g["steals"] or 0),
                "blk":      int(g["blocks"] or 0),
                "tov":      int(g["turnovers"] or 0),
                "oreb":     int(g["o_rebounds"] or 0),
                "dreb":     int(g["d_rebounds"] or 0),
                "fgm": fgm, "fga": fga,
                "tpm": tpm, "tpa": tpa,
                "ftm": ftm, "fta": fta,
                "pm":       parse_pm(g["plus_minus"]),
                "result":   "W" if int(g["wiz_pts"] or 0) > int(g["opp_pts"] or 0) else "L",
                "wiz_pts":  int(g["wiz_pts"] or 0),
                "opp_pts":  int(g["opp_pts"] or 0),
            })

        return jsonify({
            "name":         player_name,
            "games":        len(games),
            "season_pts":   season_pts, "season_reb": season_reb, "season_ast": season_ast,
            "l5_pts":  rolling("points",5),  "l10_pts": rolling("points",10),
            "l5_reb":  rolling("rebounds",5), "l10_reb": rolling("rebounds",10),
            "l5_ast":  rolling("assists",5),  "l10_ast": rolling("assists",10),
            "proj_pts": proj_pts, "proj_reb": proj_reb, "proj_ast": proj_ast,
            "pts_std":  pts_std,  "reb_std": reb_std,   "ast_std": ast_std,
            "pts_p25":  percentile(pts_list, 25), "pts_p50": percentile(pts_list,50),
            "pts_p75":  percentile(pts_list, 75), "pts_p90": percentile(pts_list,90),
            "pts_thresholds": [{"line": t, "hit_rate": hr("points", t), "hit_l10": hr_n("points",t,10)} for t in pts_thresholds],
            "reb_thresholds": [{"line": t, "hit_rate": hr("rebounds", t), "hit_l10": hr_n("rebounds",t,10)} for t in reb_thresholds],
            "ast_thresholds": [{"line": t, "hit_rate": hr("assists", t), "hit_l10": hr_n("assists",t,10)} for t in ast_thresholds],
            "opp_splits":   opp_out,
            "rolling5":     rolling5,
            "log":          list(reversed(full_log)),   # newest first
        })
    finally:
        conn.close()


# ── Season projection & pace ───────────────────────────────────────────────────
@app.route("/api/season/projection")
def get_season_projection():
    """Projects end-of-season stats based on current pace."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT name,
                    COUNT(*) AS games_played,
                    SUM(points) AS total_pts,
                    SUM(rebounds) AS total_reb,
                    SUM(assists) AS total_ast,
                    SUM(steals) AS total_stl,
                    SUM(blocks) AS total_blk,
                    SUM(turnovers) AS total_tov,
                    SUM(CAST(split_part(field_goal_percentage,'-',1) AS INT)) AS fgm,
                    SUM(CAST(split_part(field_goal_percentage,'-',2) AS INT)) AS fga,
                    SUM(CAST(split_part(three_point_percentage,'-',1) AS INT)) AS tpm,
                    SUM(CAST(split_part(three_point_percentage,'-',2) AS INT)) AS tpa
                FROM wizards_box_data WHERE team='Washington Wizards'
                GROUP BY name ORDER BY total_pts DESC
            """)
            rows = cur.fetchall()

        SEASON_GAMES = 82
        results = []
        for r in rows:
            gp = int(r["games_played"] or 0)
            if gp < 5: continue
            rem = max(0, SEASON_GAMES - gp)
            def proj(total): return round(int(total or 0) + (int(total or 0)/gp*rem)) if gp else 0
            results.append({
                "name":          r["name"],
                "games_played":  gp,
                "games_remaining": rem,
                "ppg":   round(int(r["total_pts"] or 0)/gp, 1),
                "rpg":   round(int(r["total_reb"] or 0)/gp, 1),
                "apg":   round(int(r["total_ast"] or 0)/gp, 1),
                "spg":   round(int(r["total_stl"] or 0)/gp, 1),
                "bpg":   round(int(r["total_blk"] or 0)/gp, 1),
                "fg_pct": round(int(r["fgm"] or 0)/int(r["fga"] or 1)*100, 1),
                "tp_pct": round(int(r["tpm"] or 0)/int(r["tpa"] or 1)*100, 1),
                "proj_pts": proj(r["total_pts"]),
                "proj_reb": proj(r["total_reb"]),
                "proj_ast": proj(r["total_ast"]),
                "proj_stl": proj(r["total_stl"]),
                "proj_blk": proj(r["total_blk"]),
            })
        return jsonify(results)
    finally:
        conn.close()


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    for v in ("DB_NAME","DB_USER","DB_PASSWORD"):
        if not os.environ.get(v):
            raise EnvironmentError(f"Missing: {v}")
    print("✓ Starting Wizards Scout API on http://localhost:5000")
    app.run(debug=True, port=5000)