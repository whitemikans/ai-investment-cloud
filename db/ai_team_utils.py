from __future__ import annotations

import json
import uuid
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from .models import engine


def init_ai_team_tables() -> None:
    with engine.begin() as con:
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS ai_team_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    risk_level TEXT,
                    summary TEXT,
                    market_overview TEXT,
                    recommendations_json TEXT,
                    risk_alerts TEXT,
                    actions_json TEXT,
                    full_report TEXT
                )
                """
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS ai_team_agent_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    agent_name TEXT NOT NULL,
                    output_text TEXT,
                    output_json TEXT
                )
                """
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS agent_feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    run_id TEXT,
                    ticker TEXT,
                    ai_recommendation TEXT,
                    human_decision TEXT,
                    human_reason TEXT,
                    action_taken INTEGER,
                    actual_return_1m REAL
                )
                """
            )
        )
        con.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS agent_research_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    title TEXT,
                    summary TEXT,
                    sentiment TEXT,
                    impact INTEGER,
                    source TEXT,
                    related_tickers TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
        )


def save_ai_team_report(report: dict, agent_outputs: dict[str, dict | str]) -> str:
    init_ai_team_tables()
    run_id = str(report.get("run_id") or uuid.uuid4())
    now = datetime.now().isoformat(timespec="seconds")

    with engine.begin() as con:
        con.execute(
            text(
                """
                INSERT INTO ai_team_reports(
                    run_id, created_at, risk_level, summary, market_overview,
                    recommendations_json, risk_alerts, actions_json, full_report
                )
                VALUES (
                    :run_id, :created_at, :risk_level, :summary, :market_overview,
                    :recommendations_json, :risk_alerts, :actions_json, :full_report
                )
                """
            ),
            {
                "run_id": run_id,
                "created_at": now,
                "risk_level": str(report.get("risk_level", "")),
                "summary": str(report.get("summary", "")),
                "market_overview": str(report.get("market_overview", "")),
                "recommendations_json": json.dumps(report.get("recommendations", []), ensure_ascii=False),
                "risk_alerts": str(report.get("risk_alerts", "")),
                "actions_json": json.dumps(report.get("actions", []), ensure_ascii=False),
                "full_report": str(report.get("full_report", "")),
            },
        )

        for agent_name, output in (agent_outputs or {}).items():
            if isinstance(output, dict):
                out_text = str(output.get("text", ""))
                out_json = json.dumps(output, ensure_ascii=False)
            else:
                out_text = str(output)
                out_json = ""
            con.execute(
                text(
                    """
                    INSERT INTO ai_team_agent_logs(run_id, created_at, agent_name, output_text, output_json)
                    VALUES(:run_id, :created_at, :agent_name, :output_text, :output_json)
                    """
                ),
                {
                    "run_id": run_id,
                    "created_at": now,
                    "agent_name": str(agent_name),
                    "output_text": out_text,
                    "output_json": out_json,
                },
            )
    return run_id


def get_latest_ai_team_report() -> pd.DataFrame:
    init_ai_team_tables()
    sql = """
    SELECT * FROM ai_team_reports
    ORDER BY created_at DESC, id DESC
    LIMIT 1
    """
    return pd.read_sql(text(sql), con=engine)


def get_ai_team_report_history(limit: int = 60) -> pd.DataFrame:
    init_ai_team_tables()
    sql = """
    SELECT id, run_id, created_at, risk_level, summary
    FROM ai_team_reports
    ORDER BY created_at DESC, id DESC
    LIMIT :limit_n
    """
    return pd.read_sql(text(sql), con=engine, params={"limit_n": int(limit)})


def get_ai_team_agent_logs(run_id: str) -> pd.DataFrame:
    init_ai_team_tables()
    sql = """
    SELECT run_id, created_at, agent_name, output_text, output_json
    FROM ai_team_agent_logs
    WHERE run_id = :run_id
    ORDER BY id ASC
    """
    return pd.read_sql(text(sql), con=engine, params={"run_id": str(run_id)})
