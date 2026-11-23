"""水位账册（甲路）：独立账册钉住中心水位，访视侧只读账册。

水位以「窗」为粒度：每个落点行记录 window_start / window_end 与窗指纹。
访视抓取只从最新落点的 window_end 往后开新窗，旧窗已入库行不会被重灌。
"""

from __future__ import annotations

import hashlib
import sqlite3
import uuid

from app.db import get_db
from app.services import audit as audit_svc
from app.services.errors import ServiceError

STATUS_SETTLED = "已落定"
STATUS_MISSING = "缺落点行"
STATUS_TWISTED = "指纹拧花"


def window_fingerprint(site_id: int, window_start: int, window_end: int) -> str:
    raw = f"{int(site_id)}:{int(window_start)}:{int(window_end)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _settled_rows(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM watermark_ledger
        WHERE site_id=? AND status='已落定'
        ORDER BY window_end ASC, id ASC
        """,
        (int(site_id),),
    ).fetchall()


def read_state(conn: sqlite3.Connection, site_id: int) -> dict:
    """只读判定：落点是否存在、指纹是否拧花。不写库。"""
    rows = _settled_rows(conn, site_id)
    if not rows:
        return {
            "site_id": int(site_id),
            "status": STATUS_MISSING,
            "consistency": "不一致",
            "window_start": None,
            "window_end": None,
            "ledger_code": "",
            "window_fingerprint": "",
            "pinned_at": "",
            "landing_count": 0,
        }
    twisted = False
    for row in rows:
        expect = window_fingerprint(
            row["site_id"], row["window_start"], row["window_end"]
        )
        if expect != row["window_fingerprint"]:
            twisted = True
            break
    latest = rows[-1]
    status = STATUS_TWISTED if twisted else STATUS_SETTLED
    return {
        "site_id": int(site_id),
        "status": status,
        "consistency": "一致" if status == STATUS_SETTLED else "不一致",
        "window_start": int(latest["window_start"]),
        "window_end": int(latest["window_end"]),
        "ledger_code": latest["ledger_code"],
        "window_fingerprint": latest["window_fingerprint"],
        "pinned_at": latest["pinned_at"],
        "landing_count": len(rows),
    }


def badge(state: dict) -> str:
    """水位徽标：页面中文标识，与窗止点必须同口径。"""
    end = state.get("window_end")
    end_text = f"窗止点 {end}" if end is not None else "窗止点缺失"
    return f"水位徽标：{state['status']} · {end_text}"


def site_watermark(db_path: str | None = None, site_id: int = 0) -> dict:
    with get_db(db_path) as conn:
        state = read_state(conn, site_id)
    state["badge"] = badge(state)
    return state


def site_watermarks(db_path: str | None = None) -> dict[int, dict]:
    with get_db(db_path) as conn:
        ids = [
            r["id"]
            for r in conn.execute("SELECT id FROM trial_sites ORDER BY id").fetchall()
        ]
        states = {}
        for sid in ids:
            state = read_state(conn, sid)
            state["badge"] = badge(state)
            states[sid] = state
        return states


def list_landings(
    db_path: str | None = None, site_id: int | None = None, limit: int = 100
) -> list[dict]:
    limit = max(1, min(500, int(limit or 100)))
    clauses = ["status='已落定'"]
    params: list = []
    if site_id:
        clauses.append("site_id=?")
        params.append(int(site_id))
    params.append(limit)
    with get_db(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM watermark_ledger
            WHERE {' AND '.join(clauses)}
            ORDER BY id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [dict(r) for r in rows]


def assert_can_advance(conn: sqlite3.Connection, site_id: int) -> dict:
    """抓取/入库前置闸门：徽标与窗止点须一致，否则白话拦下。"""
    state = read_state(conn, site_id)
    if state["status"] == STATUS_MISSING:
        raise _missing_error()
    if state["status"] == STATUS_TWISTED:
        raise _twisted_error()
    return state


def _missing_error():
    return ServiceError(
        "该试验中心还没有可用的水位落点行，整窗重抽已挂起；"
        "请先钉住窗止点再抓取。",
        409,
    )


def _twisted_error():
    return ServiceError(
        "水位徽标与窗止点指纹拧花，校验不通过，已拦下本次抓取与入库；"
        "请重新钉水位落点。",
        409,
    )


def next_window(conn: sqlite3.Connection, site_id: int, count: int) -> dict:
    """在已持写锁的事务内开窗：start 取最新落点 window_end。"""
    state = assert_can_advance(conn, site_id)
    start = int(state["window_end"] or 0)
    end = start + int(count)
    return {
        "site_id": int(site_id),
        "window_start": start,
        "window_end": end,
        "window_fingerprint": window_fingerprint(site_id, start, end),
    }


def record_landing(conn: sqlite3.Connection, window: dict) -> dict:
    """一次落定：写入落点行，唯一索引兜底并发钉点互盖。"""
    code = f"WM-{int(window['site_id']):03d}-{uuid.uuid4().hex[:8].upper()}"
    cur = conn.execute(
        """
        INSERT INTO watermark_ledger(
          ledger_code, site_id, window_start, window_end, window_fingerprint, status
        ) VALUES (?, ?, ?, ?, ?, '已落定')
        """,
        (
            code,
            int(window["site_id"]),
            int(window["window_start"]),
            int(window["window_end"]),
            window["window_fingerprint"],
        ),
    )
    row = conn.execute(
        "SELECT * FROM watermark_ledger WHERE id=?", (cur.lastrowid,)
    ).fetchone()
    return dict(row)


def pin_window(
    site_id: int,
    window_end: int,
    window_start: int | None = None,
    db_path: str | None = None,
) -> dict:
    """后台一次落定：同一中心同一时刻只允许一个钉点事务，失败白话吐出。"""
    site_id = int(site_id or 0)
    window_end = int(window_end or 0)
    if site_id <= 0:
        raise ServiceError("必须指定试验中心")
    if window_end <= 0:
        raise ServiceError("窗止点必须是正整数")
    try:
        with get_db(db_path, immediate=True) as conn:
            site = conn.execute(
                "SELECT id FROM trial_sites WHERE id=?", (site_id,)
            ).fetchone()
            if not site:
                raise ServiceError("试验中心不存在", 404)
            state = read_state(conn, site_id)
            start = (
                int(window_start)
                if window_start is not None
                else int(state["window_end"] or 0)
            )
            if start < 0:
                raise ServiceError("窗起点不能为负")
            occupied = conn.execute(
                """
                SELECT * FROM watermark_ledger
                WHERE site_id=? AND window_end=?
                """,
                (site_id, window_end),
            ).fetchone()
            if occupied:
                expect = window_fingerprint(
                    occupied["site_id"], occupied["window_start"], occupied["window_end"]
                )
                if expect == occupied["window_fingerprint"]:
                    raise ServiceError("该中心此窗止点已有水位落点，不能重复钉点")
                # 指纹拧花的重钉：原地修复指纹，仍是一条落点行。
                conn.execute(
                    """
                    UPDATE watermark_ledger
                    SET window_fingerprint=?, status='已落定', pinned_at=datetime('now')
                    WHERE id=?
                    """,
                    (expect, occupied["id"]),
                )
                landing = dict(
                    conn.execute(
                        "SELECT * FROM watermark_ledger WHERE id=?", (occupied["id"],)
                    ).fetchone()
                )
            else:
                if window_end <= start:
                    raise ServiceError("窗止点必须大于窗起点")
                landing = record_landing(
                    conn,
                    {
                        "site_id": site_id,
                        "window_start": start,
                        "window_end": window_end,
                        "window_fingerprint": window_fingerprint(
                            site_id, start, window_end
                        ),
                    },
                )
    except sqlite3.IntegrityError as exc:
        raise ServiceError(
            "并发钉点撞车，该窗止点已被另一名仓管钉住，请刷新后重试", 409
        ) from exc
    audit_svc.write_event(
        "钉水位落点",
        "watermark_ledger",
        int(landing["id"]),
        f"中心{site_id} 窗{start}→{window_end} {landing['ledger_code']}",
        db_path=db_path,
    )
    state = site_watermark(db_path, site_id)
    state["landing"] = landing
    return state


def landing_summary(db_path: str | None = None, conn=None) -> dict:
    if conn is not None:
        return _landing_summary(conn)
    with get_db(db_path) as own:
        return _landing_summary(own)


def _landing_summary(conn: sqlite3.Connection) -> dict:
    total = conn.execute(
        "SELECT COUNT(*) AS c FROM watermark_ledger WHERE status='已落定'"
    ).fetchone()["c"]
    sites = conn.execute(
        "SELECT COUNT(DISTINCT site_id) AS c FROM watermark_ledger WHERE status='已落定'"
    ).fetchone()["c"]
    latest = conn.execute(
        """
        SELECT site_id, window_end FROM watermark_ledger
        WHERE status='已落定'
        ORDER BY id DESC
        LIMIT 5
        """
    ).fetchall()
    return {
        "landed": total,
        "sites": sites,
        "window_end_summary": "窗止点：" + " · ".join(
            f"中心{r['site_id']}@{r['window_end']}" for r in latest
        ),
    }
