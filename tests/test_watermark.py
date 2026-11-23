from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from app.services import reconcile, settings, sites, visits, watermarks
from app.services.errors import ServiceError


def _latest_landing_id(db_path: str, site_id: int) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id FROM watermark_ledger
            WHERE site_id=? AND status='已落定'
            ORDER BY window_end DESC, id DESC
            LIMIT 1
            """,
            (site_id,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def test_crawl_and_ingest_blocked_without_watermark_landing(db_path):
    site = sites.create_site("无水中心", quota=20, db_path=db_path)
    batch = visits.create_batch(site["id"], note="无水位落点批次", db_path=db_path)
    before = visits.list_rows(db_path=db_path, batch_id=batch["id"])
    assert before == []

    with pytest.raises(ServiceError) as crawl_err:
        visits.mock_crawl(batch["id"], count=3, db_path=db_path)
    assert "水位" in crawl_err.value.message
    assert "落点" in crawl_err.value.message

    # 无水位落点仍整窗重抽 = 挂起；库里不得多出任何行
    assert visits.list_rows(db_path=db_path, batch_id=batch["id"]) == []

    listed = visits.list_batches(db_path, site_id=site["id"])[0]
    assert listed["watermark"]["status"] == "缺落点行"
    assert "水位徽标" in listed["watermark"]["badge"]
    assert listed["watermark"]["consistency"] == "不一致"

    with pytest.raises(ServiceError) as ingest_err:
        visits.ingest_ok_rows(batch["id"], db_path=db_path)
    assert "水位" in ingest_err.value.message


def test_crawl_advances_window_without_reinjecting_old_rows(db_path):
    batch = next(
        b
        for b in visits.list_batches(db_path, site_id=1)
        if b["status"] == "采集中"
    )
    site_before = watermarks.site_watermark(db_path, 1)
    assert site_before["status"] == "已落定"
    start = site_before["window_end"]
    assert start == 103

    # 先把合格行入库，验证重抓不会把它们打回待检池
    visits.ingest_ok_rows(batch["id"], db_path=db_path)
    ingested_before = {
        r["subject_key"]: r["status"]
        for r in visits.list_rows(db_path=db_path, batch_id=batch["id"])
        if r["status"] == "已入库"
    }
    assert ingested_before

    first = visits.mock_crawl(batch["id"], count=4, db_path=db_path)
    assert first["created"] == 4
    first_subjects = {r["subject_key"] for r in first["items"]}
    assert first_subjects == {f"S01-{n:04d}" for n in range(start + 1, start + 5)}

    second = visits.mock_crawl(batch["id"], count=3, db_path=db_path)
    assert second["created"] == 3
    second_subjects = {r["subject_key"] for r in second["items"]}
    assert second_subjects == {f"S01-{n:04d}" for n in range(start + 5, start + 8)}
    assert first_subjects.isdisjoint(second_subjects)

    rows = visits.list_rows(db_path=db_path, batch_id=batch["id"])
    keys = [(r["subject_key"], r["visit_code"]) for r in rows]
    assert len(keys) == len(set(keys)), "旧窗不得重灌成重复键"
    after = {r["subject_key"]: r["status"] for r in rows}
    for subject, status in ingested_before.items():
        assert after[subject] == status == "已入库"

    site_after = watermarks.site_watermark(db_path, 1)
    assert site_after["window_end"] == start + 7
    chain = [
        (x["window_start"], x["window_end"])
        for x in watermarks.list_landings(db_path, site_id=1)
    ]
    assert (start, start + 4) in chain
    assert (start + 4, start + 7) in chain


def test_twisted_fingerprint_blocks_crawl_and_ingest(db_path):
    batch = next(
        b
        for b in visits.list_batches(db_path, site_id=1)
        if b["status"] == "采集中"
    )
    landing_id = _latest_landing_id(db_path, 1)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE watermark_ledger SET window_fingerprint=? WHERE id=?",
            ("0" * 64, landing_id),
        )

    listed = visits.list_batches(db_path, site_id=1)
    assert listed
    for item in listed:
        assert item["watermark"]["status"] == "指纹拧花"
        assert "水位徽标" in item["watermark"]["badge"]
        assert item["watermark"]["consistency"] == "不一致"

    before = len(visits.list_rows(db_path=db_path, batch_id=batch["id"]))
    with pytest.raises(ServiceError) as crawl_err:
        visits.mock_crawl(batch["id"], count=2, db_path=db_path)
    assert "指纹" in crawl_err.value.message
    assert "窗止点" in crawl_err.value.message
    assert len(visits.list_rows(db_path=db_path, batch_id=batch["id"])) == before

    with pytest.raises(ServiceError) as ingest_err:
        visits.ingest_ok_rows(batch["id"], db_path=db_path)
    assert "指纹" in ingest_err.value.message


def test_concurrent_crawls_serialize_and_do_not_clobber_pins(db_path):
    batch = next(
        b
        for b in visits.list_batches(db_path, site_id=1)
        if b["status"] == "采集中"
    )
    start = watermarks.site_watermark(db_path, 1)["window_end"]
    before_rows = len(visits.list_rows(db_path=db_path, batch_id=batch["id"]))

    barrier = threading.Barrier(2)
    results: list[dict] = []
    errors: list[Exception] = []

    def work() -> None:
        barrier.wait()
        try:
            results.append(visits.mock_crawl(batch["id"], count=2, db_path=db_path))
        except Exception as exc:  # noqa: BLE001 — 失败必须是白话 ServiceError
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, [str(e) for e in errors]
    assert len(results) == 2
    assert all(r["created"] == 2 for r in results)

    end = watermarks.site_watermark(db_path, 1)["window_end"]
    assert end == start + 4
    chain = [
        (x["window_start"], x["window_end"])
        for x in watermarks.list_landings(db_path, site_id=1)
    ]
    assert chain.count((start, start + 2)) == 1
    assert chain.count((start + 2, start + 4)) == 1
    rows = visits.list_rows(db_path=db_path, batch_id=batch["id"])
    assert len(rows) == before_rows + 4
    keys = [(r["subject_key"], r["visit_code"]) for r in rows]
    assert len(keys) == len(set(keys))


def test_concurrent_pins_on_same_window_do_not_clobber(db_path):
    site = sites.create_site("并发钉点中心", quota=20, db_path=db_path)
    barrier = threading.Barrier(4)
    settled: list[dict] = []
    refused: list[ServiceError] = []

    def work() -> None:
        barrier.wait()
        try:
            settled.append(
                watermarks.pin_window(
                    site["id"], 50, window_start=10, db_path=db_path
                )
            )
        except ServiceError as exc:
            refused.append(exc)

    threads = [threading.Thread(target=work) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(settled) == 1
    assert len(refused) == 3
    for exc in refused:
        assert "已经" in exc.message or "已有" in exc.message

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT window_start, window_end, window_fingerprint
            FROM watermark_ledger
            WHERE site_id=? AND status='已落定'
            """,
            (site["id"],),
        ).fetchall()
    assert rows == [(10, 50, watermarks.window_fingerprint(site["id"], 10, 50))]
    state = watermarks.site_watermark(db_path, site["id"])
    assert state["window_end"] == 50
    assert state["status"] == "已落定"


def test_site_card_and_reconcile_matrix_show_window_end(db_path):
    site = sites.get_site(1, db_path)
    stop = site["window_end_stop"]
    assert stop["status"] == "已落定"
    assert stop["value"] == 103
    assert stop["ledger_code"]
    assert site["watermark_landings"][0]["window_end"] == 103

    listed = next(s for s in sites.list_sites(db_path) if s["id"] == 1)
    assert listed["window_end_stop"]["value"] == 103
    assert listed["watermark_status"] == "已落定"

    matrix = reconcile.site_gap_matrix(db_path)
    row = next(r for r in matrix if r["id"] == 1)
    assert row["window_end"] == 103
    assert row["watermark_status"] == "已落定"
    assert row["ledger_code"]

    dash = reconcile.dashboard(db_path)
    assert "窗止点" in dash["window_end_summary"]
    assert dash["watermarks_landed"] >= 1


def test_settings_exposes_watermark_pin_caliber(db_path):
    snapshot = settings.bulk_snapshot(db_path)
    assert "watermark_pin_caliber" in snapshot
    assert "水位" in snapshot["watermark_pin_caliber"]


def test_pages_have_chinese_watermark_labels():
    static = Path(__file__).resolve().parent.parent / "static"
    text = "\n".join(
        p.read_text(encoding="utf-8")
        for p in sorted(static.rglob("*"))
        if p.suffix in {".js", ".html"}
    )
    for term in ("水位徽标", "窗止点", "水位落点", "水位钉点口径"):
        assert term in text, f"页面缺少中文标识: {term}"
