# -*- coding: utf-8 -*-
"""rolling30 单元测试：纯函数层（不连 DB、不打飞书）。"""
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

import rolling30
from rolling30 import (WINDOW_DAYS, aggregate_rolling, filter_cohort,
                       prepare_rolling_df)

OFFICIAL = "1757724572785671"   # 弹动官方旗舰店
RENSEN = "1875393773286089"     # 弹动人参旗舰店（#6089）

STAT = date(2026, 11, 4)        # 统计日=昨日
WIN_START = STAT - timedelta(days=WINDOW_DAYS - 1)   # 2026-10-06


def row(acct, mid, stat_date, cost, created, name="x"):
    return {"account_id": acct, "stat_date": stat_date, "material_id": mid,
            "video_name": name, "video_type": None,
            "material_create_time_v2": created, "整体消耗": cost}


@pytest.fixture
def parsed_rows(monkeypatch):
    """不测名字解析本身：把编导/剪辑解析替换成确定值。"""
    monkeypatch.setattr(rolling30, "from_who_video",
                        lambda n, a: f"{a}-张三")
    monkeypatch.setattr(rolling30, "fro_j_video",
                        lambda n, a: f"{a}-李四")
    rows = [
        # 甲：官方 10-10 创建，窗口内两个消耗日（共 300）
        row(OFFICIAL, "M1", date(2026, 10, 20), 100, datetime(2026, 10, 10, 9)),
        row(OFFICIAL, "M1", date(2026, 11, 3), 200, datetime(2026, 10, 10, 9)),
        # 乙：官方 11-03 创建（窗口末端），只在 11-03 消耗 50
        row(OFFICIAL, "M2", date(2026, 11, 3), 50, datetime(2026, 11, 3, 8)),
        # 丙：人参 10-07 创建，窗口内消耗 70（与官方同编导剪辑名，验证按账户拆行）
        row(RENSEN, "M3", date(2026, 10, 20), 70, datetime(2026, 10, 7, 9)),
        # 丁：创建时间在窗口外（10-01 创建）→ 不入队列
        row(OFFICIAL, "M4", date(2026, 10, 20), 999, datetime(2026, 10, 1, 9)),
        # 戊：创建时间脏值 "" → NaT → 不入队列
        row(OFFICIAL, "M5", date(2026, 10, 20), 888, ""),
    ]
    return rows


def test_prepare_basic(parsed_rows):
    df = rolling30.prepare_rolling_df(parsed_rows)
    assert list(df["素材ID"]) == ["M1", "M1", "M2", "M3", "M4", "M5"]
    assert df["素材创建时间"].isna().sum() == 1          # 脏值 → NaT
    assert set(df["编导名称"]) == {"张三"}
    assert set(df["剪辑名称"]) == {"李四"}
    assert set(df["账号名称"]) == {"弹动官方旗舰店", "弹动人参旗舰店"}


def test_prepare_empty():
    df = rolling30.prepare_rolling_df([])
    assert df.empty and "素材ID" in df.columns and "编导名称" in df.columns


def test_filter_cohort(parsed_rows):
    df = rolling30.prepare_rolling_df(parsed_rows)
    sub = rolling30.filter_cohort(df, STAT)
    # 窗口外创建（M4）与脏值（M5）剔除；消耗日全在窗口内所以行数=4
    assert sorted(set(sub["素材ID"])) == ["M1", "M2", "M3"]
    assert len(sub) == 4


def test_aggregate_grouping(parsed_rows):
    df = rolling30.prepare_rolling_df(parsed_rows)
    sub = rolling30.filter_cohort(df, STAT)
    agg = rolling30.aggregate_rolling(sub, STAT)
    # 官方×张三×李四：M1+M2 两素材、300；人参同组合拆开：1 素材、70
    key_official = f"{STAT.isoformat()}_{OFFICIAL}_张三_李四"
    key_rensen = f"{STAT.isoformat()}_{RENSEN}_张三_李四"
    by_key = {r["记录键"]: r for r in agg.to_dict("records")}
    assert by_key[key_official]["近30日素材数"] == 2
    assert by_key[key_official]["近30日消耗"] == 350   # M1 300 + M2 50
    assert by_key[key_rensen]["近30日素材数"] == 1
    assert by_key[key_rensen]["近30日消耗"] == 70
    assert set(agg["窗口起"]) == {WIN_START} and set(agg["窗口止"]) == {STAT}


def test_unlabeled_becomes_marked(monkeypatch):
    """解析为空 → 未标记；"其他"等非空解析结果原样保留。"""
    monkeypatch.setattr(rolling30, "from_who_video", lambda n, a: "")
    monkeypatch.setattr(rolling30, "fro_j_video", lambda n, a: "其他")
    rows = [row(OFFICIAL, "M9", date(2026, 10, 20), 10, datetime(2026, 10, 9, 9))]
    df = rolling30.prepare_rolling_df(rows)
    assert df.iloc[0]["编导名称"] == "未标记"
    assert df.iloc[0]["剪辑名称"] == "其他"


from rolling30 import build_rolling_records, rolling_stat_dates


def test_rolling_stat_dates():
    days = rolling_stat_dates(date(2026, 11, 2), date(2026, 11, 4))
    assert days == [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)]
    assert rolling_stat_dates(date(2026, 11, 4), date(2026, 11, 4)) == [date(2026, 11, 4)]
    assert rolling_stat_dates(date(2026, 11, 5), date(2026, 11, 4)) == []   # start>end → 空


def test_build_records(parsed_rows):
    df = rolling30.prepare_rolling_df(parsed_rows)
    agg = rolling30.aggregate_rolling(rolling30.filter_cohort(df, STAT), STAT)
    recs = build_rolling_records(agg)
    assert len(recs) == 2
    r = recs[0]
    assert isinstance(r["统计日"], date) and isinstance(r["近30日素材数"], int)
    assert isinstance(r["近30日消耗"], float)
    assert set(r) == {"记录键", "统计日", "窗口起", "窗口止", "账号名称",
                      "编导名称", "剪辑名称", "近30日素材数", "近30日消耗"}
    keys = [r["记录键"] for r in recs]
    assert len(keys) == len(set(keys))      # 记录键唯一


from datetime import datetime

import rolling30 as r30
from sucai_every_month_api import TZ8


@pytest.fixture
def fs_calls(monkeypatch):
    """捕获 fs_call 调用；search 返回一条已存在记录用于触发 update 分支。"""
    calls = []

    def fake_fs_call(method, path, body=None, params=None):
        calls.append((method, path, body, params))
        if path.endswith("/records/search"):
            return {"items": [{"record_id": "recEXIST",
                               "fields": {"记录键": "2026-11-04_x_张三_李四"}}],
                    "has_more": False}
        return {}

    monkeypatch.setattr(r30, "fs_call", fake_fs_call)
    return calls


def test_push_rolling_idempotent(fs_calls, monkeypatch):
    monkeypatch.setattr(r30, "ROLLING_TABLE_ID", "tblROLL")
    monkeypatch.setattr(r30, "FEISHU", {"app_token": "appX"})
    recs = [{"记录键": "2026-11-04_x_张三_李四", "统计日": date(2026, 11, 4),
             "窗口起": date(2026, 10, 6), "窗口止": date(2026, 11, 4),
             "账号名称": "弹动官方旗舰店", "编导名称": "张三", "剪辑名称": "李四",
             "近30日素材数": 2, "近30日消耗": 300.0}]
    r30.push_rolling(recs)
    methods = [(m, p) for m, p, *_ in fs_calls]
    # 字段补建 + 逐统计日 search + 仅 update（记录键已存在）
    assert any(p.endswith("/fields") and m == "POST" for m, p in methods)
    searches = [b for m, p, b, _ in fs_calls if p.endswith("/records/search")]
    assert searches and searches[0]["filter"]["conditions"][0]["value"] == \
        [int(datetime(2026, 11, 4).replace(tzinfo=TZ8).timestamp() * 1000)]
    updates = [b for m, p, b, _ in fs_calls if p.endswith("/records/batch_update")]
    creates = [b for m, p, b, _ in fs_calls if p.endswith("/records/batch_create")]
    assert updates and updates[0]["records"][0]["record_id"] == "recEXIST"
    assert not creates
    assert updates[0]["records"][0]["fields"]["近30日消耗"] == 300.0


def test_push_without_table_skips(fs_calls, monkeypatch):
    monkeypatch.setattr(r30, "ROLLING_TABLE_ID", "")
    r30.push_rolling([{"记录键": "k", "统计日": date(2026, 11, 4)}])
    assert fs_calls == []
