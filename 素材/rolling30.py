# -*- coding: utf-8 -*-
"""rolling30.py — 近30日新素材看板（滚动30日队列 × 窗口合计消耗）

口径（规格：api_data_sync/docs/superpowers/specs/2026-11-05-rolling30-new-material-dashboard-design.md）：
- 统计日 = 昨日；窗口 = [统计日-29, 统计日] 含头尾，创建窗口与消耗窗口同一段
- 队列 = 素材创建时间 ∈ 窗口 的素材（创建时间脏值 NaT 不入队）
- 行 = 统计日 × 账户 × 编导×剪辑组合；指标 = 素材数(nunique) + 窗口内消耗合计(整体消耗)
- 推送：飞书同 base 新表（FEISHU_BITABLE_ROLLING_TABLE_ID），记录键幂等覆盖
"""
from datetime import timedelta

import pandas as pd

from sucai_every_month_api import (ACCOUNT_NAME, FEISHU, _pure_name,
                                   _to_ms, change_wrong_name, fetch_rows,
                                   fro_j_video, from_who_video, fs_call)

WINDOW_DAYS = 30
UNLABELED = "未标记"

# 数据表字段：名称 → (类型码, 属性)；1=文本 2=数字 5=日期
ROLLING_FIELDS = [
    ("记录键", 1, None),
    ("统计日", 5, {"date_formatter": "yyyy-MM-dd"}),
    ("窗口起", 5, {"date_formatter": "yyyy-MM-dd"}),
    ("窗口止", 5, {"date_formatter": "yyyy-MM-dd"}),
    ("账号名称", 1, None),
    ("编导名称", 1, None),
    ("剪辑名称", 1, None),
    ("近30日素材数", 2, None),
    ("近30日消耗", 2, None),
]

ROLLING_TABLE_ID = ""   # main 集成时由 env 注入（后续任务），保持空默认以可测

_EMPTY_COLS = ["account_id", "素材ID", "日期", "账号名称", "素材名称",
               "素材创建时间", "编导名称", "剪辑名称", "整体消耗"]


def prepare_rolling_df(rows):
    """fetch_rows() 输出 → 聚合所需最小列（复用主脚本的名称解析链路）。"""
    if not rows:
        return pd.DataFrame(columns=_EMPTY_COLS)
    df = pd.DataFrame(rows)
    df = df.rename(columns={"material_id": "素材ID", "stat_date": "日期",
                            "video_name": "素材名称",
                            "material_create_time_v2": "素材创建时间"})
    df["账号名称"] = df["account_id"].map(ACCOUNT_NAME)
    df["素材名称"] = df["素材名称"].fillna("")
    df["全域素材视频名称"] = df["素材名称"].apply(change_wrong_name)
    df["编导"] = df.apply(lambda r: from_who_video(r["全域素材视频名称"], r["账号名称"]), axis=1)
    df["剪辑"] = df.apply(lambda r: fro_j_video(r["全域素材视频名称"], r["账号名称"]), axis=1)
    df["编导名称"] = df.apply(
        lambda r: (_pure_name(r["编导"], r["账号名称"]) or UNLABELED), axis=1)
    df["剪辑名称"] = df.apply(
        lambda r: (_pure_name(r["剪辑"], r["账号名称"]) or UNLABELED), axis=1)
    df["素材创建时间"] = pd.to_datetime(df["素材创建时间"], errors="coerce")
    return df


def filter_cohort(df, stat_date, window_days=WINDOW_DAYS):
    """窗口 = [stat_date-(window_days-1), stat_date]；创建时间 ∈ 窗口（NaT 自动落选）。"""
    win_start = stat_date - timedelta(days=window_days - 1)
    stat = pd.to_datetime(df["日期"])
    created = df["素材创建时间"]
    in_win = (stat >= pd.Timestamp(win_start)) & (stat <= pd.Timestamp(stat_date))
    in_created = (created.dt.date >= win_start) & (created.dt.date <= stat_date)
    return df[in_win & in_created]


def aggregate_rolling(df, stat_date, window_days=WINDOW_DAYS):
    """队列 → 统计日×账户×编导×剪辑 一行：素材数 + 窗口内消耗合计。"""
    win_start = stat_date - timedelta(days=window_days - 1)
    cols = ["记录键", "统计日", "窗口起", "窗口止", "account_id", "账号名称",
            "编导名称", "剪辑名称", "近30日素材数", "近30日消耗"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    g = (df.groupby(["account_id", "账号名称", "编导名称", "剪辑名称"],
                    as_index=False)
           .agg(近30日素材数=("素材ID", "nunique"), 近30日消耗=("整体消耗", "sum")))
    g["近30日消耗"] = g["近30日消耗"].fillna(0).astype(float).round(2)
    g["统计日"] = stat_date
    g["窗口起"] = win_start
    g["窗口止"] = stat_date
    g["记录键"] = g.apply(
        lambda r: f"{r['统计日'].isoformat()}_{r['account_id']}_"
                  f"{r['编导名称']}_{r['剪辑名称']}", axis=1)
    return g[cols]
