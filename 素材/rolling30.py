# -*- coding: utf-8 -*-
"""rolling30.py — 近30日新素材看板（滚动30日队列 × 窗口合计消耗）

口径（规格：api_data_sync/docs/superpowers/specs/2026-11-05-rolling30-new-material-dashboard-design.md）：
- 统计日 = 昨日；窗口 = [统计日-29, 统计日] 含头尾，创建窗口与消耗窗口同一段
- 队列 = 素材创建时间 ∈ 窗口 的素材（创建时间脏值 NaT 不入队）
- 行 = 统计日 × 账户 × 编导×剪辑组合；指标 = 素材数(nunique) + 窗口内消耗合计(整体消耗)
- 推送：飞书同 base 新表（FEISHU_BITABLE_ROLLING_TABLE_ID），记录键幂等覆盖
"""
import time
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


def rolling_stat_dates(rolling_start, end):
    """--rolling-start D → 统计日序列 [D..end] 含头尾。"""
    if rolling_start > end:
        return []
    return [rolling_start + timedelta(days=i)
            for i in range((end - rolling_start).days + 1)]


def build_rolling_records(agg):
    """聚合 DataFrame → 推送行（原生类型；日期保持 date，飞书序列化在 push 段做）。"""
    out = []
    for r in agg.to_dict("records"):
        out.append({
            "记录键": r["记录键"],
            "统计日": r["统计日"],
            "窗口起": r["窗口起"],
            "窗口止": r["窗口止"],
            "账号名称": r["账号名称"],
            "编导名称": r["编导名称"],
            "剪辑名称": r["剪辑名称"],
            "近30日素材数": int(r["近30日素材数"]),
            "近30日消耗": float(r["近30日消耗"]),
        })
    return out


def _rolling_base():
    return f"/bitable/v1/apps/{FEISHU['app_token']}/tables/{ROLLING_TABLE_ID}"


def ensure_rolling_fields():
    """确保「近30日新素材看板」表字段齐全（缺则建；表本身需在 base 中先建）。"""
    base = _rolling_base()
    have, page_token = set(), ""
    while True:
        d = fs_call("GET", f"{base}/fields",
                    params={"page_size": 100, "page_token": page_token})
        have |= {f["field_name"] for f in d.get("items") or []}
        if not d.get("has_more"):
            break
        page_token = d["page_token"]
    for name, ftype, prop in ROLLING_FIELDS:
        if name not in have:
            fs_call("POST", f"{base}/fields",
                    body={"field_name": name, "type": ftype, "property": prop})
            print(f"  [近30日] 新建字段 {name}")


def push_rolling(records):
    """记录键幂等推送：逐统计日 search（日期字段 is=毫秒时间戳）→ create/update。"""
    if not ROLLING_TABLE_ID:
        print("[近30日] 未配置 FEISHU_BITABLE_ROLLING_TABLE_ID，跳过推送")
        return
    print(f"[近30日] 待推 {len(records)} 行")
    if not records:
        return
    by_key = {r["记录键"]: r for r in records}      # 同键保留最后一条
    records = list(by_key.values())
    ensure_rolling_fields()
    base = _rolling_base()

    key2rid = {}
    for d in sorted({r["统计日"] for r in records}):
        page_token = ""
        while True:
            resp = fs_call("POST", f"{base}/records/search", body={
                "field_names": ["记录键"],
                "filter": {"conjunction": "and", "conditions": [
                    {"field_name": "统计日", "operator": "is",
                     "value": [_to_ms(d)]}]}},
                params={"page_size": 500, "page_token": page_token})
            for item in resp.get("items") or []:
                kv = (item.get("fields") or {}).get("记录键")
                if isinstance(kv, list):
                    kv = "".join(seg.get("text", "") for seg in kv)
                if kv:
                    key2rid[kv] = item["record_id"]
            if not resp.get("has_more"):
                break
            page_token = resp.get("page_token") or ""

    def feishu_fields(r):
        return {
            "记录键": r["记录键"],
            "统计日": _to_ms(r["统计日"]),
            "窗口起": _to_ms(r["窗口起"]),
            "窗口止": _to_ms(r["窗口止"]),
            "账号名称": r["账号名称"],
            "编导名称": r["编导名称"],
            "剪辑名称": r["剪辑名称"],
            "近30日素材数": r["近30日素材数"],
            "近30日消耗": r["近30日消耗"],
        }

    to_create = [{"fields": feishu_fields(r)}
                 for k, r in by_key.items() if k not in key2rid]
    to_update = [{"record_id": key2rid[k], "fields": feishu_fields(r)}
                 for k, r in by_key.items() if k in key2rid]
    for i in range(0, len(to_create), 500):
        fs_call("POST", f"{base}/records/batch_create",
                body={"records": to_create[i:i + 500]})
        time.sleep(0.3)
    for i in range(0, len(to_update), 500):
        fs_call("POST", f"{base}/records/batch_update",
                body={"records": to_update[i:i + 500]})
        time.sleep(0.3)
    print(f"[近30日] 完成：新建 {len(to_create)} 行，更新 {len(to_update)} 行")


def run_rolling(conn, stat_dates, dry_run=False):
    """编排：[min-29, max] 一次取数 → 逐统计日切片聚合 →（dry-run 打印 | 推送）。"""
    if not stat_dates:
        return
    lo = min(stat_dates) - timedelta(days=WINDOW_DAYS - 1)
    hi = max(stat_dates)
    rows = fetch_rows(conn, lo, hi)
    print(f"[近30日] 读取 {len(rows)} 行（{lo} ~ {hi}）")
    df = prepare_rolling_df(rows)
    records = []
    for d in stat_dates:
        sub = filter_cohort(df, d)
        records.extend(build_rolling_records(aggregate_rolling(sub, d)))
    if dry_run:
        print(f"[近30日] dry-run 共 {len(records)} 行，示例：")
        for r in records[:20]:
            print("   ", r)
        return
    push_rolling(records)
