# -*- coding: utf-8 -*-
"""素材消耗数据同步：qianchuan.qianchuan_overall_video_material_daily（乘方素材）
→ qianchuan_sucai.t_material_data（中文列，同主键）

业务已切换乘方投放，素材数据一律以乘方接口为准：
- 消耗/成交/ROI/追投等与后台一致；播放类指标乘方接口不回填，同步为 0
- 视频完播数 = 播放数 × 完播率 / 100（乘方表无完播数字段，派生）
- 素材创建时间：乘方表无此字段，从 t_material_data 历史映射回填；映射不到用首见日期近似
- 抖音号：按账号常量映射（与 Excel 导入通道一致）
- 独占写入（方案 D7）：乘方与全域报表素材归因口径不同（当日交集为 0），
  按 账号×日期 先删后插，防止双轨并存导致消耗翻倍
- 幂等可重跑；默认增量最近 3 天（对齐 api 侧 overlap_days=2 的回流修订）
- 昨日源表无行 → 输出 WARNING 并以非零退出（R8：防服务未跑导致静默空转）

用法：
    python -X utf8 sync_material_from_api.py                 # 增量（近3天）
    python -X utf8 sync_material_from_api.py --start 2026-09-01 [--end 2026-09-27]
"""
import os
import sys
from datetime import date, datetime, timedelta

import pymysql

# ---- 配置：环境变量优先（systemd EnvironmentFile 注入），本机默认 localhost ----
DB_CONFIG = {
    "host": os.getenv("SUCAI_DB_HOST", "localhost"),
    "port": int(os.getenv("SUCAI_DB_PORT", "3306")),
    "user": os.getenv("SUCAI_DB_USER", "root"),
    "password": os.getenv("SUCAI_DB_PASSWORD", "123456"),
    "charset": "utf8mb4",
}
QC_DB = "qianchuan"                # 只读：乘方素材表
SUCAI_DB = "qianchuan_sucai"       # 写入：t_material_data
SRC_TABLE = "qianchuan_overall_video_material_daily"

# ---- 账号映射：account_id → (账号名称, 抖音号)。抖音号与 excel_to_mysql 导入通道一致 ----
# 只切直播在投的官方旗舰店；人参直播 09-10 已停（消耗转商品推广，不在直播接口范围）、
# 个人护理无当月投放——两者继续走人工 Excel 通道，恢复直播投放后加入即可
ACCOUNT_MAP = {
    "1757724572785671": ("弹动官方旗舰店", "弹动官方旗舰店"),
}

# ---- 列映射：t_material_data 中文列 → 乘方表列（率值为百分数原样存，与 Excel 同格式） ----
COLUMN_MAP = {
    "整体消耗": "stat_cost_for_roi2",
    "整体支付ROI": "total_prepay_and_pay_order_roi2",
    "整体成交金额": "total_pay_order_gmv_include_coupon_for_roi2",
    "整体成交订单数": "total_pay_order_count_for_roi2",
    "整体成交订单成本": "total_cost_per_pay_order_for_roi2",
    "用户实际支付金额": "total_pay_order_gmv_for_roi2",
    "智能优惠券金额": "total_pay_order_coupon_amount_for_roi2",
    "电商平台补贴金额": "total_ecom_platform_subsidy_amount_for_roi2",
    "整体展现次数": "live_show_count_for_roi2_v2",
    "整体点击率": "live_cvr_rate_for_roi2_v2",
    "整体点击次数": "live_watch_count_for_roi2_v2",
    "整体转化率": "live_convert_rate_for_roi2_v2",
    "基础消耗": "basic_stat_cost_for_roi2_v2",
    "整体成交金额占比": "total_pay_order_gmv_rate_for_roi2",
    "整体消耗占比": "cost_rate_for_roi2",
    "整体点击单价": "total_cpc_for_roi2",
    "整体千次展现费用": "total_ecpm_for_roi2",
    "整体预售订单数": "total_prepay_order_count_for_roi2",
    "整体预售订单金额": "total_prepay_order_gmv_for_roi2",
    "整体未完结预售订单预估金额": "total_unfinished_estimate_order_gmv_for_roi2",
    "净成交ROI": "total_prepay_and_pay_settle_roi2_1h",
    "净成交金额": "total_order_settle_amount_for_roi2_1h",
    "净成交订单数": "total_order_settle_count_for_roi2_1h",
    "净成交订单成本": "total_cost_per_pay_order_settle_for_roi2_1h",
    "用户实际支付净成交金额": "total_order_real_settle_amount_for_roi2_1h",
    "智能优惠券未退款金额": "no_refund_ecom_coupon_amount_for_roi2",
    "电商平台补贴未退款金额": "no_refund_ecom_platform_subsidy_amount_for_roi2",
    "净成交金额结算率": "total_order_settle_amount_rate_for_roi2_1h",
    "净成交订单结算率": "total_order_settle_count_rate_for_roi2_1h",
    "1小时内退款订单数": "total_refund_order_count_for_roi2_1h",
    "1小时内退款金额": "total_refund_order_gmv_for_roi2_1h_all",
    "1小时内退款率": "total_refund_order_gmv_for_roi2_1h_rate",
    "视频点赞数": "video_like_count_for_roi2",
    "新增粉丝数": "video_follow_count_for_roi2",
    "平均观看时长": "video_avg_watch_duration_for_roi2",
    "视频播放数": "video_play_count_for_roi2_v2",
    "视频完播率": "video_play_finish_rate_for_roi2_v2",
    "视频评论数": "video_comment_count_for_roi2_v2",
    "2秒播放率": "video_play_duration_2s_rate_for_roi2",
    "3秒播放率": "video_play_duration_3s_rate_for_roi2",
    "5秒播放率": "video_play_duration_5s_rate_for_roi2",
    "10秒播放率": "video_play_duration_10s_rate_for_roi2",
    "追投调控消耗": "additional_delivery_stat_cost_for_roi2_assist",
    "追投调控成交订单数": "additional_delivery_total_pay_order_count_for_roi2_assist",
    "追投调控成交金额": "ad_total_pay_order_gmv_include_coupon_for_roi2_assist",
    "追投调控支付ROI": "additional_delivery_total_prepay_and_pay_order_roi2_assist",
    "追投调控展示次数": "additional_delivery_show_cnt_for_roi2_assist",
    "追投调控点击率": "additional_delivery_ctr_for_roi2_assist",
    "追投调控点击次数": "additional_delivery_click_cnt_for_roi2_assist",
    "追投调控转化率": "additional_delivery_convert_rate_for_roi2_assist",
    "追投调控用户实际支付金额": "additional_delivery_total_pay_order_gmv_for_roi2_assist",
    "追投调控成交智能优惠券金额": "ad_total_pay_order_coupon_amount_for_roi2_assist",
    "追投调控电商平台补贴金额": "ad_total_ecom_platform_subsidy_amount_for_roi2_assist",
    "追投调控未完结预售订单预估金额": "ad_total_unfinished_estimate_order_gmv_for_roi2_assist",
    "追投调控成交成本": "additional_delivery_pay_convert_cost_for_roi2_assist_v2",
    "追投调控成交人数": "additional_delivery_pay_convert_cnt_for_roi2_assist_v2",
}
PK_COLS = ["素材ID", "抖音号", "账号名称", "日期"]


def parse_args():
    start = end = None
    argv = sys.argv[1:]
    if "--start" in argv:
        start = date.fromisoformat(argv[argv.index("--start") + 1])
    if "--end" in argv:
        end = date.fromisoformat(argv[argv.index("--end") + 1])
    if end is None:
        end = date.today() - timedelta(days=1)
    if start is None:
        start = end - timedelta(days=2)      # 近 3 天窗口
    return start, end


def load_create_time_map(conn):
    """素材ID → 素材创建时间（t_material_data 历史静态映射，Excel 累积全量）"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT `素材ID`, MAX(`素材创建时间`) FROM `{SUCAI_DB}`.`t_material_data` "
                    "WHERE `素材创建时间` IS NOT NULL AND `素材创建时间` <> '' GROUP BY `素材ID`")
        return {r[0]: r[1] for r in cur.fetchall()}


def load_name_map(conn):
    """素材ID → 素材视频名称（取历史最近一次非空名称；乘方主题不回填名称字段）"""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT t.`素材ID`, t.`全域素材视频名称` FROM ("
            f"  SELECT `素材ID`, `全域素材视频名称`,"
            f"         ROW_NUMBER() OVER (PARTITION BY `素材ID` ORDER BY `日期` DESC) rn"
            f"  FROM `{SUCAI_DB}`.`t_material_data` WHERE `全域素材视频名称` <> '') t"
            " WHERE t.rn = 1")
        return dict(cur.fetchall())


def load_first_seen_map(conn, start):
    """素材ID → 乘方表首见日期（映射缺失时的创建时间近似）"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT material_id, MIN(stat_date) FROM `{QC_DB}`.`{SRC_TABLE}` "
                    f"WHERE stat_date >= %s GROUP BY material_id", (start,))
        return {r[0]: r[1] for r in cur.fetchall()}


def fetch_rows(conn, start, end):
    cols = ["account_id", "stat_date", "material_id", "video_name", "video_type"] + \
        sorted(set(COLUMN_MAP.values()))
    col_sql = ", ".join(f"`{c}`" for c in cols)
    ids = ", ".join("'"+k+"'" for k in ACCOUNT_MAP)
    with conn.cursor() as cur:
        cur.execute(f"SELECT {col_sql} FROM `{QC_DB}`.`{SRC_TABLE}` "
                    f"WHERE account_id IN ({ids}) AND stat_date BETWEEN %s AND %s",
                    (start, end))
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]


def transform(rows, ct_map, name_map, seen_map):
    out = []
    for r in rows:
        acct, douyin = ACCOUNT_MAP[r["account_id"]]
        mid = str(r["material_id"])
        created = ct_map.get(mid) or (seen_map.get(mid).strftime("%Y-%m-%d 00:00:00")
                                      if seen_map.get(mid) else None)
        rec = {
            "素材ID": mid,
            "抖音号": douyin,
            "账号名称": acct,
            "日期": r["stat_date"],
            "素材创建时间": created,
            "全域素材视频类型": r["video_type"] or "",
            "全域素材视频名称": r["video_name"] or name_map.get(mid, ""),
        }
        for cn, qc in COLUMN_MAP.items():
            rec[cn] = r.get(qc)
        # 派生：视频完播数 = 播放数 × 完播率 / 100（乘方表无完播数字段）
        play = rec.get("视频播放数") or 0
        finish_rate = rec.get("视频完播率") or 0
        rec["视频完播数"] = round(play * finish_rate / 100) if play and finish_rate else 0
        out.append(rec)
    return out


def upsert(conn, records):
    """独占写入（方案 D7）：按 账号名称×日期 先删后插，事务内完成。

    乘方主题与全域报表的素材归因口径不同（同一消耗总盘、两套素材行，
    实测当日交集为 0），UPSERT 会并存导致翻倍——必须先清当日旧行。
    """
    if not records:
        return 0
    cols = list(records[0].keys())
    col_sql = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = (f"INSERT INTO `{SUCAI_DB}`.`t_material_data` ({col_sql}) "
                  f"VALUES ({placeholders})")
    data = [tuple(r[c] for c in cols) for r in records]
    days = sorted({(r["账号名称"], r["日期"]) for r in records})
    with conn.cursor() as cur:
        for acct, d in days:
            cur.execute(f"DELETE FROM `{SUCAI_DB}`.`t_material_data` "
                        "WHERE `账号名称`=%s AND `日期`=%s", (acct, d))
            print(f"  独占清理 {acct} {d}: 删除 {cur.rowcount} 行旧数据")
        cur.executemany(insert_sql, data)
    return len(data)


def main():
    start, end = parse_args()
    print(f"[素材同步] 乘方表 → t_material_data  {start} ~ {end}")
    conn = pymysql.connect(**DB_CONFIG)
    warn = False
    try:
        rows = fetch_rows(conn, start, end)
        if not rows:
            print(f"[WARNING] 源表 {start}~{end} 无任何行：请检查 api_data_sync 乘方素材同步是否运行")
            warn = True
        ct_map = load_create_time_map(conn)
        name_map = load_name_map(conn)
        seen_map = load_first_seen_map(conn, start)
        records = transform(rows, ct_map, name_map, seen_map)
        n = upsert(conn, records)
        conn.commit()

        # 汇总
        stat = {}
        for r in records:
            k = (r["账号名称"], r["日期"])
            s = stat.setdefault(k, [0, 0.0])
            s[0] += 1
            s[1] += float(r.get("整体消耗") or 0)
        for (acct, d), (cnt, cost) in sorted(stat.items(), key=lambda x: (x[0][0], x[0][1])):
            print(f"  {acct} {d}: {cnt} 行, 消耗 {cost:.2f}")
        named = sum(1 for r in records if r["全域素材视频名称"])
        ct_ok = sum(1 for r in records if r["素材创建时间"])
        print(f"[素材同步] 完成：读取 {len(rows)} 行，写入 {n} 行，"
              f"名称回填 {named}/{len(records)}，创建时间回填 {ct_ok}/{len(records)}")
    finally:
        conn.close()
    if warn:
        sys.exit(1)


if __name__ == "__main__":
    main()
