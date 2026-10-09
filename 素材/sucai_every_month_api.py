# -*- coding: utf-8 -*-
"""sucai_every_month_api.py
素材日数据打标报表（API 链路版）：
  读 qianchuan.qianchuan_overall_material_daily（乘方素材，type_v3=3 视频口径）
  → 名称/创建时间回填（t_material_data 历史映射）→ 打标（编导/剪辑/类型/需求/视角/
  命名标准/日爆款）→ 聚合
  → 写 qianchuan 库 t_sucai_daily_report（UPSERT 键：素材ID+账号名称+日期）
  + t_sucai_editor/clipper_daily_report（UPSERT 键：账号名称+编导/剪辑+日期）
  → 推送飞书多维表格「新素材」数据表（一个需求两张视图）：
    · 每日：昨日当月新素材消耗
    · 月度绩效：当月创建的素材 × 当月1号~次月3号的消耗（按 归属月 分组即得）
    行规则：统计日 D 归属当月窗口；D 为 1~3 号时额外归属上月窗口；
            素材只进「素材创建月 == 归属窗口月」的行
    幂等：记录键 = 账户ID_素材ID_归属月_日期，查重后 update/create，随3天回流窗口重推覆盖修订
    同批行落库：qianchuan.t_sucai_new_material_daily（镜像表，PK：账户ID+素材ID+归属月+日期）

与旧版 sucai_every_month.py 的差异：
- 数据源：qianchuan 库乘方素材表（不再读 t_material_data）
- 输出：不再写 Excel / t_sucai_monthly_report
- 去掉：月爆款判定（依赖"日期=全部"行，DATE 列下不存在）
- 部署：Ubuntu，systemd timer 每日 07:00（素材端点 06:30/06:45 完成后）
- 前置检查：昨日素材端点同步状态须 success 且行数>0，否则退出（防半截数据打标）

飞书配置（.env，未配置则跳过推送）：
    FEISHU_APP_ID / FEISHU_APP_SECRET          企业自建应用（需 bitable:app 权限）
    FEISHU_BITABLE_APP_TOKEN / FEISHU_BITABLE_TABLE_ID   多维表格文档与数据表

用法:
    python -X utf8 sucai_every_month_api.py                       # 处理昨日
    python -X utf8 sucai_every_month_api.py --start 2026-09-01 --end 2026-09-29
"""
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pymysql
import requests

# ---- 配置：环境变量优先（与 api_sync .env 同套机制），本机默认 localhost ----
DB = dict(host=os.getenv("API_SYNC_DB_HOST", "localhost"),
          port=int(os.getenv("API_SYNC_DB_PORT", "3306")),
          user=os.getenv("API_SYNC_DB_USER", "root"),
          password=os.getenv("API_SYNC_DB_PASSWORD", "123456"),
          charset="utf8mb4")
QC_DB = "qianchuan"            # 读：乘方素材表
SUCAI_DB = "qianchuan_sucai"   # 写：t_sucai_* 打标输出表

# 同步范围账户（乘方素材表内）
SYNC_ACCOUNTS = ("1757724572785671", "1859998937306571", "1875393773286089")
ACCOUNT_NAME = {"1757724572785671": "弹动官方旗舰店",
                "1859998937306571": "弹动人参旗舰店",
                "1875393773286089": "弹动人参旗舰店"}

# ---- 飞书多维表格（新素材每日消耗 + 月度绩效窗口），未配置则跳过推送 ----
FEISHU = dict(app_id=os.getenv("FEISHU_APP_ID", ""),
              app_secret=os.getenv("FEISHU_APP_SECRET", ""),
              app_token=os.getenv("FEISHU_BITABLE_APP_TOKEN", ""),
              table_id=os.getenv("FEISHU_BITABLE_TABLE_ID", ""))
FEISHU_API = "https://open.feishu.cn/open-apis"
TZ8 = timezone(timedelta(hours=8))   # 飞书日期字段按北京时间转毫秒时间戳

# 数据表字段：名称 → (类型码, 属性)；1=文本 2=数字 5=日期
# （剪辑/日期/消耗 为用户已建字段，沿用命名；其余缺则自动建）
FEISHU_FIELDS = [
    ("记录键", 1, None),
    ("归属月", 1, None),
    ("日期", 5, {"date_formatter": "yyyy-MM-dd"}),
    ("账号名称", 1, None),
    ("素材ID", 1, None),
    ("素材名称", 1, None),
    ("素材创建时间", 5, {"date_formatter": "yyyy-MM-dd HH:mm"}),
    ("编导", 1, None),
    ("剪辑", 1, None),
    ("剪辑名称", 1, None),
    ("整体消耗", 2, None),
    ("整体成交金额", 2, None),
    ("整体成交订单数", 2, None),
    ("整体展现次数", 2, None),
    ("视频播放数", 2, None),
]
FEISHU_NUM_COLS = {"整体消耗": "整体消耗", "整体成交金额": "整体成交金额",
                   "整体成交订单数": "整体成交订单数", "整体展现次数": "整体展现次数",
                   "视频播放数": "视频播放数"}

# 归属月黑名单：这些月的窗口行不产出不落库（2026-09 绩效已另结，用户要求飞书只保留 10 月起；
# 手动回补跨到 10/1~3 时避免重推 9 月尾窗。要恢复某月把它从集合里删掉重跑即可）
EXCLUDE_MONTHS = {"2026-09"}

# ---- 乘方素材表列 → 中文列 ----
COLUMN_MAP = {
    "stat_cost_for_roi2": "整体消耗",
    "total_prepay_and_pay_order_roi2": "整体支付ROI",
    "total_pay_order_gmv_include_coupon_for_roi2": "整体成交金额",
    "total_pay_order_count_for_roi2": "整体成交订单数",
    "total_cost_per_pay_order_for_roi2": "整体成交订单成本",
    "total_pay_order_gmv_for_roi2": "用户实际支付金额",
    "total_pay_order_coupon_amount_for_roi2": "智能优惠券金额",
    "total_ecom_platform_subsidy_amount_for_roi2": "电商平台补贴金额",
    "live_show_count_for_roi2_v2": "整体展现次数",
    "live_cvr_rate_for_roi2_v2": "整体点击率",
    "live_watch_count_for_roi2_v2": "整体点击次数",
    "live_convert_rate_for_roi2_v2": "整体转化率",
    "basic_stat_cost_for_roi2_v2": "基础消耗",
    "total_pay_order_gmv_rate_for_roi2": "整体成交金额占比",
    "cost_rate_for_roi2": "整体消耗占比",
    "total_cpc_for_roi2": "整体点击单价",
    "total_ecpm_for_roi2": "整体千次展现费用",
    "total_prepay_order_count_for_roi2": "整体预售订单数",
    "total_prepay_order_gmv_for_roi2": "整体预售订单金额",
    "total_unfinished_estimate_order_gmv_for_roi2": "整体未完结预售订单预估金额",
    "total_prepay_and_pay_settle_roi2_1h": "净成交ROI",
    "total_order_settle_amount_for_roi2_1h": "净成交金额",
    "total_order_settle_count_for_roi2_1h": "净成交订单数",
    "total_cost_per_pay_order_settle_for_roi2_1h": "净成交订单成本",
    "total_order_real_settle_amount_for_roi2_1h": "用户实际支付净成交金额",
    "no_refund_ecom_coupon_amount_for_roi2": "智能优惠券未退款金额",
    "no_refund_ecom_platform_subsidy_amount_for_roi2": "电商平台补贴未退款金额",
    "total_order_settle_amount_rate_for_roi2_1h": "净成交金额结算率",
    "total_order_settle_count_rate_for_roi2_1h": "净成交订单结算率",
    "total_refund_order_count_for_roi2_1h": "1小时内退款订单数",
    "total_refund_order_gmv_for_roi2_1h_all": "1小时内退款金额",
    "total_refund_order_gmv_for_roi2_1h_rate": "1小时内退款率",
    "video_like_count_for_roi2": "视频点赞数",
    "video_follow_count_for_roi2": "新增粉丝数",
    "video_avg_watch_duration_for_roi2": "平均观看时长",
    "video_play_count_for_roi2_v2": "视频播放数",
    "video_play_finish_rate_for_roi2_v2": "视频完播率",
    "video_comment_count_for_roi2_v2": "视频评论数",
    "video_play_duration_2s_rate_for_roi2": "2秒播放率",
    "video_play_duration_3s_rate_for_roi2": "3秒播放率",
    "video_play_duration_5s_rate_for_roi2": "5秒播放率",
    "video_play_duration_10s_rate_for_roi2": "10秒播放率",
    "additional_delivery_stat_cost_for_roi2_assist": "追投调控消耗",
    "additional_delivery_total_pay_order_count_for_roi2_assist": "追投调控成交订单数",
    "ad_total_pay_order_gmv_include_coupon_for_roi2_assist": "追投调控成交金额",
    "additional_delivery_total_prepay_and_pay_order_roi2_assist": "追投调控支付ROI",
    "additional_delivery_show_cnt_for_roi2_assist": "追投调控展示次数",
    "additional_delivery_ctr_for_roi2_assist": "追投调控点击率",
    "additional_delivery_click_cnt_for_roi2_assist": "追投调控点击次数",
    "additional_delivery_convert_rate_for_roi2_assist": "追投调控转化率",
    "additional_delivery_total_pay_order_gmv_for_roi2_assist": "追投调控用户实际支付金额",
    "ad_total_pay_order_coupon_amount_for_roi2_assist": "追投调控成交智能优惠券金额",
    "ad_total_ecom_platform_subsidy_amount_for_roi2_assist": "追投调控电商平台补贴金额",
    "ad_total_unfinished_estimate_order_gmv_for_roi2_assist": "追投调控未完结预售订单预估金额",
    "additional_delivery_pay_convert_cost_for_roi2_assist_v2": "追投调控成交成本",
    "additional_delivery_pay_convert_cnt_for_roi2_assist_v2": "追投调控成交人数",
}

# ---- 打标规则（1:1 搬自旧版 sucai_every_month.py）----
MANUAL_ID_NAME_MAP = {
    "7673330686392877094": "人参-0813【A-种草-主页素材01】-J钰灵-B子鱼",
    "7673455109986304043": "人参-0813【A-种草-主页素材02】-J钰灵-B子鱼",
    "7673396118743629830": "人参-0813【A-种草-主页素材03】-J钰灵-B子鱼",
}


def get_product_name(name_series):
    name_str = name_series.astype(str)
    coconut = name_str.str.contains("椰子").sum()
    caviar = name_str.str.contains("鱼子酱").sum()
    if coconut > caviar:
        return "椰子"
    if caviar > coconut:
        return "鱼子酱"
    return "其他"


def change_wrong_name(video_name):
    if pd.isna(video_name):
        return ""
    if "达人-健芳" in str(video_name):
        return str(video_name).replace("达人-健芳", "达人健芳")
    return video_name


def from_who_video(video_name, account_name=""):
    prefix = account_name if account_name else ""
    if pd.isna(video_name):
        return prefix + "-其他" if prefix else "其他"
    vn = str(video_name)
    if "梦新" in vn:
        tag = "B梦新"
    elif "子鱼" in vn:
        tag = "B子鱼"
    elif "余倩" in vn:
        tag = "B余倩"
    elif "榆" in vn:
        tag = "林晓榆"
    elif "乐晴" in vn:
        tag = "B乐晴"
    elif "AIGC动态创意" in vn:
        tag = "AIGC"
    elif "B无" in vn:
        tag = "商务自传"
    elif "天爆" in vn:
        tag = "天爆"
    elif "腾飞" in vn:
        tag = "腾飞"
    else:
        tag = "其他"
    return prefix + "-" + tag if prefix else tag


def fro_j_video(video_name, account_name=""):
    prefix = account_name if account_name else ""
    if pd.isna(video_name):
        return prefix + "-其他" if prefix else "其他"
    vn = str(video_name)
    if "佳慧" in vn:
        tag = "J佳慧"
    elif "凯练" in vn:
        tag = "J凯练"
    elif "安褀" in vn or "J安" in vn or "安祺" in vn:
        tag = "J安褀"
    elif "钰灵" in vn:
        tag = "J钰灵"
    elif "伟健" in vn:
        tag = "J伟健"
    elif "学顺" in vn or "榆-李" in vn:
        tag = "J学顺"
    elif "俊彬" in vn:
        tag = "J俊彬"
    elif "陈坤" in vn or "-坤" in vn:
        tag = "J陈坤"
    elif "星骅" in vn:
        tag = "J星骅"
    elif "J无" in vn:
        tag = "J无"
    else:
        tag = "其他"
    return prefix + "-" + tag if prefix else tag


def sucai_label(df):
    def leix(x):
        x = str(x)
        for kw, name in [("配音展示", "配音展示"), ("出境口播", "出境口播"),
                         ("对比测评", "对比测评"), ("营销号", "营销号"),
                         ("店播IP", "店播IP"), ("轻剧情", "轻剧情"),
                         ("打卡", "打卡"), ("溯源", "溯源"),
                         ("live图", "live图"), ("Iive图", "live图"),
                         ("采访", "采访"), ("无", "无")]:
            if kw in x:
                return name
        return "未识别类型"

    def xuqiu(x):
        x = str(x)
        for kw, name in [("价格", "价格"), ("情感", "情感"), ("机制", "机制"),
                         ("种草", "种草"), ("图文", "图文"), ("live图", "图文"),
                         ("Iive图", "图文"), ("剧情", "剧情"), ("痛点", "痛点"),
                         ("AIGC", "AIGC")]:
            if kw in x:
                return name
        return "其他"

    def shijiao(x):
        x = str(x)
        if "商" in x:
            return "商"
        if "用" in x:
            return "用"
        if "专" in x:
            return "专"
        return "未识别视角"

    df["素材类型"] = df["全域素材视频名称"].apply(leix)
    df["素材需求"] = df["全域素材视频名称"].apply(xuqiu)
    df["素材视角"] = df["全域素材视频名称"].apply(shijiao)
    return df


def check_video_name(df):
    df["素材创建时间"] = pd.to_datetime(df["素材创建时间"], errors="coerce")
    df["是否可区分剪辑和编导"] = np.where(
        df["素材创建时间"] < "2025-10-13", "未执行命名标准",
        np.where((df["剪辑"].fillna("") != "") & (df["编导"].fillna("") != "其他"),
                 "是", "否"))
    df["素材类型-达人-AI-编导&剪辑"] = np.where(
        df["编导"].astype(str).str.contains("AIGC", na=False), "AIGC",
        np.where(df["编导"] == "其他", "达人", "编导&剪辑"))
    return df


def check_top_video(df):
    df["按日是否爆款"] = np.where(df["整体消耗"].fillna(0) >= 4000, 1, 0)
    return df


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
        start = end - timedelta(days=2)
    return start, end


def precheck(conn, d):
    cur = conn.cursor()
    cur.execute("""SELECT status, row_count FROM _sync_log
                   WHERE endpoint='overall_material_daily' AND biz_date=%s
                   ORDER BY id DESC LIMIT 1""", (d,))
    r = cur.fetchone()
    if r is None:
        return False, f"{d} 素材端点无同步记录"
    status, rc = r
    if status != "success" or not rc:
        return False, f"{d} 素材端点状态={status} 行数={rc}"
    return True, ""


def fetch_rows(conn, start, end):
    cur = conn.cursor()
    # API 列 AS 中文名：返回行直接以中文列为键
    pairs = sorted(COLUMN_MAP.items(), key=lambda kv: kv[0])
    dims = ["account_id", "stat_date", "material_id", "video_name", "video_type",
            "material_create_time_v2"]
    sel_parts = [f"`{d_}`" for d_ in dims] + [f"`{api}` AS `{cn}`" for api, cn in pairs]
    col_sql = ", ".join(sel_parts)
    ids = ", ".join(f"'{a}'" for a in SYNC_ACCOUNTS)
    cur.execute(f"""SELECT {col_sql} FROM qianchuan_overall_material_daily
                    WHERE stat_date BETWEEN %s AND %s AND account_id IN ({ids})
                    ORDER BY stat_date, material_id""", (start, end))
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


# ---- 飞书推送 ----

def _fs_token():
    """tenant_access_token（进程内缓存，2小时有效期内复用）"""
    global _FS_TOKEN
    if _FS_TOKEN:
        return _FS_TOKEN
    r = requests.post(f"{FEISHU_API}/auth/v3/tenant_access_token/internal",
                      json={"app_id": FEISHU["app_id"],
                            "app_secret": FEISHU["app_secret"]}, timeout=15).json()
    if r.get("code") != 0:
        raise RuntimeError(f"飞书 token 获取失败: {r.get('code')} {r.get('msg')}")
    _FS_TOKEN = r["tenant_access_token"]
    return _FS_TOKEN


_FS_TOKEN = None


def fs_call(method, path, body=None, params=None):
    params = {k: v for k, v in (params or {}).items() if v}   # 空值参数不传（page_token="" 会校验失败）
    r = requests.request(method, f"{FEISHU_API}{path}",
                         headers={"Authorization": f"Bearer {_fs_token()}"},
                         json=body, params=params, timeout=30)
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"飞书 API {path} 失败: {data.get('code')} {data.get('msg')}")
    return data.get("data") or {}


def fs_ensure_fields():
    """确保数据表字段齐全（缺则建），并补两个视图"""
    base = f"/bitable/v1/apps/{FEISHU['app_token']}/tables/{FEISHU['table_id']}"
    have, page_token = set(), ""
    while True:
        d = fs_call("GET", f"{base}/fields",
                    params={"page_size": 100, "page_token": page_token})
        have |= {f["field_name"] for f in d.get("items") or []}
        if not d.get("has_more"):
            break
        page_token = d["page_token"]
    for name, ftype, prop in FEISHU_FIELDS:
        if name not in have:
            fs_call("POST", f"{base}/fields",
                    body={"field_name": name, "type": ftype, "property": prop})
            print(f"  [飞书] 新建字段 {name}")


def _clipper_name(clipper, account_name):
    """剪辑列剔除账号前缀与间隔符 → 纯人名（如 弹动人参旗舰店-B子鱼 → B子鱼）"""
    s = "" if clipper is None or pd.isna(clipper) else str(clipper)
    prefix = f"{account_name}-"
    return s[len(prefix):] if account_name and s.startswith(prefix) else s


def _to_ms(v):
    """datetime/date → 北京时间毫秒时间戳（None/NaT → None）"""
    if isinstance(v, (datetime, date)) and not pd.isna(v):
        if not isinstance(v, datetime):
            v = datetime(v.year, v.month, v.day)
        return int(v.replace(tzinfo=TZ8).timestamp() * 1000)
    return None


def build_feishu_rows(df):
    """生成推送行（原生类型，MySQL 镜像表与飞书共用；飞书格式在 push 段序列化）。
    窗口规则：统计日 D 归属当月；D 为 1~3 号时额外归属上月；
    素材只进「素材创建月 == 归属窗口月」的行——
    9月绩效 = 创建时间在9月的素材 × 9/1~10/3 的消耗，10月同理延续到11/3。
    """
    ct = pd.to_datetime(df["素材创建时间"], errors="coerce")
    out, skipped = [], 0
    for i, row in df.iterrows():
        d, c = row["日期"], ct.iloc[i]
        if pd.isna(c):
            skipped += 1
            continue
        prev = d.replace(day=1) - timedelta(days=1)
        windows = [(d.year, d.month)] + ([(prev.year, prev.month)] if d.day <= 3 else [])
        for y, m in windows:
            if (c.year, c.month) != (y, m):
                continue
            ym = f"{y:04d}-{m:02d}"
            if ym in EXCLUDE_MONTHS:
                continue
            rec = {
                "account_id": str(row["account_id"]),
                "素材ID": str(row["素材ID"]),
                "归属月": ym,
                "日期": d,
                "账号名称": row.get("账号名称"),
                "素材名称": row.get("全域素材视频名称"),
                "素材创建时间": c.to_pydatetime(),
                "编导": row.get("编导"),
                "剪辑": row.get("剪辑"),
                "剪辑名称": _clipper_name(row.get("剪辑"), row.get("账号名称")),
            }
            for col in FEISHU_NUM_COLS.values():
                v = row.get(col)
                rec[col] = None if v is None or pd.isna(v) else float(v)
            out.append(rec)
    return out, skipped


def push_feishu(rows, skipped):
    print(f"[飞书] 待推 {len(rows)} 行（创建时间为空跳过 {skipped} 行）")
    if not rows:
        return
    fs_ensure_fields()
    base = f"/bitable/v1/apps/{FEISHU['app_token']}/tables/{FEISHU['table_id']}"

    # 原生类型 → 飞书格式（日期转毫秒时间戳；键含账户ID防同人参双账户折叠）
    records = []
    for r in rows:
        f = {
            "记录键": f"{r['account_id']}_{r['素材ID']}_{r['归属月']}_{r['日期'].isoformat()}",
            "归属月": r["归属月"],
            "日期": _to_ms(r["日期"]),
            "账号名称": r["账号名称"],
            "素材ID": r["素材ID"],
            "素材名称": r["素材名称"],
            "素材创建时间": _to_ms(r["素材创建时间"]),
            "编导": r["编导"],
            "剪辑": r["剪辑"],
            "剪辑名称": r["剪辑名称"],
        }
        for col in FEISHU_NUM_COLS.values():
            f[col] = r.get(col)
        records.append({"fields": f})

    # 按记录键去重（同键保留最后一条）
    by_key = {r["fields"]["记录键"]: r for r in records}
    records = list(by_key.values())

    # 查已存在记录 → record_id 映射。
    # 飞书文本字段 is 筛选仅支持单值 → 按「归属月」逐值筛选分页拉取，
    # 一次运行最多涉及 2 个月，扫描量恒定不随表增长。
    key2rid = {}
    for ym in sorted({r["fields"]["归属月"] for r in records}):
        page_token = ""
        while True:
            d = fs_call("POST", f"{base}/records/search", body={
                "field_names": ["记录键"],
                "filter": {"conjunction": "and", "conditions": [
                    {"field_name": "归属月", "operator": "is", "value": [ym]}]}},
                params={"page_size": 500, "page_token": page_token})
            for item in d.get("items") or []:
                kv = (item.get("fields") or {}).get("记录键")
                if isinstance(kv, list):   # 富文本形式兜底
                    kv = "".join(seg.get("text", "") for seg in kv)
                if kv:
                    key2rid[kv] = item["record_id"]
            if not d.get("has_more"):
                break
            page_token = d["page_token"]

    to_create = [r for r in records if r["fields"]["记录键"] not in key2rid]
    to_update = [{"record_id": key2rid[r["fields"]["记录键"]], "fields": r["fields"]}
                 for r in records if r["fields"]["记录键"] in key2rid]
    for i in range(0, len(to_create), 500):
        fs_call("POST", f"{base}/records/batch_create",
                body={"records": to_create[i:i + 500]})
        time.sleep(0.3)
    for i in range(0, len(to_update), 500):
        fs_call("POST", f"{base}/records/batch_update",
                body={"records": to_update[i:i + 500]})
        time.sleep(0.3)
    print(f"[飞书] 完成：新建 {len(to_create)} 行，更新 {len(to_update)} 行")


def main():
    start, end = parse_args()
    print(f"[run] {start} ~ {end}")
    conn = pymysql.connect(**DB, database=QC_DB)
    sc = conn

    # ---- 前置检查（处理区间内每一天的素材端点状态）----
    d = start
    while d <= end:
        ok, msg = precheck(conn, d)
        if not ok:
            print(f"[前置检查失败] {msg}", file=sys.stderr)
            sys.exit(1)
        d += timedelta(days=1)

    # ---- 读乘方素材表 ----
    rows = fetch_rows(conn, start, end)
    print(f"[读取] {len(rows)} 行")
    if not rows:
        print("[警告] 无数据行", file=sys.stderr)
        sys.exit(1)

    # ---- 名称/创建时间直接用素材表自带值（API 返回 + 存量已回填）----
    df = pd.DataFrame(rows)
    df = df.rename(columns={"material_id": "素材ID", "stat_date": "日期",
                            "video_name": "素材名称",
                            "material_create_time_v2": "素材创建时间"})
    df["账号名称"] = df["account_id"].map(ACCOUNT_NAME)
    df["素材名称"] = df["素材名称"].fillna("")
    df["素材创建时间"] = df["素材创建时间"].fillna("")

    # ---- 打标 ----
    df["产品名称"] = get_product_name(df["素材名称"])
    df["全域素材视频名称"] = df["素材名称"].apply(change_wrong_name)
    df["编导"] = df.apply(lambda r: from_who_video(r["全域素材视频名称"], r["账号名称"]), axis=1)
    df["剪辑"] = df.apply(lambda r: fro_j_video(r["全域素材视频名称"], r["账号名称"]), axis=1)
    df = sucai_label(df)
    df = check_video_name(df)
    df = check_top_video(df)
    df["视频完播数"] = (df["视频播放数"].fillna(0) * df["视频完播率"].fillna(0) / 100).round()
    df["10秒播放数"] = (df["10秒播放率"].fillna(0) * df["视频播放数"].fillna(0) / 100).round()

    # ---- 生成飞书推送行（键含 account_id，须在英文维度列 drop 之前）----
    fs_rows, fs_skipped = build_feishu_rows(df)

    # ---- 写 t_sucai_daily_report（UPSERT 键：素材ID+账号名称+日期）----
    cur = sc.cursor()
    # 丢弃英文维度列（中文对应列已生成），避免混入 INSERT 列清单
    df = df.drop(columns=["account_id", "stat_date", "material_id", "video_type",
                          "素材名称"], errors="ignore")
    report_cols = [c for c in df.columns]
    ph = ", ".join(["%s"] * len(report_cols))
    col_sql = ", ".join(f"`{c}`" for c in report_cols)
    upd = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in report_cols
                    if c not in ("素材ID", "账号名称", "日期"))
    data = [tuple(None if pd.isna(v) else v for v in r) for r in
            df[report_cols].itertuples(index=False)]
    cur.executemany(
        f"INSERT INTO qianchuan.t_sucai_daily_report ({col_sql}) "
        f"VALUES ({ph}) ON DUPLICATE KEY UPDATE {upd}", data)
    sc.commit()
    print(f"[写入] t_sucai_daily_report: {len(data)} 行")

    # ---- editor/clipper 聚合 ----
    agg_cols = ["整体成交金额", "整体消耗", "整体成交订单数", "整体展现次数",
                "视频播放数", "视频完播数", "3秒播放次数", "5秒播放次数", "10秒播放次数"]
    df["3秒播放次数"] = df["视频播放数"].fillna(0) * df["3秒播放率"].fillna(0) / 100
    df["5秒播放次数"] = df["视频播放数"].fillna(0) * df["5秒播放率"].fillna(0) / 100
    df["10秒播放次数"] = df["10秒播放数"]
    cur2 = conn.cursor()
    for dim_col, table in [("编导", "t_sucai_editor_daily_report"),
                           ("剪辑", "t_sucai_clipper_daily_report")]:
        # 全量版
        g = df.groupby(["账号名称", dim_col, "日期"], as_index=False)[agg_cols].sum()
        g["数据截止日期"] = datetime.combine(end, datetime.min.time())
        gcols = ["账号名称", dim_col, "日期", "数据截止日期"] + agg_cols
        ph2 = ", ".join(["%s"] * len(gcols))
        col2 = ", ".join(f"`{c}`" for c in gcols)
        upd = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in gcols
                        if c not in ("账号名称", dim_col, "日期"))
        cur2.executemany(
            f"INSERT INTO {table} ({col2}) VALUES ({ph2}) ON DUPLICATE KEY UPDATE {upd}",
            [tuple(None if pd.isna(v) else v for v in r) for r in g[gcols].itertuples(index=False)])
        print(f"[写入] {table}: {len(g)} 行")

    # ---- 写 t_sucai_new_material_daily（飞书新素材表镜像，同事务）----
    if fs_rows:
        nm_cols = ["account_id", "素材ID", "归属月", "日期", "账号名称", "素材名称",
                   "素材创建时间", "编导", "剪辑", "剪辑名称"] + list(FEISHU_NUM_COLS.values())
        ph3 = ", ".join(["%s"] * len(nm_cols))
        col3 = ", ".join(f"`{c}`" for c in nm_cols)
        upd3 = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in nm_cols
                         if c not in ("account_id", "素材ID", "归属月", "日期"))
        cur3 = conn.cursor()
        cur3.executemany(
            f"INSERT INTO t_sucai_new_material_daily ({col3}) VALUES ({ph3}) "
            f"ON DUPLICATE KEY UPDATE {upd3}",
            [tuple(r.get(c) for c in nm_cols) for r in fs_rows])
        print(f"[写入] t_sucai_new_material_daily: {len(fs_rows)} 行")

    conn.commit()
    conn.close()

    # ---- 飞书推送（新素材：每日昨日消耗 + 月度绩效窗口）----
    # 放在 MySQL 提交之后：飞书失败不影响入库，exit 1 留 journalctl 痕迹，手动重跑即可
    if FEISHU["app_id"] and FEISHU["app_token"]:
        try:
            push_feishu(fs_rows, fs_skipped)
        except Exception as e:
            print(f"[飞书推送失败] {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("[飞书] 未配置 FEISHU_* 环境变量，跳过推送")
    print("[完成]")


if __name__ == "__main__":
    main()
