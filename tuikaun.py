import pandas as pd
df = pd.read_excel(r"C:\Users\Administrator\Desktop\1759227741_498385cbd6ce8ca203133c8982c6796ccSqUBCXQ.xlsx")
df["是否直播"] = df["流量体裁"].apply(lambda x:'是' if "直播" in x else "否")
df["是否退款"] = df["售后状态"].apply(lambda x:'是' if "退款成功" in x else "否")
print(df.groupby("是否直播").count())
df = df.astype("str")
df.to_excel("订单数据.xlsx",index=False)