"""pytdxdata 快速上手：报价 / K线 / 分时 / 板块汇总。

运行：
    python examples/quickstart.py

说明：数据来自通达信公开行情服务器（无需账号，15 分钟延时）。
标的一律用字符串（``sh600000`` / ``sz000001`` / ``hk00700`` / ``usAAPL`` / ``cffex:IFL0``）。
"""

import asyncio
import time
from pytdxdata import TdxData,KlinePeriod
from pytdxdata.codec.industry import parse_tdxhy_cfg
import asyncpg,datetime
from dataclasses import asdict
import pandas as pd

K_INSERT="INSERT INTO quants_{} (datetime, code, code_type, open, close, high, low, vol, amount) VALUES ( to_timestamp($1,'YYYY-MM-DD HH24:MI'), $2, $3,$4,$5,$6,$7,$8,$9)  ON CONFLICT DO NOTHING"
CODESQL_0="select ts_code symbol from stock_info  ORDER BY RANDOM()"
CODESQL_1="SELECT CONCAT('sh', zs) AS symbol FROM tdx_index WHERE level in (8,9) ORDER BY RANDOM()"
def convert_symbol(symbol: str) -> str:
    code, market = symbol.split(".")
    return market.lower() + code

async def getandsave(td,pgconn,codetype,kperiod,periodname,count,today=None):
    if today is None:
        today = datetime.datetime.today().strftime('%Y-%m-%d')
    sql=CODESQL_0
    if codetype==1:
        sql=CODESQL_1
    result=await pgconn.fetch(sql)
    t0 = time.monotonic()
    for tmp in result:
        if codetype==0:
            code=convert_symbol(tmp['symbol'])
        else:
            code=tmp['symbol']
        bars = await td.get_bars(code,kperiod,count=count)
        try:
            datas = pd.DataFrame([asdict(b) for b in bars])
            datas["datetime"] = pd.to_datetime(datas[["year", "month", "day", "hour", "minute"]]).dt.strftime("%Y-%m-%d %H:%M")
            datas["codetype"]=codetype
            datas.index = pd.to_datetime(datas.datetime)
            datas=datas.loc[today]
            values = datas.reindex(columns=['datetime','code','codetype','open','close','high','low','vol','amount']).values.tolist()
            await pgconn.executemany(K_INSERT.format(periodname), values)
        except  (KeyError , ValueError):
            continue
    print(f"COMPETEL.{codetype}  {periodname}...{time.monotonic()-t0}")

async def main() -> None:
    # TdxData 为异步上下文管理器：进入时建连，退出时释放
    async with TdxData() as td:
        pgconn=await asyncpg.connect("postgres://quant:quant@192.168.2.249/quant")
        try:
            await getandsave(td,pgconn,0,KlinePeriod.DAY,'kday',2)
            await getandsave(td,pgconn,0,KlinePeriod.MIN_60,'k60',5)
            await getandsave(td,pgconn,0,KlinePeriod.MIN_30,'k30',10)
            await getandsave(td,pgconn,0,KlinePeriod.MIN_5,'k5',50)
            await getandsave(td,pgconn,0,KlinePeriod.MIN_1,'k1',250)
            await getandsave(td,pgconn,1,KlinePeriod.DAY,'kday',2)
            await getandsave(td,pgconn,1,KlinePeriod.MIN_60,'k60',5)
            await getandsave(td,pgconn,1,KlinePeriod.MIN_30,'k30',10)
            await getandsave(td,pgconn,1,KlinePeriod.MIN_5,'k5',50)
            await getandsave(td,pgconn,1,KlinePeriod.MIN_1,'k1',250)
        finally:
            await pgconn.close()

async def main2() -> None:
    # TdxData 为异步上下文管理器：进入时建连，退出时释放
    async with TdxData() as td:
        #bars = await td.get_ticks('899601',codetype=1)
        #bars = await td.get_auction('sh600783')
        bars = await td.get_ticks(['sh601878','sz300115'])
        print(bars)

async def main3():
     async with TdxData() as td:
        #bars = await td.get_ticks('sz300115')
        bars = await td.get_bars('000001',codetype=1)
        print(bars)

async def main4():
    async with TdxData() as td:
        #``block_zs.dat``（行业）/ ``block_gn.dat``（概念）/ ``block_fg.dat``（风格）
        aaa= parse_tdxhy_cfg(await td.get_file("tdxzs.cfg"))
        tdxhy=await td.get_industry_map()
        bb=await td.get_block_parsed('block.dat')
        bzs =await td.get_block_parsed('block_zs.dat')
        bgn =await td.get_block_parsed('block_gn.dat')
        bfg =await td.get_block_parsed('block_fg.dat')
        pass
        

if __name__ == "__main__":
    asyncio.run(main3())
