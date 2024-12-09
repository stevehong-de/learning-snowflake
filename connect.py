"""
Dependecies to run the script:
pip3 install snowflake-connector-python
pip3 install "snowflake-connector-python[pandas]"
pip3 install jwt
"""

import pandas as pd
from snowflake import connector

print("Start connection")

ctx = connector.connect(
    user="STEVEHONG",
    password="SbH!0418!!",
    account="gw36690.ap-northeast-2.aws",
    warehouse="SMALLWAREHOUSE",
    database="TESTDB",
    schema="ECOMMERCE",
    autocommit=True,
)

print("OK")

db_cursor_eb = ctx.cursor()
res = db_cursor_eb.execute("""
SELECT CUSTOMERID, COUNT(DISTINCT INVOICENO) AS N_ORDERS
FROM INVOICES
GROUP BY COUNTRY, CUSTOMERID
ORDER BY N_ORDERS DESC
LIMIT 10;
"""
)
# Fetches all records retrieved by the query and formats them in pandas DataFrame
df = res.fetch_pandas_all()
print(df)