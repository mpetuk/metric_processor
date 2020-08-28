
# coding: utf-8

# In[57]:


import pandas as pd
import psycopg2 as pg
import time
import calendar
import sys
import traceback
import pickle
import numpy as np


# In[97]:


def makeDbConn(host=None, user=None, password=None, port=None, dbname=None, dsnString=None):
    if dsnString is not None:
        dsn = dsnString
    else:
        dsn = "host=%s user=%s" % (host, user)

        dbParams = {
            'password': password,
            'port': port,
            'dbname': dbname
        }

        for param in dbParams.keys():
            if dbParams[param] is None:
                continue
            else:
                dsn = dsn + " %s=%s" % (param, dbParams[param])

    try:
        conn = pg.connect("%s" % (dsn))
        return conn
    except Exception as e:
        print ("Failed to connect to db with dsn: %s\nERROR: %s" % (dsn, str(e)))                                     
        return str(e)


# In[98]:


def setup():
    import configparser
    c = configparser.RawConfigParser()
    c.read('../config')
    return c


# In[99]:


config = setup()


# In[100]:


dw_host = config.get('dw', 'host')
dw_port = int(config.get('dw', 'port'))
dw_user = config.get('dw', 'user')
dw_pass = config.get('dw', 'password')
dw_dbname = config.get('dw', 'dbname')

#Create Connection
dw_con = makeDbConn(
    host = dw_host,
    user = dw_user,
    password = dw_pass,
    port = dw_port,
    dbname = dw_dbname)
dw_cur = dw_con.cursor()


# In[6]:


## Build base table 
query_text0="""
                 DROP TABLE IF EXISTS public_work_tbls.mp_driver;
                 CREATE TABLE public_work_tbls.mp_driver AS
                 SELECT  a.vin
                        ,a.vin_last_dt
                        ,a.vin_frst_dt

                        /* DIMENSIONS */
                        ,v.style_id
                        ,a.dealer_id
                        ,CAST(a.dealer_id AS TEXT) ||'*'||CAST(v.style_id AS TEXT) AS dealer_style
                        ,d.zip
                        ,CAST(d.zip AS TEXT) ||'*'||CAST(v.style_id AS TEXT) AS zip_style
                        ,CASE WHEN a.vin_last_dt = CURRENT_DATE - 1  THEN 1 ELSE 0 END AS today_snapshot
                        ,CASE WHEN a.vin_last_dt > CURRENT_DATE - 46 THEN 1 ELSE 0 END AS snapshot_45
                        ,CASE WHEN a.vin_last_dt > CURRENT_DATE - 61 THEN 1 ELSE 0 END AS snapshot_60
                        ,CASE WHEN a.vin_last_dt > CURRENT_DATE - 91 THEN 1 ELSE 0 END AS snapshot_90
                        ,a.sold_flg

                        /* METRICS */
                        ,a.price
                        ,a.mileage
                        ,CASE
                                WHEN v.model_year !~'^([0-9]*)$'
                                THEN NULL
                                WHEN (trim(v.model_year)::INT >= TO_CHAR(CURRENT_DATE, 'YYYY')::INT)
                                THEN a.mileage::NUMERIC::INT
                                ELSE (a.mileage::NUMERIC::INT / (TO_CHAR(CURRENT_DATE, 'YYYY')::INT - trim(v.model_year)::INT)::NUMERIC(10,2))::NUMERIC(10,2)
                         END AS miles_per_year
                        ,vin_last_dt  - vin_frst_dt +1 AS days_on_lot

                FROM dw.dealer_vin_price_sale_hist_c a
                    INNER JOIN dw.dealer_c d ON a.dealer_id = d.dealer_id
                    LEFT OUTER JOIN ods.chrome__vin_c v ON a.vin = v.vin
                    WHERE latest_record_flg
                          AND v.model_year IS NOT NULL and v.style_id is not null
                          AND vin_last_dt > CURRENT_DATE - 121
;
"""
dw_cur.execute(query_text0)
dw_con.commit()


# In[93]:


query_market_driver="""
                 CREATE TABLE public_work_tbls.mp_market_driver AS
                 SELECT vin
                       ,vin_last_dt
                       ,vin_frst_dt
                       ,zip_style
                       ,b.zip1 as zip
                       ,b.zip2
                       ,b.distance
                       ,sold_flg
                       ,price
                       ,mileage
                       ,miles_per_year
                       ,days_on_lot

                FROM 
                (SELECT DISTINCT zip 
                 FROM public_work_tbls.mp_driver
                ) zips
                INNER JOIN dw.zip_distance_2010_c b
                    ON zips.zip = b.zip1
                INNER JOIN public_work_tbls.mp_driver x
                    ON b.zip2 = x.zip
                WHERE b.dist{dist};

;
""".format(dist=100)


# In[24]:


def table_exists(con, table_schema, table_name):
    exists = False
    try:
        cur = con.cursor()
        cur.execute("select exists(select table_name from information_schema.tables where table_name='" + table_name + "' and table_schema = '" + table_schema + "')")
        exists = cur.fetchone()[0]
        cur.close()
    except pg.Error as e:
        print (e)
    return exists


# In[25]:


table_exists(dw_con, 'public_work_tbls','mp_market_driver')


# In[79]:


def create_table(var_, dim_):
    query_text="""
DROP TABLE IF EXISTS public_work_tbls.{var}_{dim}_{days};    
CREATE TABLE public_work_tbls.{var}_{dim}_{days} AS
SELECT {dim}

,SUM(CASE WHEN sold_flg THEN 1 ELSE 0 END) AS cars_sold
,SUM(CASE WHEN not sold_flg THEN 1 ELSE 0 END) AS cars_lstd
,SUM(moved_on) AS cars_moved_on

,ROUND(AVG(CASE WHEN sold_flg AND (ct_by_sold < 6 OR LN({var}) BETWEEN (avg_log - 2*std) AND (avg_log + 2*std)) THEN {var} END)) AS avg_{var}_{dim}_sold
,ROUND(MAX(CASE WHEN sold_flg AND (ct_by_sold < 6 OR LN({var}) BETWEEN (avg_log - 2*std) AND (avg_log + 2*std)) THEN {var} END)) AS max_{var}_{dim}_sold
,ROUND(MIN(CASE WHEN sold_flg AND (ct_by_sold < 6 OR LN({var}) BETWEEN (avg_log - 2*std) AND (avg_log + 2*std)) THEN {var} END)) AS min_{var}_{dim}_sold

,ROUND(AVG(CASE WHEN NOT sold_flg AND (ct_by_sold < 6 OR LN({var}) BETWEEN (avg_log - 2*std) AND (avg_log + 2*std)) THEN {var} END)) AS avg_{var}_{dim}_lstd
,ROUND(MAX(CASE WHEN NOT sold_flg AND (ct_by_sold < 6 OR LN({var}) BETWEEN (avg_log - 2*std) AND (avg_log + 2*std)) THEN {var} END)) AS max_{var}_{dim}_lstd
,ROUND(MIN(CASE WHEN NOT sold_flg AND (ct_by_sold < 6 OR LN({var}) BETWEEN (avg_log - 2*std) AND (avg_log + 2*std)) THEN {var} END)) AS min_{var}_{dim}_lstd

,ROUND(AVG(CASE WHEN sold_flg AND row_id BETWEEN ct_by_sold/2.0 AND ct_by_sold/2.0 + 1 THEN {var} END)) AS median_{var}_sold
,ROUND(AVG(CASE WHEN NOT sold_flg AND row_id BETWEEN ct_by_sold/2.0 AND ct_by_sold/2.0 + 1 THEN {var} END)) AS median_{var}_lstd

,CEIL(SUM(CASE WHEN NOT sold_flg THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN sold_flg AND ct_by_sold > 2 THEN 1 ELSE 0 END)/CAST ({days} AS FLOAT),0)) AS {dim}_days_supply

FROM
(
SELECT DISTINCT {dim}
       ,sold_flg
       ,{var}
       ,COUNT(*) OVER (PARTITION BY {dim}, sold_flg) AS ct_by_sold
       ,CASE WHEN vin_frst_dt > CURRENT_DATE -  {days} THEN 1 ELSE 0 END AS moved_on
       ,AVG(LN({var})) OVER (PARTITION BY {dim}, sold_flg) AS avg_log  
       ,STDDEV(LN({var})) OVER (PARTITION BY {dim}, sold_flg) AS  std
       ,ROW_NUMBER() OVER (PARTITION BY {dim}, sold_flg order by {var}) AS row_id

FROM  {DATA}
WHERE {var} > 0 
       AND vin_last_dt >  CURRENT_DATE -  {days} 
) as foo
GROUP BY {dim}
    """.format( DATA=source, days = 90, var = var_, dim = dim_)
    dw_cur.execute(query_text)
    dw_con.commit()


# In[ ]:


var = ['price', 'mileage', 'mileage_per_year','days_on_lot']
dim = ['style_id','dealer_id', 'zip','zip_style','dealer_style','today_snapshot', 'snapshot_45', 'snapshot_60', 'snapshot_90']


# In[ ]:


for d in dim:
    if d in ['zip', 'zip_style']:
        global source
        source = 'public_work_tbls.mp_market_driver'
        if not table_exists(dw_con, 'public_work_tbls', 'mp_market_driver'):
            dw_cur.execute(query_market_driver)
            dw_con.commit()
    else:
        source = 'public_work_tbls.mp_driver'
    for v in var:
        create_table(v, d)


# In[ ]:


dw_cur.close()
dw_con.close()

