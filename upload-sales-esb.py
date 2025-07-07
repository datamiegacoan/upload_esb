import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import re

# --- Setup credentials ---
PROJECT_ID = 'mie-gacoan-418408'
DATASET = 'sales_data'

# --- BigQuery client ---
import json
service_account_info = st.secrets["GCP_SERVICE_ACCOUNT"]
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


st.title("🟢 Run BigQuery Update")

query1 = """
--combined esb data & websmart
CREATE OR REPLACE TABLE `mie-gacoan-418408.sales_data.data_combined` AS
with cte_1 as ( 
  SELECT
  `Sales Date` AS Date,
  NEW_CODE_STORE AS RESTO,
  CASE
  WHEN `Visit Purpose` IN ('01.DINE IN','02.TAKE AWAY','06.COMPLIMENT') THEN 'OFFLINE'
  WHEN `Visit Purpose` IN ('03.GOFOOD INT') THEN 'GOFOOD'
  WHEN `Visit Purpose` IN ('04.GRAB FOOD INT') THEN 'GRABFOOD'
  WHEN `Visit Purpose` IN ('05.SHOPEE FOOD INT') THEN 'SHOPEEFOOD'
  ELSE 'Unknown'
  END AS Order_Type,
  COUNT(DISTINCT `Bill Number`) AS total_tc,
  SUM(`Net Sales`) AS total_sales_net
FROM
  `mie-gacoan-418408.sales_data.esb_sales_recapitulation_report` as esb_sales
left join `mie-gacoan-418408.data_stores.master_data` AS master_data
ON esb_sales.Branch = master_data.CODE_GIS
GROUP BY
  Date,
  RESTO,
  Order_Type
ORDER BY
  Date
),

cte_2 as (
select 
Date,
RESTO,
CASE 
  WHEN sales_value.Order_Type IN ('EDC', 'CASH', 'RESERVASI', 'COMPLIMENT', 'QRIS SHOPEE', 'CLOUD VOUCHER', 'OTHERS', 'VOUCHER', 'EDC CIMB', 'QRIS ESB ORDER TABLE', 'OVO', 'DEBIT BCA', 'QRIS ESB KIOSK', 'VISA CARD', 'EDC NIAGA', 'QRISIA', 'QRIS TELKOM', 'TUNAI', 'MASTER CARD', 'FLAZZ', 'EDC BRI QRIS', 'SHOPEE', 'QRIS BNI', 'QRIS', 'GOPAY') THEN 'OFFLINE'
  WHEN sales_value.Order_Type IN ('GRAB FOOD', 'GRAB_FD') THEN 'GRABFOOD'
  WHEN sales_value.Order_Type IN ('SHOPEEPAY', 'SHOPEE PAY', 'SHOPEEFOOD INT') THEN 'SHOPEEFOOD'
  WHEN sales_value.Order_Type IN ('GO RESTO', 'GOFOOD', 'GORESTO') THEN 'GOFOOD'
  WHEN sales_value.Order_Type IN ('OKJEK', 'AIRASIA FOOD') THEN 'OTHER ONLINE'
  ELSE 'Unknown'
END AS Order_Type,
SUM(Value)/1.1 as total_sales_net,
FROM `mie-gacoan-418408.sales_data.sales_value` as sales_value
group by 
Date,
RESTO,
Order_Type
),

cte_3 as (
select 
Date,
RESTO,
CASE 
  WHEN sales_tc.Order_Type IN ('EDC', 'CASH', 'RESERVASI', 'COMPLIMENT', 'QRIS SHOPEE', 'CLOUD VOUCHER', 'OTHERS', 'VOUCHER', 'EDC CIMB', 'QRIS ESB ORDER TABLE', 'OVO', 'DEBIT BCA', 'QRIS ESB KIOSK', 'VISA CARD', 'EDC NIAGA', 'QRISIA', 'QRIS TELKOM', 'TUNAI', 'MASTER CARD', 'FLAZZ', 'EDC BRI QRIS', 'SHOPEE', 'QRIS BNI', 'QRIS', 'GOPAY') THEN 'OFFLINE'
  WHEN sales_tc.Order_Type IN ('GRAB FOOD', 'GRAB_FD') THEN 'GRABFOOD'
  WHEN sales_tc.Order_Type IN ('SHOPEEPAY', 'SHOPEE PAY', 'SHOPEEFOOD INT') THEN 'SHOPEEFOOD'
  WHEN sales_tc.Order_Type IN ('GO RESTO', 'GOFOOD', 'GORESTO') THEN 'GOFOOD'
  WHEN sales_tc.Order_Type IN ('OKJEK', 'AIRASIA FOOD') THEN 'OTHER ONLINE'
  ELSE 'Unknown'
END AS Order_Type,
SUM(Value) as total_tc,
FROM `mie-gacoan-418408.sales_data.sales_tc` as sales_tc
group by 
Date,
RESTO,
Order_Type
),

cte_4 as (
  select
  cte_2.Date,
  cte_2.RESTO,
  cte_2.Order_Type,
  cte_3.total_tc,
  cte_2.total_sales_net
from cte_2
join cte_3
on 
  cte_2.Date = cte_3.Date and
  cte_2.RESTO = cte_3.RESTO and
  cte_2.Order_Type = cte_3.Order_Type
),

cte_5 as (
  SELECT Date, RESTO, Order_Type, total_tc, total_sales_net
  FROM cte_1
  UNION ALL
  SELECT Date, RESTO, Order_Type, total_tc, total_sales_net
  FROM cte_4
)

select
Date,
RESTO,
Order_Type,
CASE 
WHEN Order_Type IN ('GOFOOD', 'GRABFOOD','SHOPEEFOOD') THEN 'ONLINE'
ELSE 'OFFLINE'
END AS Order_Type_Group,
total_tc,
total_sales_net
from cte_5
;

-- daily1 -- detail order type------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.daily1` AS
with cte_1 as(
select 
  data_combined.Date, 
  data_combined.RESTO,
  data_combined.Order_Type,
  data_combined.Order_Type_Group,
  data_combined.total_tc,
  data_combined.total_sales_net,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS
from `mie-gacoan-418408.sales_data.data_combined` as data_combined
left join `mie-gacoan-418408.data_stores.master_data` AS master_data
on data_combined.RESTO = master_data.NEW_CODE_STORE
)

select
  Date,
  RESTO,
  Order_Type,
  Order_Type_Group,
  total_sales_net,
  total_tc,
  LAG((total_sales_net)/1.1) OVER (PARTITION BY RESTO, Order_Type ORDER BY Date) AS previous_sales_net,
  LAG((total_tc)) OVER (PARTITION BY RESTO, Order_Type ORDER BY Date) AS previous_tc,
  OPEN_STORE,
  NAMA_OPS,
  AREA_HEAD,
  RM,
  CITY,
  AM,
  PROVINSI,
  KOTA,
  JAVA___OUTER_JAVA,
  TYPE_KITCHEN,
  STATUS,
  CODE_GIS
from cte_1
;

--daily2 -- detail order type group ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.daily2` AS
with cte_1 as (
  select
  data_combined.Date, 
  data_combined.RESTO,
  data_combined.Order_Type_Group,
  sum(data_combined.total_sales_net) as total_sales_net,
  sum(data_combined.total_tc) as total_tc,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS  
from `mie-gacoan-418408.sales_data.data_combined` as data_combined
left join `mie-gacoan-418408.data_stores.master_data` AS master_data
on data_combined.RESTO = master_data.NEW_CODE_STORE
group by
  data_combined.Date, 
  data_combined.RESTO,
  data_combined.Order_Type_Group,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS
)

select
  cte_1.Date,
  cte_1.RESTO,
  cte_1.Order_Type_Group,
  cte_1.total_sales_net,
  cte_1.total_tc,
  LAG(cte_1.total_sales_net) OVER (PARTITION BY cte_1.RESTO, cte_1.Order_Type_Group ORDER BY cte_1.Date) AS previous_sales_net,
  LAG(cte_1.total_tc) OVER (PARTITION BY cte_1.RESTO, cte_1.Order_Type_Group ORDER BY cte_1.Date) AS previous_tc,
  cte_1.OPEN_STORE,
  cte_1.NAMA_OPS,
  cte_1.AREA_HEAD,
  cte_1.RM,
  cte_1.CITY,
  cte_1.AM,
  cte_1.PROVINSI,
  cte_1.KOTA,
  cte_1.JAVA___OUTER_JAVA,
  cte_1.TYPE_KITCHEN,
  cte_1.STATUS,
  cte_1.CODE_GIS
from cte_1
order by cte_1.Date desc
;

--daily3 -- detail---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.daily3` AS

with cte_1 as (
  select
  data_combined.Date, 
  data_combined.RESTO,
  sum(data_combined.total_sales_net) as total_sales_net,
  sum(data_combined.total_tc) as total_tc,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS
from `mie-gacoan-418408.sales_data.data_combined` as data_combined
left join `mie-gacoan-418408.data_stores.master_data` AS master_data
on data_combined.RESTO = master_data.NEW_CODE_STORE
group by
  data_combined.Date, 
  data_combined.RESTO,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS
)

select
  cte_1.Date,
  cte_1.RESTO,
  cte_1.total_sales_net,
  cte_1.total_tc,
  LAG(cte_1.total_sales_net) OVER (PARTITION BY cte_1.RESTO ORDER BY cte_1.Date) AS previous_sales_net,
  LAG(cte_1.total_tc) OVER (PARTITION BY cte_1.RESTO ORDER BY cte_1.Date) AS previous_tc,
  cte_1.OPEN_STORE,
  cte_1.NAMA_OPS,
  cte_1.AREA_HEAD,
  cte_1.RM,
  cte_1.CITY,
  cte_1.AM,
  cte_1.PROVINSI,
  cte_1.KOTA,
  cte_1.JAVA___OUTER_JAVA,
  cte_1.TYPE_KITCHEN,
  cte_1.STATUS,
  cte_1.CODE_GIS
from cte_1
;

--monthly1 - detail order type -----------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.monthly1` AS
with cte_1 as (
  select
  DATE_TRUNC(data_combined.Date, month) as month, 
  data_combined.RESTO,
  data_combined.Order_Type,
  data_combined.Order_Type_Group,
  sum(data_combined.total_sales_net) as total_sales_net,
  sum(data_combined.total_tc) as total_tc,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS  
from `mie-gacoan-418408.sales_data.data_combined` as data_combined
left join `mie-gacoan-418408.data_stores.master_data` AS master_data
on data_combined.RESTO = master_data.NEW_CODE_STORE
group by
  month,
  data_combined.RESTO,
  data_combined.Order_Type,
  data_combined.Order_Type_Group,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS
),

cte_2 as (
  select 
    DATE_TRUNC(daily1.Date, month) as month,
    daily1.RESTO,
    daily1.Order_Type_Group,
    daily1.Order_Type,
    avg(daily1.total_sales_net) as avg_sales_net,
    avg(daily1.total_tc) as avg_tc,
  from mie-gacoan-418408.dashboard_sales.daily1 as daily1
  group by
    month,
    daily1.RESTO,
    daily1.Order_Type_Group,
    daily1.Order_Type
)

select
  cte_1.month,
  cte_1.RESTO,
  cte_1.Order_Type_Group,
  cte_1.Order_Type,
  cte_1.total_sales_net,
  cte_1.total_tc,
  cte_2.avg_sales_net,
  cte_2.avg_tc,
  LAG(cte_1.total_sales_net) OVER (PARTITION BY cte_1.RESTO, cte_1.Order_Type_Group,cte_1.Order_Type ORDER BY cte_1.month) AS previous_sales_net,
  LAG(cte_1.total_tc) OVER (PARTITION BY cte_1.RESTO, cte_1.Order_Type_Group,cte_1.Order_Type ORDER BY cte_1.month) AS previous_tc,
  LAG(cte_2.avg_sales_net) OVER (PARTITION BY cte_1.RESTO, cte_1.Order_Type_Group,cte_1.Order_Type ORDER BY cte_1.month) AS avg_previous_sales_net,
  LAG(cte_2.avg_tc) OVER (PARTITION BY cte_1.RESTO, cte_1.Order_Type_Group,cte_1.Order_Type ORDER BY cte_1.month) AS avg_previous_tc,
  cte_1.OPEN_STORE,
  cte_1.NAMA_OPS,
  cte_1.AREA_HEAD,
  cte_1.RM,
  cte_1.CITY,
  cte_1.AM,
  cte_1.PROVINSI,
  cte_1.KOTA,
  cte_1.JAVA___OUTER_JAVA,
  cte_1.TYPE_KITCHEN,
  cte_1.STATUS,
  cte_1.CODE_GIS
from cte_1
left join cte_2
on 
  cte_1.month = cte_2.month and 
  cte_1.RESTO = cte_2.RESTO and
  cte_1.Order_Type_Group = cte_2.Order_Type_Group and
  cte_1.Order_Type = cte_2.Order_Type
;

--monthly2 detail order type group -----------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.monthly2` AS
with cte_1 as (
  select
  DATE_TRUNC(data_combined.Date, month) as month, 
  data_combined.RESTO,
  data_combined.Order_Type_Group,
  sum(data_combined.total_sales_net) as total_sales_net,
  sum(data_combined.total_tc) as total_tc,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS
from `mie-gacoan-418408.sales_data.data_combined` as data_combined
left join `mie-gacoan-418408.data_stores.master_data` AS master_data
on data_combined.RESTO = master_data.NEW_CODE_STORE
group by
  month,
  data_combined.RESTO,
  data_combined.Order_Type_Group,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS
),

cte_2 as (
  select 
    DATE_TRUNC(Daily2.Date, month) as month,
    Daily2.RESTO,
    Daily2.Order_Type_Group,
    avg(Daily2.total_sales_net) as avg_sales_net,
    avg(Daily2.total_tc) as avg_tc,
  from mie-gacoan-418408.dashboard_sales.daily2 as daily2
  group by
    month,
    daily2.RESTO,
    daily2.Order_Type_Group
)

select
  cte_1.month,
  cte_1.RESTO,
  cte_1.Order_Type_Group,
  cte_1.total_sales_net,
  cte_1.total_tc,
  cte_2.avg_sales_net,
  cte_2.avg_tc,
  LAG(cte_1.total_sales_net) OVER (PARTITION BY cte_1.RESTO,cte_1.Order_Type_Group ORDER BY cte_1.month) AS previous_sales_net,
  LAG(cte_1.total_tc) OVER (PARTITION BY cte_1.RESTO,cte_1.Order_Type_Group ORDER BY cte_1.month) AS previous_tc,
  LAG(cte_2.avg_sales_net) OVER (PARTITION BY cte_1.RESTO,cte_1.Order_Type_Group ORDER BY cte_1.month) AS avg_previous_sales_net,
  LAG(cte_2.avg_tc) OVER (PARTITION BY cte_1.RESTO,cte_1.Order_Type_Group ORDER BY cte_1.month) AS avg_previous_tc,
  cte_1.OPEN_STORE,
  cte_1.NAMA_OPS,
  cte_1.AREA_HEAD,
  cte_1.RM,
  cte_1.CITY,
  cte_1.AM,
  cte_1.PROVINSI,
  cte_1.KOTA,
  cte_1.JAVA___OUTER_JAVA,
  cte_1.TYPE_KITCHEN,
  cte_1.STATUS,
  cte_1.CODE_GIS
from cte_1
left join cte_2
on 
  cte_1.month = cte_2.month and 
  cte_1.RESTO = cte_2.RESTO and
  cte_1.Order_Type_Group = cte_2.Order_Type_Group
;

--monthly3 -- detail -----------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.monthly3` AS
with cte_1 as (
  select
  DATE_TRUNC(data_combined.Date, month) as month, 
  data_combined.RESTO,
  sum(data_combined.total_sales_net) as total_sales_net,
  sum(data_combined.total_tc) as total_tc,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS  
from `mie-gacoan-418408.sales_data.data_combined` as data_combined
left join `mie-gacoan-418408.data_stores.master_data` AS master_data
on data_combined.RESTO = master_data.NEW_CODE_STORE
group by
  month,
  data_combined.RESTO,
  master_data.OPEN_STORE,
  master_data.NAMA_OPS,
  master_data.AREA_HEAD,
  master_data.RM,
  master_data.CITY,
  master_data.AM,
  master_data.PROVINSI,
  master_data.KOTA,
  master_data.JAVA___OUTER_JAVA,
  master_data.TYPE_KITCHEN,
  master_data.STATUS,
  master_data.CODE_GIS
),

cte_2 as (
  select 
    DATE_TRUNC(Daily3.Date, month) as month,
    Daily3.RESTO,
    avg(Daily3.total_sales_net) as avg_sales_net,
    avg(Daily3.total_tc) as avg_tc,
  from mie-gacoan-418408.dashboard_sales.daily3 as daily3
  group by
    month,
    daily3.RESTO
)

select
  cte_1.month,
  cte_1.RESTO,
  cte_1.total_sales_net,
  cte_1.total_tc,
  cte_2.avg_sales_net,
  cte_2.avg_tc,
  LAG(cte_1.total_sales_net) OVER (PARTITION BY cte_1.RESTO ORDER BY cte_1.month) AS previous_sales_net,
  LAG(cte_1.total_tc) OVER (PARTITION BY cte_1.RESTO ORDER BY cte_1.month) AS previous_tc,
  LAG(cte_2.avg_sales_net) OVER (PARTITION BY cte_1.RESTO ORDER BY cte_1.month) AS avg_previous_sales_net,
  LAG(cte_2.avg_tc) OVER (PARTITION BY cte_1.RESTO ORDER BY cte_1.month) AS avg_previous_tc,
  cte_1.OPEN_STORE,
  cte_1.NAMA_OPS,
  cte_1.AREA_HEAD,
  cte_1.RM,
  cte_1.CITY,
  cte_1.AM,
  cte_1.PROVINSI,
  cte_1.KOTA,
  cte_1.JAVA___OUTER_JAVA,
  cte_1.TYPE_KITCHEN,
  cte_1.STATUS,
  cte_1.CODE_GIS
from cte_1
left join cte_2
on 
  cte_1.month = cte_2.month and 
  cte_1.RESTO = cte_2.RESTO
;

--sales 90days--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.sales_90days` AS

with 
cte_1 as (
  select 
    Date, 
    RESTO, 
    OPEN_STORE, 
    total_sales_net, 
    DATE_DIFF(DATE(TIMESTAMP(DATE)), OPEN_STORE, DAY)+1 AS days_opened
from `mie-gacoan-418408.dashboard_sales.daily3`
where OPEN_STORE >= '2024-10-01'
group by 
  Date, 
  RESTO, 
  OPEN_STORE,
  total_sales_net
)

select *
from cte_1
where days_opened <= 90
;

--sales nso-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.sales_nso` AS
SELECT Date, RESTO, NAMA_OPS, OPEN_STORE, PROVINSI, KOTA, JAVA___OUTER_JAVA, sum(total_sales_net) as sales_nett  
FROM mie-gacoan-418408.dashboard_sales.daily1
WHERE EXTRACT(YEAR FROM Date) = EXTRACT(YEAR FROM OPEN_STORE)
and Date >= '2025-01-01'
group by Date, RESTO, NAMA_OPS, OPEN_STORE, PROVINSI, KOTA, JAVA___OUTER_JAVA
order by Date asc
;

--tracker_bod_daily------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.tracker_bod.tracker_bod_daily` AS
with 
  cte_1 as (
    select 
      code,
      date_daily,
      date_trunc(date_daily, month) as month,
      target_sales_nett
    from `mie-gacoan-418408.sales_data.target_sales_2025`
  ),

  cte_2 as (
    select 
      Date,
      DATE_TRUNC(Date, month) as month,
      RESTO,
      sum(total_sales_net) as actual_sales_nett
    from `mie-gacoan-418408.dashboard_sales.daily1`
    group by
      Date,
      RESTO
  ),

  cte_3 as (
    select
      date as month,
      RESTO,
      percentage_gap,
      sales_taf
    from `mie-gacoan-418408.sales_data.data_sales_taf`
  ),

  cte_4 as(
    select
    cte_1.code,
    cte_1.date_daily,
    cte_1.month,
    cte_1.target_sales_nett,
    cte_2.actual_sales_nett
    from cte_1
    left join cte_2
    on cte_1.code = cte_2.RESTO and cte_1.date_daily = cte_2.Date
  ),

  cte_5 as(
    select
    cte_4.code,
    cte_4.date_daily,
    cte_4.month,
    cte_4.target_sales_nett,
    cte_4.actual_sales_nett,
    CASE
    when cte_4.code = 'HOMAL1'then cte_3.sales_taf
    WHEN cte_4.actual_sales_nett * cte_3.percentage_gap IS NULL 
      OR cte_4.actual_sales_nett * cte_3.percentage_gap = 0
      THEN cte_4.actual_sales_nett
      ELSE cte_4.actual_sales_nett * cte_3.percentage_gap
    END AS actual_sales_nett_2
  from cte_4
  left join cte_3
  on cte_4.month = cte_3.month and cte_4.code = cte_3.RESTO
  ),

  cte_6 as(
    select
      NEW_CODE_STORE,
      OPEN_STORE,
      STORE,
      PROVINSI,
      KOTA,
      JAVA___OUTER_JAVA,
      AREA_HEAD
    from `mie-gacoan-418408.data_stores.master_data`
  )

select
  cte_5.code,
  cte_5.date_daily,
  EXTRACT(WEEK FROM cte_5.date_daily) AS week_number,
  cte_5.month,
  cte_5.target_sales_nett,
  CASE 
  WHEN cte_5.date_daily >= CURRENT_DATE() THEN null
  ELSE cte_5.target_sales_nett 
  END AS ytd_target_sales,
  cte_6.OPEN_STORE,
  EXTRACT(YEAR FROM cte_6.OPEN_STORE) AS year_open_store, 
  COALESCE(GREATEST(EXTRACT(YEAR FROM cte_6.OPEN_STORE), 2022), 2016) AS year_open_store2,
  CASE 
  WHEN cte_5.code = 'HOMAL1' THEN 'HO MALANG'
  ELSE cte_6.STORE
  END AS STORE,
  cte_6.PROVINSI,
  cte_6.KOTA,
  cte_6.JAVA___OUTER_JAVA,
  cte_6.AREA_HEAD,
  cte_5.actual_sales_nett,
  cte_5.actual_sales_nett_2
from cte_5
left join cte_6
on cte_5.code = cte_6.NEW_CODE_STORE
order by date_daily desc
;
"""

query2 = """
--combine smart and esb prodmix ------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.sales_data.data_combined_menu` AS
with cte_1 as (
select
  prodmix.Date,
  prodmix.Code,
  prodmix.product,
  prodmix.QTY,
  category.product_cleaned,
  category.category,
  category.product_cleaned_2
from `mie-gacoan-418408.sales_data.productmix` as prodmix
left join `mie-gacoan-418408.data_stores.prodmix_category_named` as category
on LOWER(REGEXP_REPLACE(TRIM(prodmix.Product), r'[\s]+', ' ')) 
   = LOWER(REGEXP_REPLACE(TRIM(category.product), r'[\s]+', ' '))
where Date <= '2025-05-31' 
),

cte_2 as(
select
  cte_1.Date,
  cte_1.Code,
  cte_1.product_cleaned,
  cte_1.category,
  cte_1.product_cleaned_2,
  sum(cte_1.QTY) as Qty
from cte_1
group by
  cte_1.Date,
  cte_1.Code,
  cte_1.product_cleaned,
  cte_1.category,
  cte_1.product_cleaned_2
),

cte_3 as (
select
  prodmix.Branch,
  prodmix.`Sales Date` as Date,
  prodmix.Qty,
  TRIM(prodmix.`Menu Name`) as menu,
  category.product_cleaned,
  category.category,
  category.product_cleaned_2
from `mie-gacoan-418408.sales_data.esb_menu_recapitulation_report` as prodmix
left join `mie-gacoan-418408.data_stores.prodmix_esb_category_named` as category
on LOWER(REGEXP_REPLACE(TRIM(prodmix.`Menu Name`), r'[\s]+', ' ')) 
   = LOWER(REGEXP_REPLACE(TRIM(category.product), r'[\s]+', ' '))
),


cte_4 as (
select
  cte_3.Date,
  master_data.NEW_CODE_STORE as Code,
  cte_3.product_cleaned,
  cte_3.category,
  cte_3.product_cleaned_2,
  sum(cte_3.QTY) as Qty
from cte_3
left join `mie-gacoan-418408.data_stores.master_data` AS master_data
on cte_3.Branch = master_data.CODE_GIS
group by
  cte_3.Date,
  Code,
  cte_3.product_cleaned,
  cte_3.category,
  cte_3.product_cleaned_2
)

select Date, Code, product_cleaned, category, product_cleaned_2, Qty
from cte_2
UNION ALL
select Date, Code, product_cleaned, category, product_cleaned_2, Qty
from cte_4
order by Date desc
;

-- prodmix1 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.prodmix1` AS
select 
    menu.date,
    menu.Code,
    menu.product_cleaned,
    menu.category,
    menu.product_cleaned_2,
    menu.Qty,
    master_data.OPEN_STORE,
    master_data.NAMA_OPS,
    master_data.AREA_HEAD,
    master_data.RM,
    master_data.CITY,
    master_data.AM,
    master_data.PROVINSI,
    master_data.KOTA,
    master_data.JAVA___OUTER_JAVA,
    master_data.TYPE_KITCHEN,
    master_data.STATUS
from  `mie-gacoan-418408.sales_data.data_combined_menu` as menu
inner join `mie-gacoan-418408.data_stores.master_data` AS master_data
ON menu.Code = master_data.NEW_CODE_STORE
;

-- prodmix1 per category -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `mie-gacoan-418408.dashboard_sales.prodmix2` AS
select 
    menu.date,
    menu.Code,
    menu.category,
    sum(menu.Qty) as Qty,
    master_data.OPEN_STORE,
    master_data.NAMA_OPS,
    master_data.AREA_HEAD,
    master_data.RM,
    master_data.CITY,
    master_data.AM,
    master_data.PROVINSI,
    master_data.KOTA,
    master_data.JAVA___OUTER_JAVA,
    master_data.TYPE_KITCHEN,
    master_data.STATUS
from  `mie-gacoan-418408.sales_data.data_combined_menu` as menu
inner join `mie-gacoan-418408.data_stores.master_data` AS master_data
ON menu.Code = master_data.NEW_CODE_STORE
group by 
    menu.date,
    menu.Code,
    menu.category,
    master_data.OPEN_STORE,
    master_data.NAMA_OPS,
    master_data.AREA_HEAD,
    master_data.RM,
    master_data.CITY,
    master_data.AM,
    master_data.PROVINSI,
    master_data.KOTA,
    master_data.JAVA___OUTER_JAVA,
    master_data.TYPE_KITCHEN,
    master_data.STATUS
;
"""

if st.button("Run Query 1 - Update Sales"):
    with st.spinner("Running Query 1..."):
        try:
            client.query(query1).result()
            st.success("✅ Query berhasil dijalankan!")
        except Exception as e:
            st.error(f"❌ Query gagal: {e}")

if st.button("Run Query 2 - Update Menu"):
    with st.spinner("Running Query 2..."):
        try:
            client.query(query2).result()
            st.success("✅ Query berhasil dijalankan!")
        except Exception as e:
            st.error(f"❌ Query gagal: {e}")




st.title("📤 Upload File ESB to BigQuery")

file_type = st.selectbox("Pilih jenis file", ["Sales", "Menu", "Service Time"])

uploaded_files = st.file_uploader(
    "Choose Excel file(s) (.xlsx)",
    type="xlsx",
    accept_multiple_files=True
)

if uploaded_files:
    df_list = []

    try:
        for uploaded_file in uploaded_files:
            filename = uploaded_file.name

            if file_type == "Sales":
                TABLE = 'esb_sales_recapitulation_report'
                df = pd.read_excel(uploaded_file, header=13, dtype=str)

                # Drop rows tanpa Bill Number
                df = df[df["Bill Number"].notna() & (df["Bill Number"].str.strip() != "")]

                numeric_columns = [
                    "Pax Total", "Subtotal", "Menu Discount", "Bill Discount", "Voucher Discount",
                    "Net Sales", "Service Charge Total", "Tax Total", "VAT Total", "Delivery Cost",
                    "Order Fee", "Platform Fee", "Voucher Sales Total", "Rounding Total", "Grand Total"
                ]

                for col in numeric_columns:
                    df[col] = df[col].astype(float)

                date_columns = ["Sales Date", "Sales In Date", "Sales Out Date"]
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

                time_columns = ["Sales In Time", "Sales Out Time"]
                for col in time_columns:
                    df[col] = pd.to_datetime(df[col], format="%H:%M:%S", errors="coerce").dt.time

            elif file_type == "Menu":
                TABLE = 'esb_menu_recapitulation_report'
                df = pd.read_excel(uploaded_file, header=12, dtype=str)
                
                date_columns = ["Sales Date"]
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

                numeric_columns = [
                    "Qty", "Subtotal", "Service Charge", "Tax Total", "VAT Total", "Total"]

                for col in numeric_columns:
                    df[col] = df[col].astype(float)

            elif file_type == "Service Time":
                TABLE = 'esb_menu_completion_summary_report'
                df = pd.read_excel(uploaded_file, header=10, dtype=str)

                # Convert Sales Date in ke datetime
                df["Sales Date In"] = pd.to_datetime(df["Sales Date In"], errors="coerce")

                # Convert kolom durasi
                duration_columns = ["Kitchen Process", "Checker Process", "Total Process"]
                for col in duration_columns:
                    df[col] = pd.to_timedelta(df[col], errors="coerce")
                    df[col] = df[col].apply(lambda x: str(x).split(" ")[-1] if pd.notnull(x) else None)

                numeric_columns = ["Kitchen Qty", "Checker Qty"]
                for col in numeric_columns:
                    df[col] = df[col].astype(float)

            else:
                st.error("❌ Jenis file tidak dikenali.")
                continue

            st.success(f"✅ File **{uploaded_file.name}** successfully processed!")
            st.write(df.head(5))
            df_list.append(df)

        combined_df = pd.concat(df_list, ignore_index=True)
        st.info(f"✅ All files combined! Shape: {combined_df.shape}")

        if combined_df.isnull().values.any():
            st.warning("⚠️ There are missing values in combined data.")
        else:
            st.info("✅ No missing values detected.")

        mode = st.radio("Upload mode", ["Append", "Overwrite"])

        if st.button("Upload to BigQuery"):
            table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

            if mode == "Append":
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            else:
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

            with st.spinner("Uploading to BigQuery..."):
                job = client.load_table_from_dataframe(combined_df, table_id, job_config=job_config)
                job.result()

            st.success(f"✅ Upload to BigQuery successful! Mode: {mode}")

    except Exception as e:
        st.error(f"❌ Failed to process file: {e}")
