# Databricks notebook source
import pyspark.sql as ps

# COMMAND ----------

def get_history_data(game: str) -> ps.DataFrame:
  JC_GAMES = ['Bingo Pop', 'Cookie Jam', 'Cookie Jam Blast', 'Emoji Blitz', \
                 'Genies and Gems', "Harry Potter", 'Mahjong', 'Panda Pop']
  MARKET_LIST = ['IT', 'GO']

  start_dt = '2020-01-01' if game in JC_GAMES else '2021-09-01'
  
  sql = f"""
  select 
     APPLICATION_FAMILY_NAME, 
     MARKET_CD, 
     case 
       when USER_SOURCE_TYPE_CD in ('MM', 'MK', 'MC') then 'Paid'
       else 'Non-Paid'
     end as SOURCE,
     CHANNEL_NAME,
     case
       when promotion_name like "%AEO_PURCHASE%" or promotion_name like "%APAYER%" then "AEO_Purchase"
       when promotion_name like "%MAI_OCPM%" or promotion_name like "%AOPM%" then "MAI_oCPM"
       when promotion_name like "%VO_REVENUE%" or promotion_name like "%AREVENUE%" then "VO_Revenue"
       when promotion_name like "%MINROAS%" or promotion_name like "%AROAS%" then "minROAS"
       when promotion_name like "%MAI_AEO%" then "MAI_AEO"
       when promotion_name like "%MULTIBID%" then "MultiBid or Unknown"
       when promotion_name like "%TCPA%" then "tCPA"
       when promotion_name like "%TROAS%" then "tROAS"
       when promotion_name like "%TCPI%" then "tCPI"
       else "NonSocial"
     end as CAMPAIGN_TYPE,
     case
       when country_iso_3 = 'USA' THEN 'US'
       when country_iso_3 in ('CAN', 'AUS', 'GBR', 'JPN') then 'T1'
       when country_iso_3 in ('KOR','FRA','DEU','NOR','DNK','CHE','SWE','AUT','FIN','HKG','IRL','ISR','ITA','NLD','NZL','SAU','SGP','ESP','TWN','ARE') then 'T2'
       else 'ROW'
     end as COUNTRY_TIER,
     date_sub(CALENDAR_DT, WEEKDAY(CALENDAR_DT)) as INSTALL_DT,
     sum(EXPENSE_AMT) as UA_COST,
     sum(USER_QTY) as INSTALL_NUM
  from pr_analytics_agg.fact_promotion_expense_daily
  where MVP_CAMPAIGN_TYPE = 'New Installs' and APPLICATION_FAMILY_NAME = '{game}' and MARKET_CD in {tuple(MARKET_LIST)} and CALENDAR_DT >= '{start_dt}'
  group by 1, 2, 3, 4, 5, 6, 7
  having sum(USER_QTY) > 0
  order by 1, 2, 3, 4, 5, 6, 7
  """
  df = spark.sql(sql)
  return df

# COMMAND ----------

df = get_history_data('Panda Pop')
display(df)

# COMMAND ----------

### Revise Query

# COMMAND ----------


