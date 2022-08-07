import duckdb 
con = duckdb.connect()
con.execute("PRAGMA enable_object_cache")
df =con.execute('''
PRAGMA enable_profiling='json';

PRAGMA profile_output='./0-Code/malloy.json';
WITH __stage0 AS (
  SELECT
    group_set,
    MAX((CASE WHEN group_set=0 THEN
      COUNT( 1)
      END)) OVER () as "_count_all__1",
    CASE WHEN group_set IN (2,3,4) THEN
      region_0."R_NAME"
      END as "R_NAME__2",
    CASE WHEN group_set IN (3,4) THEN
      nation_0."N_NAME"
      END as "N_NAME__3",
    CASE WHEN group_set=4 THEN
      lineitem."L_RETURNFLAG"
      END as "L_RETURNFLAG__4",
    MAX((CASE WHEN group_set=3 THEN
      COUNT( 1)
      END)) OVER (PARTITION BY CASE WHEN group_set IN (3,4) THEN
      nation_0."N_NAME"
      END, CASE WHEN group_set IN (2,3,4) THEN
      region_0."R_NAME"
      END) as "_count_all__4",
    CASE WHEN group_set=4 THEN
      COUNT( 1)
      END as "_count__4",
    (CASE WHEN group_set=4 THEN
      COUNT( 1)
      END)*1.0/(MAX((CASE WHEN group_set=3 THEN
      COUNT( 1)
      END)) OVER (PARTITION BY CASE WHEN group_set IN (3,4) THEN
      nation_0."N_NAME"
      END, CASE WHEN group_set IN (2,3,4) THEN
      region_0."R_NAME"
      END)) as "_Ratio__4"
  FROM '/Users/mimoune.djouallah/Desktop/TPC-H-SF10/Parquet/lineitem/*.parquet' as lineitem
  LEFT JOIN '/Users/mimoune.djouallah/Desktop/TPC-H-SF10/Parquet/orders.parquet' AS orders_0
    ON orders_0."O_ORDERKEY"=lineitem."L_ORDERKEY"
  LEFT JOIN '/Users/mimoune.djouallah/Desktop/TPC-H-SF10/Parquet/customer.parquet' AS customer_0
    ON customer_0."C_CUSTKEY"=orders_0."O_CUSTKEY"
  LEFT JOIN '/Users/mimoune.djouallah/Desktop/TPC-H-SF10/Parquet/nation.parquet' AS nation_0
    ON nation_0."N_NATIONKEY"=customer_0."C_NATIONKEY"
  LEFT JOIN '/Users/mimoune.djouallah/Desktop/TPC-H-SF10/Parquet/region.parquet' AS region_0
    ON region_0."R_REGIONKEY"=nation_0."N_REGIONKEY"
  CROSS JOIN (SELECT UNNEST(GENERATE_SERIES(0,4,1)) as group_set  ) as group_set
  GROUP BY 1,3,4,5
)
, __stage1 AS (
  SELECT 
    CASE WHEN group_set=4 THEN 3  ELSE group_set END as group_set,
    FIRST("_count_all__1") FILTER (WHERE "_count_all__1" IS NOT NULL) as "_count_all__1",
    CASE WHEN group_set IN (2,3,4) THEN
      "R_NAME__2"
      END as "R_NAME__2",
    CASE WHEN group_set IN (3,4) THEN
      "N_NAME__3"
      END as "N_NAME__3",
    COALESCE(LIST({
      "L_RETURNFLAG": "L_RETURNFLAG__4", 
      "_count_all": "_count_all__4", 
      "_count": "_count__4", 
      "_Ratio": "_Ratio__4"}  ORDER BY  "_count_all__4" desc NULLS LAST) FILTER (WHERE group_set=4),[]) as "by_flag__3"
  FROM __stage0
  WHERE group_set NOT IN (0)
  GROUP BY 1,3,4
)
, __stage2 AS (
  SELECT 
    CASE WHEN group_set=3 THEN 2  ELSE group_set END as group_set,
    FIRST("_count_all__1") FILTER (WHERE "_count_all__1" IS NOT NULL) as "_count_all__1",
    CASE WHEN group_set IN (2,3,4) THEN
      "R_NAME__2"
      END as "R_NAME__2",
    COALESCE(LIST({
      "N_NAME": "N_NAME__3", 
      "by_flag": "by_flag__3"}  ORDER BY  "N_NAME__3" asc NULLS LAST) FILTER (WHERE group_set=3),[]) as "yyy__2"
  FROM __stage1
  GROUP BY 1,3
)
SELECT
  MAX(CASE WHEN group_set=1 THEN _count_all__1 END) as "_count_all",
  COALESCE(LIST({
    "R_NAME": "R_NAME__2", 
    "yyy": "yyy__2"}  ORDER BY  "R_NAME__2" asc NULLS LAST) FILTER (WHERE group_set=2),[]) as "xxx"
FROM __stage2
ORDER BY 1 desc NULLS LAST
    
''').fetch_df()
print(df)