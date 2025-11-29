# Glue Job: Load ONLY FACT fact_supplychain_events (all dims already loaded)
# - Reads from Glue Catalog 'rawdata' (LIMIT 10,000 rows for quick validation)
# - Looks up surrogate keys by reading Redshift dims
# - Inserts only non-identity columns
# - Writes in HASH buckets (by order_id) so rows appear progressively

import sys, re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, concat_ws, to_date, date_format, lit, hash as f_hash, pmod, upper, trim
)

# --- CONFIG ---
CONN_NAME     = "Redshift connection"
DEFAULT_DB    = "dev"
SCHEMA        = "public"
GLUE_DB_NAME  = "logistream"
FACT_BUCKETS  = 20        # increase if you want smaller batches (e.g., 50/100)
CUSTOMER_BUCKETS = 50

# --- INIT ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
jvm = sc._jvm

# --- HELPERS ---
def _normalize_jdbc_url(raw_url: str, default_db: str) -> str:
    m = re.match(r"^jdbc:redshift://([^/]+)(?:/([^?;]*))?(.*)$", raw_url)
    if not m:
        raise ValueError(f"Unrecognized Redshift JDBC URL: {raw_url}")
    hostport, db_in_url, tail = m.group(1), (m.group(2) or "").strip(), (m.group(3) or "")
    if ":" in hostport:
        host, port = hostport.split(":", 1); port = int(port)
    else:
        host, port = hostport, 5439
    db = db_in_url if db_in_url else default_db
    jdbc_url = f"jdbc:redshift://{host}:{port}/{db}{tail}"
    jdbc_url += f"{'&' if '?' in jdbc_url else '?'}ssl=true&loginTimeout=15&socketTimeout=60&tcpKeepAlive=true"
    return jdbc_url

def exec_sql(jvm, jdbc_url: str, user: str, pwd: str, sql: str):
    conn = None
    try:
        conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, pwd)
        conn.setAutoCommit(False)
        stmt = conn.createStatement(); stmt.execute(sql); stmt.close()
        conn.commit()
    except Exception:
        try:
            if conn: conn.rollback()
        finally:
            raise
    finally:
        if conn: conn.close()

def read_rs(select_sql: str) -> DataFrame:
    return (spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"({select_sql}) t")
            .option("driver", "com.amazon.redshift.jdbc42.Driver")
            .option("user", user)
            .option("password", pwd)
            .load())

def write_to_redshift(df: DataFrame, table_name: str, mode: str = "append"):
    cnt = df.count()
    if cnt == 0:
        print(f"[WARN] {table_name}: empty DF, skipping."); return
    (df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{SCHEMA}.{table_name}")
        .option("driver", "com.amazon.redshift.jdbc42.Driver")
        .option("user", user)
        .option("password", pwd)
        .option("batchsize", 10000)
        .mode(mode)
        .save())
    print(f"[OK] {table_name}: wrote {cnt} rows.")

# --- MAIN ---
def main():
    # JDBC info
    conf = glueContext.extract_jdbc_conf(CONN_NAME)
    global jdbc_url, user, pwd
    jdbc_url = _normalize_jdbc_url(conf["url"], DEFAULT_DB)
    user     = conf.get("user")
    pwd      = conf.get("password")
    if not user or not pwd:
        raise RuntimeError(f"Glue Connection '{CONN_NAME}' missing user/password.")
    
    # SOURCE
    print("[INFO] Loading rawdata from Glue Catalog (LIMIT 10,000 rows)")
    df_raw_full = glueContext.create_dynamic_frame.from_catalog(
        database=GLUE_DB_NAME,
        table_name="rawdata"
    ).toDF()
    df_raw = df_raw_full.limit(500).cache()
    src_cnt = df_raw.count()
    print(f"[INFO] Using df_raw limited to {src_cnt} rows")
        
    #  ---------- dim_department ----------
    df_dept = df_raw.select(
        col("department_id").alias("dept_id"),
        col("department_name").alias("dept_name")
    ).dropDuplicates(["dept_id"])
    exec_sql(jvm, jdbc_url, user, pwd, f'TRUNCATE TABLE "{SCHEMA}"."dim_department";')
    write_to_redshift(df_dept.select("dept_id","dept_name"), "dim_department")

    # ---------- dim_category ----------
    dept_map = read_rs(f'SELECT dept_id, dept_key FROM "{SCHEMA}"."dim_department"')
    df_cat = (df_raw.select(
                col("category_id").alias("cat_id"),
                col("category_name").alias("cat_name"),
                col("department_id").alias("dept_id"))
              .dropDuplicates(["cat_id"])
              .join(dept_map, ["dept_id"], "left"))
    exec_sql(jvm, jdbc_url, user, pwd, f'TRUNCATE TABLE "{SCHEMA}"."dim_category";')
    write_to_redshift(df_cat.select("cat_id","cat_name","dept_key"), "dim_category")

    # ---------- dim_product ----------
    cat_map = read_rs(f'SELECT cat_id, cat_key FROM "{SCHEMA}"."dim_category"')
    df_prod = (df_raw.select(
                    col("product_card_id"),
                    col("product_name"),
                    col("order_item_product_price").alias("product_price"),
                    col("category_id").alias("cat_id"))
                .dropDuplicates(["product_card_id"])
                .join(cat_map, ["cat_id"], "left"))
    exec_sql(jvm, jdbc_url, user, pwd, f'TRUNCATE TABLE "{SCHEMA}"."dim_product";')
    write_to_redshift(df_prod.select("product_card_id","product_name","product_price","cat_key"),
                      "dim_product")

    # ---------- dim_customer (BATCHED) ----------
    df_customer_src = df_raw.select(
        col("customer_id"),
        col("customer_fname").alias("first_name"),
        col("customer_lname").alias("last_name"),
        col("customer_segment").alias("segment"),
        col("customer_country").alias("country")
    ).dropDuplicates(["customer_id"])
    exec_sql(jvm, jdbc_url, user, pwd, f'TRUNCATE TABLE "{SCHEMA}"."dim_customer";')
    print(f"[INFO] Writing dim_customer in {CUSTOMER_BUCKETS} buckets...")
    for b in range(CUSTOMER_BUCKETS):
        df_chunk = df_customer_src.where(
            pmod(f_hash(col("customer_id")), lit(CUSTOMER_BUCKETS)) == lit(b)
        )
        if df_chunk.count() == 0:
            if b % 10 == 0:
                print(f"[INFO] dim_customer bucket {b}: 0 rows")
            continue
        write_to_redshift(
            df_chunk.select("customer_id","first_name","last_name","segment","country"),
            "dim_customer",
            mode="append"
        )

    #---------- dim_execution_status ----------
    #Redshift table has identity status_key; we only insert non-identity columns.
    df_status = (df_raw
                 .select(
                     col("shipping_mode"),
                     col("delivery_status"),
                     col("order_status")
                 )
                 .dropDuplicates())
    exec_sql(jvm, jdbc_url, user, pwd, f'TRUNCATE TABLE "{SCHEMA}"."dim_execution_status";')
    write_to_redshift(df_status.select("shipping_mode","delivery_status","order_status"),
                      "dim_execution_status",
                      mode="append")

    print("[DONE] Loaded dim_execution_status (others skipped as requested).")
    
   # ---------- dim_date ----------
    #Build a slim date dimension from order & shipping timestamps
    df_dates_order = (df_raw
        .select(to_date(col("order_date_dateorders"), "M/d/yyyy H:m").alias("date_actual"))
        .where(col("date_actual").isNotNull()))
    df_dates_shipping = (df_raw
        .select(to_date(col("shipping_date_dateorders"), "M/d/yyyy H:m").alias("date_actual"))
        .where(col("date_actual").isNotNull()))

    df_dates = (df_dates_order.union(df_dates_shipping).distinct()
        .select(
            col("date_actual"),
            date_format(col("date_actual"), "yyyyMMdd").cast("int").alias("date_key")
        )
        .dropDuplicates(["date_key"]))

    # Soft-overwrite: TRUNCATE then append
    exec_sql(jvm, jdbc_url, user, pwd, f'TRUNCATE TABLE "{SCHEMA}"."dim_date";')
    write_to_redshift(df_dates.select("date_key","date_actual"), "dim_date", mode="append")

    print("[DONE] Loaded dim_date (execution_status and prior dims skipped).")
    
    # ---------- dim_route_shapes ----------
    print("[INFO] Loading processed_routes from Glue Catalog (LIMIT 10,000 rows)")
    df_routes_full = glueContext.create_dynamic_frame.from_catalog(
        database=GLUE_DB_NAME,
        table_name="processed_routes"
    ).toDF()
    df_routes = df_routes_full.limit(10000).cache()
    from pyspark.sql.functions import concat_ws
    df_route_shapes = (df_routes
        .select("origin_lat","origin_long","dest_lat","dest_long","shape_wkt")
        .withColumn("route_key_composite",
                    concat_ws("_", col("origin_lat"), col("origin_long"),
                                  col("dest_lat"),   col("dest_long")))
        .dropDuplicates(["route_key_composite"])
        .select("origin_lat","origin_long","dest_lat","dest_long","shape_wkt"))
    exec_sql(jvm, jdbc_url, user, pwd, f'TRUNCATE TABLE "{SCHEMA}"."dim_route_shapes";')
    write_to_redshift(df_route_shapes, "dim_route_shapes", mode="append")

    # ---------- dim_geography ----------
    #Build geography rows from order-side location (no identity column in insert)
    df_geo = (df_raw
        .select(
            col("order_city").alias("city"),
            col("order_state").alias("state"),
            col("order_country").alias("country"),
            col("order_region").alias("region"),
            col("market"),
            col("latitude_src").alias("latitude"),
            col("longitude_src").alias("longitude")
        )
        .dropDuplicates(["city","state","country"])
    )

    # Soft-overwrite target and insert (let Redshift identity generate geo_key)
    exec_sql(jvm, jdbc_url, user, pwd, f'TRUNCATE TABLE "{SCHEMA}"."dim_geography";')
    write_to_redshift(
        df_geo.select("city","state","country","region","market","latitude","longitude"),
        "dim_geography",
        mode="append"
    )

    print("[DONE] Loaded dim_geography (previous dim_route_shapes is commented).")

    # === DIM LOOKUPS FROM REDSHIFT ===
    print("[INFO] Reading dimension maps from Redshift")

    # product: product_card_id -> product_key
    prod_map = read_rs(f'SELECT product_key, product_card_id FROM "{SCHEMA}"."dim_product"').dropDuplicates(["product_card_id"])

    # customer: customer_id -> customer_key
    cust_map = read_rs(f'SELECT customer_key, customer_id FROM "{SCHEMA}"."dim_customer"').dropDuplicates(["customer_id"])

    # execution status: (shipping_mode, delivery_status, order_status) -> status_key
    stat_map = read_rs(
        f'''SELECT status_key,
                   shipping_mode,
                   delivery_status,
                   order_status
            FROM "{SCHEMA}"."dim_execution_status"'''
    ).select(
        col("status_key"),
        upper(trim(col("shipping_mode"))).alias("sm"),
        upper(trim(col("delivery_status"))).alias("ds"),
        upper(trim(col("order_status"))).alias("os")
    ).dropDuplicates(["sm","ds","os"])

    # route shapes: composite(origin/dest) -> route_shape_key
    routes_map = read_rs(
        f'''SELECT route_shape_key,
                   origin_lat, origin_long, dest_lat, dest_long
            FROM "{SCHEMA}"."dim_route_shapes"'''
    ).withColumn(
        "route_key_composite",
        concat_ws("_",
                  col("origin_lat"), col("origin_long"),
                  col("dest_lat"),   col("dest_long"))
    ).select("route_shape_key","route_key_composite").dropDuplicates(["route_key_composite"])

    # geography (order): city/state/country -> geo_key
    geo_map = read_rs(
        f'''SELECT geo_key, city, state, country
            FROM "{SCHEMA}"."dim_geography"'''
    ).select(
        col("geo_key").alias("order_geo_key"),
        upper(trim(col("city"))).alias("g_city"),
        upper(trim(col("state"))).alias("g_state"),
        upper(trim(col("country"))).alias("g_country"),
    ).dropDuplicates(["g_city","g_state","g_country"])

    # === PREPARE FACT SOURCE ===
    print("[INFO] Preparing fact source with derived keys")
    # Build composite, date keys, and normalized status fields for join
    df_src = (df_raw
        .withColumn(
            "route_key_composite",
            concat_ws("_",
                col("latitude_src"), col("longitude_src"),
                col("latitude_dest"), col("longitude_dest"))
        )
        .withColumn("order_date_key",
            date_format(to_date(col("order_date_dateorders"), "M/d/yyyy H:m"), "yyyyMMdd").cast("int"))
        .withColumn("shipping_date_key",
            date_format(to_date(col("shipping_date_dateorders"), "M/d/yyyy H:m"), "yyyyMMdd").cast("int"))
        .withColumn("sm", upper(trim(col("shipping_mode"))))
        .withColumn("ds", upper(trim(col("delivery_status"))))
        .withColumn("os", upper(trim(col("order_status"))))
        .withColumn("g_city",    upper(trim(col("order_city"))))
        .withColumn("g_state",   upper(trim(col("order_state"))))
        .withColumn("g_country", upper(trim(col("order_country"))))
    )

    # === JOIN TO DIM MAPS ===
    df_fact = (df_src
        # product
        .join(prod_map.select("product_key","product_card_id"),
              on="product_card_id", how="left")
        # customer (raw has order_customer_id)
        .join(cust_map.select("customer_key","customer_id"),
              df_src["order_customer_id"] == cust_map["customer_id"], how="left")
        # route shape
        .join(routes_map, on="route_key_composite", how="left")
        # execution status
        .join(stat_map, on=["sm","ds","os"], how="left")
        # geography (order location)
        .join(geo_map, on=["g_city","g_state","g_country"], how="left")
    )

    # === SELECT FINAL FACT COLUMNS ===
    df_final = df_fact.select(
        col("order_id"),
        col("order_item_id"),
        col("sales"),
        col("order_item_quantity").alias("quantity"),
        col("order_profit_per_order").alias("profit"),
        col("order_item_discount_rate").alias("discount_rate"),
        col("days_for_shipping_real").alias("days_real"),
        col("days_for_shipment_scheduled").alias("days_scheduled"),
        col("late_delivery_risk").alias("late_risk"),
        # FKs
        col("product_key"),
        col("customer_key"),
        col("status_key"),
        col("route_shape_key"),
        col("order_geo_key"),
        col("order_date_key"),
        col("shipping_date_key")
    )

    # === WRITE IN BUCKETS (by order_id) ===
    print(f"[INFO] Writing fact_supplychain_events in {FACT_BUCKETS} buckets (by order_id hash)")
    for b in range(FACT_BUCKETS):
        df_chunk = df_final.where(
            pmod(f_hash(col("order_id")), lit(FACT_BUCKETS)) == lit(b)
        )
        cnt = df_chunk.count()
        if cnt == 0:
            if b % 5 == 0:
                print(f"[INFO] fact bucket {b}: 0 rows")
            continue
        print(f"[INFO] fact bucket {b}/{FACT_BUCKETS-1}: {cnt} rows")
        write_to_redshift(df_chunk, "fact_supplychain_events", mode="append")

    print("[DONE] Loaded fact_supplychain_events (dims assumed pre-loaded).")

if __name__ == "__main__":
    main()