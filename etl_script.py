from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ETL_Pipeline_NestedSQL").enableHiveSupport().getOrCreate()

stg_db = "staging_db"
core_db = "core_db"
final_db = "final_db"
load_date = datetime.now().strftime("%Y-%m-%d")

print(f"Starting ETL for {load_date}")

# Step 1: Base dataset with customer + account + transaction
df = spark.sql(f"""
    SELECT DISTINCT
        c.customer_id,
        c.name,
        a.account_id,
        a.account_type,
        a.status,
        t.txn_id,
        t.amount,
        t.txn_type,
        t.txn_date,
        coalesce(c.segment, 'UNKNOWN') as segment,
        '{load_date}' as load_date
    FROM {stg_db}.customer c
    LEFT JOIN {stg_db}.account a
        ON c.customer_id = a.customer_id
        AND a.load_date = '{load_date}'
    LEFT JOIN (
        SELECT account_id, txn_id, amount, txn_type, txn_date, merchant_id
        FROM {stg_db}.transaction
        WHERE load_date = '{load_date}'
          AND txn_date >= date_sub(current_date(), 30)
    ) t
        ON a.account_id = t.account_id
""")
df.createOrReplaceTempView("base_layer")

# Step 2: Union with archived sources
df_union = spark.sql(f"""
    SELECT * FROM base_layer
    UNION
    SELECT 
        c.customer_id,
        c.name,
        a.account_id,
        a.account_type,
        a.status,
        tx.txn_id,
        tx.amount,
        tx.txn_type,
        tx.txn_date,
        coalesce(c.segment, 'UNKNOWN') as segment,
        '{load_date}' as load_date,
        tx.merchant_id
    FROM {core_db}.archived_customer c
    INNER JOIN {core_db}.archived_account a
        ON c.customer_id = a.customer_id
    LEFT JOIN {core_db}.archived_transaction tx
        ON a.account_id = tx.account_id
        AND tx.txn_date > '2023-01-01'
""")
df_union.createOrReplaceTempView("union_layer")

# Step 3: Nested join logic (business rules layered in)
df_nested = spark.sql(f"""
    SELECT 
        u.customer_id,
        u.account_id,
        u.account_type,
        SUM(CASE WHEN u.txn_type = 'DEBIT' THEN u.amount ELSE 0 END) as total_debit,
        SUM(CASE WHEN u.txn_type = 'CREDIT' THEN u.amount ELSE 0 END) as total_credit,
        COUNT(DISTINCT u.txn_id) as txn_count,
        seg.segment_name,
        coalesce(r.region, 'UNKNOWN') as region,
        MAX(u.txn_date) as last_txn_date,
        prod.product_name,
        prod.product_category,
        br.branch_name,
        br.branch_region,
        merch.merchant_category,
        f.fraud_flag,
        cb.external_credit_score,
        s.sanctions_hit
    FROM union_layer u
    LEFT JOIN (
        SELECT DISTINCT customer_id, segment_name
        FROM {core_db}.segmentation_rules
    ) seg
        ON u.customer_id = seg.customer_id
    LEFT JOIN (
        SELECT cust_id, region
        FROM {core_db}.customer_region
        WHERE active_flag = 'Y'
    ) r
        ON u.customer_id = r.cust_id
    LEFT JOIN (
        SELECT account_id, product_name, product_category
        FROM {core_db}.product_dim
    ) prod
        ON u.account_id = prod.account_id
    LEFT JOIN (
        SELECT account_id, branch_name, branch_region
        FROM {core_db}.branch_dim
    ) br
        ON u.account_id = br.account_id
    LEFT JOIN (
        SELECT merchant_id, merchant_category
        FROM {core_db}.merchant_dim
    ) merch
        ON u.merchant_id = merch.merchant_id
    LEFT JOIN (
        SELECT DISTINCT txn_id, fraud_flag
        FROM {core_db}.fraud_flags
        WHERE active_flag = 'Y'
    ) f
        ON u.txn_id = f.txn_id
    LEFT JOIN (
        SELECT customer_id, external_credit_score
        FROM {core_db}.credit_bureau
        WHERE report_date = '{load_date}'
    ) cb
        ON u.customer_id = cb.customer_id
    LEFT JOIN (
        SELECT customer_id, CASE WHEN blacklisted = 'Y' THEN 1 ELSE 0 END AS sanctions_hit
        FROM {core_db}.sanctions_list
    ) s
        ON u.customer_id = s.customer_id
    GROUP BY u.customer_id, u.account_id, u.account_type,
             seg.segment_name, r.region,
             prod.product_name, prod.product_category,
             br.branch_name, br.branch_region,
             merch.merchant_category,
             f.fraud_flag, cb.external_credit_score, s.sanctions_hit
""")
df_nested.createOrReplaceTempView("nested_layer")

# Step 4: Final aggregation and join with reference data
df_final = spark.sql(f"""
    SELECT 
        n.customer_id,
        COUNT(DISTINCT n.account_id) as total_accounts,
        SUM(n.total_debit) as total_debit,
        SUM(n.total_credit) as total_credit,
        MAX(n.last_txn_date) as last_txn_date,
        n.segment_name,
        n.region,
        n.product_name,
        n.product_category,
        n.branch_name,
        n.branch_region,
        n.merchant_category,
        risk.risk_score,
        MAX(n.external_credit_score) as credit_score,
        SUM(CASE WHEN n.fraud_flag = 'Y' THEN 1 ELSE 0 END) as fraud_txn_count,
        MAX(n.sanctions_hit) as sanctions_match
    FROM nested_layer n
    LEFT JOIN (
        SELECT customer_id, AVG(score) as risk_score
        FROM {core_db}.risk_assessment
        WHERE as_of_date = '{load_date}'
        GROUP BY customer_id
    ) risk
        ON n.customer_id = risk.customer_id
    GROUP BY n.customer_id, n.segment_name, n.region, n.product_name, 
             n.product_category, n.branch_name, n.branch_region, 
             n.merchant_category, risk.risk_score
""")

# Step 5: Write final output
df_final.write.mode("overwrite").saveAsTable(f"{final_db}.customer_summary")

print("ETL completed successfully")
