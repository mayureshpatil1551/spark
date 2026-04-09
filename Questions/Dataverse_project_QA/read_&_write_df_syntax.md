    # 🚀 End-to-End Data Engineering Pipeline (Interview Ready)

    ---

    ## 🎤 Interview Summary

    **"We ingest data from multiple sources (DB, files, APIs), convert them into Spark DataFrames, store them in Iceberg tables, apply transformations (TL → ATL → Harmonization), and finally push data to downstream systems."**

    ---

    # 🔽 Ingestion Layer (Create DataFrames)

    ## 1️⃣ Oracle Ingestion (JDBC)
    ```python
    jdbc_url = "jdbc:oracle:thin:@host:port/service"

    df_oracle = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "schema.table_name") \
        .option("user", "username") \
        .option("password", "password") \
        .option("fetchsize", 10000) \
        .option("partitionColumn", "id") \
        .option("lowerBound", 1) \
        .option("upperBound", 1000000) \
        .option("numPartitions", 8) \
        .load()
    ```

    ---

    ## 2️⃣ CSV File Ingestion
    ```python
    df_csv = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("delimiter", ",") \
        .option("mode", "PERMISSIVE") \
        .load("/path/file.csv")
    ```

    ---

    ## 3️⃣ Excel File Ingestion
    ```python
    df_excel = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("dataAddress", "'Sheet1'!A1") \
        .load("/path/file.xlsx")
    ```

    ---

    ## 4️⃣ XML File Ingestion
    ```python
    df_xml = spark.read \
        .format("xml") \
        .option("rowTag", "record") \
        .option("inferSchema", True) \
        .load("/path/file.xml")
    ```

    ---

    ## 5️⃣ Salesforce API Ingestion (OAuth)
    ```python
    import requests

    # Step 1: Get Access Token
    payload = {
        "grant_type": "password",
        "client_id": "client_id",
        "client_secret": "client_secret",
        "username": "username",
        "password": "password"
    }

    auth_url = "https://login.salesforce.com/services/oauth2/token"
    res = requests.post(auth_url, data=payload)
    access_token = res.json()["access_token"]
    instance_url = res.json()["instance_url"]

    # Step 2: Query Data
    query_url = f"{instance_url}/services/data/vXX.X/query?q=SELECT+Id,Name+FROM+Account"
    headers = {"Authorization": f"Bearer {access_token}"}

    resp = requests.get(query_url, headers=headers)
    data = resp.json()

    df_salesforce = spark.createDataFrame(data["records"])
    ```

    ---

    ## 6️⃣ Smartsheet API Ingestion
    ```python
    import requests

    url = "https://api.smartsheet.com/2.0/sheets/{sheet_id}"
    headers = {"Authorization": "Bearer token"}

    resp = requests.get(url, headers=headers)
    data = resp.json()

    df_smartsheet = spark.createDataFrame(data["rows"])
    ```

    ---

    # 🧊 Load into Iceberg Table
    ```python
    df.write \
    .format("iceberg") \
    .mode("append") \
    .save("catalog.db.table_name")
    ```

    ---

    # 🔄 Transformation Layers

    ## 🔹 TL Layer (Full Load + Incremental Merge)

    ### Full Load
    ```python
    df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .save("catalog.db.table")
    ```

    ### Incremental Load (MERGE)
    ```python
    spark.sql("""
    MERGE INTO catalog.db.table t
    USING staging_table s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    ```

    ---

    ## 🔹 ATL Layer (Advanced Transformations)
    ```python
    df_atl = df_orders \
        .join(df_products, "product_id") \
        .groupBy("product_id") \
        .agg({"amount": "sum"})
    ```

    ---

    ## 🔹 Harmonization Layer
    ```python
    from pyspark.sql import functions as F

    df_harm = df_atl \
        .withColumnRenamed("cust_id", "customer_id") \
        .withColumn("country", F.lit("IND")) \
        .withColumn("standard_date", F.to_date("date", "yyyy-MM-dd"))
    ```

    ---

    # 📤 Outbound Layer

    ## 1️⃣ CSV Output
    ```python
    df.write \
    .format("csv") \
    .option("header", True) \
    .mode("overwrite") \
    .save("/output/path")
    ```

    ---

    ## 2️⃣ Oracle Output
    ```python
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "schema.output_table") \
        .option("user", "username") \
        .option("password", "password") \
        .mode("append") \
        .save()
    ```

    ---

    ## 3️⃣ Salesforce REST API Output
    ```python
    for row in df.toLocalIterator():
        requests.post(query_url, headers=headers, json=row.asDict())
    ```

    ---

    ## 4️⃣ Smartsheet API Output
    ```python
    for row in df.toLocalIterator():
        requests.post(url, headers=headers, json=row.asDict())
    ```

    ---

    # ❓ Interview Questions

    ### Q1: How do you handle incremental loads?
    **Answer:** Using MERGE INTO (upsert) in Iceberg based on primary key.

    ---

    ### Q2: How do you ingest API data securely?
    **Answer:** Using OAuth/token-based authentication and handling pagination.

    ---

    ### Q3: Why Iceberg?
    **Answer:** ACID, schema evolution, partition evolution.

    ---

    # 🎯 Interview Tip

    👉 Always explain flow:
    **Source → Ingestion → Iceberg → TL (Full + Incremental) → ATL → Harmonization → Output**

    ---

