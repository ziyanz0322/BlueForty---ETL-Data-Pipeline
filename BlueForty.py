import snowflake.connector
import os
import glob
import re
import csv
import pathlib
import psycopg2
from datetime import datetime

conn = snowflake.connector.connect(
    user="Karthick0111", password="Rithanyashr333", account="qptljyh-umb01835"
)
cs = conn.cursor()
# SET UP
cs.execute("CREATE DATABASE IF NOT EXISTS PROCURE_DB")
cs.execute("USE DATABASE PROCURE_DB")
cs.execute("CREATE SCHEMA IF NOT EXISTS STAGE")
cs.execute("CREATE SCHEMA IF NOT EXISTS CORE")
cs.execute("USE SCHEMA STAGE")

# Question 1
# SET UP STAGE
cs.execute(r"""
CREATE OR REPLACE FILE FORMAT STAGE.FF_PURCHASES_CSV
  TYPE=CSV
  SKIP_HEADER=1
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  TRIM_SPACE=TRUE
  NULL_IF = ('\N','NULL','','N/A')
  EMPTY_FIELD_AS_NULL=TRUE
  DATE_FORMAT='YYYY-MM-DD'
  TIMESTAMP_FORMAT='AUTO'
""")
cs.execute(
    "CREATE STAGE IF NOT EXISTS STAGE.PURCHASES_INTSTAGE FILE_FORMAT=STAGE.FF_PURCHASES_CSV"
)
cs.execute("""
CREATE OR REPLACE TABLE CORE.PURCHASES (
  PurchaseOrderID              NUMBER(38,0),
  PurchaseOrderLineID          NUMBER(38,0),
  SupplierID                   NUMBER(38,0),
  StockItemID                  NUMBER(38,0),
  OrderedOuters                NUMBER(18,4),
  ReceivedOuters               NUMBER(18,4),
  ExpectedUnitPricePerOuter    NUMBER(18,4),
  OrderDate                    DATE,
  ExpectedDeliveryDate         DATE,
  LastReceiptDate              DATE,
  DeliveryMethodID             NUMBER(38,0),
  ContactPersonID              NUMBER(38,0),
  SupplierReference            STRING,
  IsOrderFinalized             BOOLEAN,
  IsOrderLineFinalized         BOOLEAN,
  Description                  STRING,
  SRC_FILENAME                 STRING,
  SRC_FILE_TS                  TIMESTAMP_NTZ
)
""")

# PUT
LOCAL_DIR = os.getenv("PURCHASES_DIR", "./ETL/Data/Monthly PO Data")
files = sorted(glob.glob(os.path.join(LOCAL_DIR, "*.csv")))

for fp in files:
    name = pathlib.Path(fp).name
    m = re.search(r"(20\d{2})[-_](\d{1,2})", name)
    if m:
        yyyy, mm = m.group(1), f"{int(m.group(2)):02d}"
        subpath = f"purchases/{yyyy}/{mm}/"
    else:
        subpath = "purchases/misc/00/"
    uri = "file://" + str(pathlib.Path(fp).resolve())
    cs.execute(
        f"PUT '{uri}' @STAGE.PURCHASES_INTSTAGE/{subpath} AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=8"
    )

# COPY INTO CORE
copy_sql = r"""
COPY INTO CORE.PURCHASES
(
  PurchaseOrderID, PurchaseOrderLineID, SupplierID, StockItemID,
  OrderedOuters, ReceivedOuters, ExpectedUnitPricePerOuter,
  OrderDate, ExpectedDeliveryDate, LastReceiptDate,
  DeliveryMethodID, ContactPersonID, SupplierReference,
  IsOrderFinalized, IsOrderLineFinalized, Description,
  SRC_FILENAME, SRC_FILE_TS
)
FROM (
  SELECT
    TRY_TO_NUMBER($1)                                   AS PurchaseOrderID,
    TRY_TO_NUMBER($13)                                  AS PurchaseOrderLineID,
    TRY_TO_NUMBER($2)                                   AS SupplierID,
    TRY_TO_NUMBER($14)                                  AS StockItemID,
    TRY_TO_NUMERIC($15,18,4)                            AS OrderedOuters,
    TRY_TO_NUMERIC($17,18,4)                            AS ReceivedOuters,
    TRY_TO_NUMERIC($19,18,4)                            AS ExpectedUnitPricePerOuter,
    TRY_TO_DATE($3,  'MM/DD/YYYY')                      AS OrderDate,
    TRY_TO_DATE($6,  'MM/DD/YYYY')                      AS ExpectedDeliveryDate,
    TRY_TO_DATE($20, 'MM/DD/YYYY')                      AS LastReceiptDate,
    TRY_TO_NUMBER($4)                                   AS DeliveryMethodID,
    TRY_TO_NUMBER($5)                                   AS ContactPersonID,
    NULLIF($7::STRING, '')                              AS SupplierReference,
    ($8::INT = 1)                                       AS IsOrderFinalized,
    ($21::INT = 1)                                      AS IsOrderLineFinalized,
    TRIM($16)                                           AS Description,
    METADATA$FILENAME                                   AS SRC_FILENAME,
    METADATA$FILE_LAST_MODIFIED                         AS SRC_FILE_TS
  FROM @STAGE.PURCHASES_INTSTAGE
       ( FILE_FORMAT => 'STAGE.FF_PURCHASES_CSV',
         PATTERN     => '.*purchases/.*\.(csv|csv\.gz)$' )
)
FILE_FORMAT = (FORMAT_NAME = 'STAGE.FF_PURCHASES_CSV')
ON_ERROR = 'CONTINUE'
PURGE = TRUE
"""
cs.execute(copy_sql)

# Question 2 -->set a new table CORE.PURCHASE_ORDER_TOTALS
cs.execute("""
CREATE OR REPLACE VIEW CORE.PURCHASE_ORDER_TOTALS AS
SELECT PurchaseOrderID, ORDERDATE, SUPPLIERID,
  ROUND(SUM(COALESCE(ReceivedOuters,0) * COALESCE(ExpectedUnitPricePerOuter,0)), 2) AS POAmount
FROM CORE.PURCHASES
GROUP BY PurchaseOrderID, ORDERDATE, SUPPLIERID
ORDER BY PurchaseOrderID, ORDERDATE
""")

# Question 3
# SET UP
cs.execute("CREATE OR REPLACE FILE FORMAT STAGE.FF_XML TYPE = XML")
cs.execute("""
CREATE STAGE IF NOT EXISTS STAGE.INVOICES
  FILE_FORMAT = STAGE.FF_XML
""")

cs.execute("""
CREATE OR REPLACE TABLE CORE.SUPPLIER_INVOICES (
  SupplierTransactionID   NUMBER(38,0),
  SupplierID              NUMBER(38,0),
  PurchaseOrderID         NUMBER(38,0),
  SupplierInvoiceNumber   STRING,
  TransactionDate         DATE,
  AmountExcludingTax      NUMBER(18,2),
  TaxAmount               NUMBER(18,2),
  TransactionAmount       NUMBER(18,2),
  OutstandingBalance      NUMBER(18,2),
  FinalizationDate        DATE,
  IsFinalized             BOOLEAN,
  XML_INDEX               NUMBER
)
""")

# PUT
cs.execute(
    "PUT 'file:///home/jovyan/Downloads/rsm-ict/ETL/Data/Supplier Transactions XML.xml' @STAGE.INVOICES OVERWRITE=TRUE"
)

# COPY INTO
cs.execute("""
CREATE OR REPLACE TABLE CORE.SUPPLIER_INVOICES_XML_RAW (
  DOC           VARIANT,
  SRC_FILENAME  STRING
)
""")

cs.execute("""
COPY INTO CORE.SUPPLIER_INVOICES_XML_RAW
FROM (
  SELECT
    $1,
    METADATA$FILENAME
  FROM @STAGE.INVOICES (FILE_FORMAT => STAGE.FF_XML)
)
FILE_FORMAT = (FORMAT_NAME = 'STAGE.FF_XML')
ON_ERROR    = 'ABORT_STATEMENT'
""")

# SHRED DATA
cs.execute("""
INSERT INTO CORE.SUPPLIER_INVOICES (
  SupplierTransactionID, SupplierID, PurchaseOrderID, SupplierInvoiceNumber,
  TransactionDate, AmountExcludingTax, TaxAmount, TransactionAmount,
  OutstandingBalance, FinalizationDate, IsFinalized, XML_INDEX
)
SELECT
  XMLGET(t.value, 'SupplierTransactionID'):"$"::NUMBER,
  XMLGET(t.value, 'SupplierID'):"$"::NUMBER,
  NULLIF(XMLGET(t.value, 'PurchaseOrderID'):"$"::VARCHAR,'')::NUMBER,
  NULLIF(XMLGET(t.value, 'SupplierInvoiceNumber'):"$"::VARCHAR,''),
  TRY_TO_DATE(XMLGET(t.value, 'TransactionDate'):"$"::STRING),
  XMLGET(t.value, 'AmountExcludingTax'):"$"::NUMBER(18,2),
  XMLGET(t.value, 'TaxAmount'):"$"::NUMBER(18,2),
  XMLGET(t.value, 'TransactionAmount'):"$"::NUMBER(18,2),
  XMLGET(t.value, 'OutstandingBalance'):"$"::NUMBER(18,2),
  TRY_TO_DATE(XMLGET(t.value, 'FinalizationDate'):"$"::STRING),
  (XMLGET(t.value, 'IsFinalized'):"$"::INT = 1),
  t.index
FROM CORE.SUPPLIER_INVOICES_XML_RAW r,
     LATERAL FLATTEN(input => r.DOC:"$") t
WHERE XMLGET(t.value, 'SupplierTransactionID'):"$" IS NOT NULL
""")

# Question 4
# JOIN TWO TABLE
cs.execute("USE SCHEMA CORE")
cs.execute("""
WITH Invoice_Amount AS (
    SELECT PurchaseOrderID, SUPPLIERID, SUM(AmountExcludingTax) AS InvoiceExTaxTotal
    FROM CORE.SUPPLIER_INVOICES
    GROUP BY PurchaseOrderID, SUPPLIERID
)
SELECT A.PurchaseOrderID, A.SUPPLIERID, InvoiceExTaxTotal, POAmount
FROM Invoice_Amount AS A
JOIN CORE.PURCHASE_ORDER_TOTALS AS B
USING(PurchaseOrderID)
ORDER BY A.PurchaseOrderID, A.SUPPLIERID
""")

# Question 5
cs.execute("""
CREATE OR REPLACE VIEW CORE.PURCHASE_ORDERS_AND_INVOICES AS
WITH Invoice_Amount AS (
    SELECT PurchaseOrderID, SUPPLIERID, SUM(AmountExcludingTax) AS InvoiceExTaxTotal
    FROM CORE.SUPPLIER_INVOICES
    GROUP BY PurchaseOrderID, SUPPLIERID
)
SELECT B.*, InvoiceExTaxTotal,
        InvoiceExTaxTotal - POAmount AS invoiced_vs_quoted
FROM Invoice_Amount AS A
JOIN CORE.PURCHASE_ORDER_TOTALS AS B
USING(PurchaseOrderID)
ORDER BY PurchaseOrderID
""")
# Resulted in a view with 2065 records


# Question 6
# Simple data type
def _is_int(s):
    try:
        int(s)
        return True
    except:
        return False


def _is_float(s):
    try:
        float(s)
        return True
    except:
        return False


def _is_date(s):
    date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]
    for fmt in date_formats:
        try:
            datetime.strptime(s, fmt)
            return True
        except:
            pass
    return False


def infer_snowflake_type(values):
    clean_values = [v for v in values if v not in (None, "", "NULL", "\\N")]
    if not clean_values:
        return "STRING"
    if all(_is_date(v) for v in clean_values):
        return "DATE"
    elif all(_is_int(v) for v in clean_values):
        return "INTEGER"
    elif all(_is_float(v) for v in clean_values):
        return "FLOAT"
    else:
        return "STRING"


def create_table_from_csv(csv_path, table_name):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        sample_data = [[] for _ in headers]
        for i, row in enumerate(reader):
            if i >= 100:
                break
            for j, value in enumerate(row[: len(headers)]):
                sample_data[j].append(value)
    column_defs = []
    for header, values in zip(headers, sample_data):
        data_type = infer_snowflake_type(values)
        column_defs.append(f'"{header}" {data_type}')

    columns_str = ", ".join(column_defs)
    create_sql = f"CREATE OR REPLACE TABLE {table_name} ({columns_str})"
    return create_sql, headers


# Configuration
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "8765"))
PG_DB = os.getenv("PG_DB", "WestCoastImporters")
PG_USER = os.getenv("PG_USER", "jovyan")
PG_PASS = os.getenv("PG_PASS", "postgres")
PG_TABLE = os.getenv("PG_SUPPLIER_TABLE", "supplier_case")
PG_CONFIG = dict(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
)


def extract_and_load_supplier_data(cs):
    # Step 1: Extract from PostgreSQL to CSV
    csv_file_path = "/home/jovyan/Downloads/rsm-ict/ETL/Data/supplier_case.csv"
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
    # Connect to PostgreSQL and export data
    pg_conn = psycopg2.connect(**PG_CONFIG)
    with pg_conn, pg_conn.cursor() as cur:
        with open(csv_file_path, "w", encoding="utf-8") as f:
            cur.copy_expert("COPY supplier_case TO STDOUT WITH CSV HEADER", f)
    pg_conn.close()

    # Step 2: Create Snowflake table structure
    target_table = "CORE.SUPPLIER_CASE"
    create_sql, headers = create_table_from_csv(csv_file_path, target_table)
    # print("Generated table structure:")
    # print(create_sql)
    cs.execute(create_sql)

    # Step 3: Create file format and stage
    cs.execute("""
    CREATE OR REPLACE FILE FORMAT STAGE.FF_SUPPLIER_CSV
      TYPE = CSV
      SKIP_HEADER = 1
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      NULL_IF = ('NULL','','N/A')
      EMPTY_FIELD_AS_NULL = TRUE
    """)

    cs.execute(
        "CREATE OR REPLACE STAGE STAGE.SUPPLIER_DATA FILE_FORMAT = STAGE.FF_SUPPLIER_CSV"
    )

    # Step 4: Upload file to stage
    file_uri = f"file://{pathlib.Path(csv_file_path).resolve()}"
    cs.execute(f"PUT '{file_uri}' @STAGE.SUPPLIER_DATA OVERWRITE=TRUE")

    # Step 5: Load data into table
    column_list = ", ".join([f"${i + 1}" for i in range(len(headers))])

    copy_sql_supplier = f"""
    COPY INTO {target_table}
    FROM (
      SELECT {column_list}
      FROM @STAGE.SUPPLIER_DATA
    )
    FILE_FORMAT = (FORMAT_NAME = 'STAGE.FF_SUPPLIER_CSV')
    ON_ERROR = 'CONTINUE'
    """
    cs.execute(copy_sql_supplier)


extract_and_load_supplier_data(cs)
cs.execute("""
CREATE OR REPLACE VIEW CORE.SUPPLIER_ZIP5 AS
SELECT
  REGEXP_REPLACE(
    LPAD(
      COALESCE(
        CAST("postalpostalcode" AS STRING),
        CAST("deliverypostalcode" AS STRING),
        ''
      ),
      5, '0'
    ),
    '[^0-9]', ''
  ) AS ZIP5,
  "supplierid", "suppliername"
FROM CORE.SUPPLIER_CASE
WHERE COALESCE(
        CAST("postalpostalcode" AS STRING),
        CAST("deliverypostalcode" AS STRING)
      ) IS NOT NULL
  AND COALESCE(
        CAST("postalpostalcode" AS STRING),
        CAST("deliverypostalcode" AS STRING)
      ) != ''
""")

# Question 7
# Create a new stage or replace an existing one
cs.execute("""
    CREATE OR REPLACE STAGE my_stage
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = '\t');
""")

# Upload the text file to the stage
cs.execute("""
    PUT file:///home/jovyan/Downloads/rsm-ict/ETL/Data/2021_Gaz_zcta_national.txt @my_stage auto_compress=true;
""")

# Create a table for the zip code to geo-location mapping
cs.execute("""
    CREATE OR REPLACE TABLE zipcode_geolocation (
        zip_code VARCHAR(10),
        latitude FLOAT,
        longitude FLOAT
    );
""")

# Load data from the staged file into the 'zipcode_geolocation' table
cs.execute("""
    COPY INTO zipcode_geolocation (zip_code, latitude, longitude)
    FROM (
        SELECT
            TRY_CAST(t.$1 AS VARCHAR(10)) AS zip_code,  -- Cast first field as ZIP Code
            TRY_CAST(t.$3 AS FLOAT) AS latitude,        -- Cast third field as latitude
            TRY_CAST(t.$4 AS FLOAT) AS longitude        -- Cast fourth field as longitude
        FROM @my_stage/2021_Gaz_zcta_national.txt.gz t
    )
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' FIELD_DELIMITER = '\t' SKIP_HEADER = 1);
""")

# Closest weather station data computation
cs.execute("""
          CREATE OR REPLACE TABLE CORE.CLOSEST_STATIONS AS
          WITH distinct_zip AS(
            SELECT DISTINCT sc."postalpostalcode" AS zip_code,
                zg.latitude AS lat,
                zg.longitude AS lon
            FROM CORE.SUPPLIER_CASE AS sc JOIN ZIPCODE_GEOLOCATION AS zg
            on zg.zip_code = sc."postalpostalcode"
            WHERE sc."postalpostalcode" is not null
           ),distance_calculations AS (
            SELECT
                z.zip_code,
                s.NOAA_WEATHER_STATION_ID AS station_id,
                2 * 6371 * ASIN(SQRT(POWER(SIN((RADIANS(s.LATITUDE - z.lat)) / 2), 2) +
                  COS(RADIANS(z.lat)) * COS(RADIANS(s.LATITUDE)) * POWER(SIN((RADIANS(s.LONGITUDE - z.lon)) / 2), 2)))
                AS dist_km
            FROM distinct_zip AS z
            JOIN WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_STATION_INDEX AS s
          )
            SELECT zip_code, station_id
            FROM (select *, row_number() OVER (partition by zip_code order by dist_km) AS rn
                      FROM distance_calculations)
            WHERE rn = 1;
      """)
# Query resulted in 8 records.

# Maximum temperature for each day for all suppliers
cs.execute("""
CREATE OR REPLACE TABLE CORE.SUPPLIER_ZIP_CODE_WEATHER AS
SELECT
  A.zip_code,
  B.DATE::date as date,
  B.VALUE as high_temperature
FROM CORE.CLOSEST_STATIONS AS A
JOIN WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_METRICS_TIMESERIES AS B
ON B.NOAA_WEATHER_STATION_ID = A.station_id
WHERE B.VARIABLE_NAME = 'Maximum Temperature'
ORDER BY A.zip_code, date;
""")
# Query resulted in 28.6k records.

# Question 8
cs.execute("""
CREATE OR REPLACE TABLE CORE.PURCHASES_WITH_WEATHER AS
SELECT A.*, B."postalpostalcode" AS ZIP, C.high_temperature
FROM CORE.PURCHASE_ORDERS_AND_INVOICES AS A
JOIN CORE.SUPPLIER_CASE AS B
ON A.SupplierID = B."supplierid"
JOIN CORE.SUPPLIER_ZIP_CODE_WEATHER AS C
ON C.ZIP_CODE = B."postalpostalcode" AND C.DATE = A.ORDERDATE;
""")
# Query resulted in 1.7k records.
