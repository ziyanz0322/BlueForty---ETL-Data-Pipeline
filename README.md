# BlueForty---ETL-Data-Pipeline

A complete ETL pipeline that loads purchase orders, invoices, and geographic data from PostgreSQL to Snowflake, with data transformation and reconciliation analysis.

## Features

- **Purchase Order Processing** - Load PO data from CSV files to Snowflake
- **Invoice Processing** - Parse and load supplier invoices from XML files
- **Geographic Mapping** - Associate suppliers with locations by zip code
- **Weather Integration** - Join historical weather data using Cybersyn data source
- **Data Transformation** - Create views for PO-to-invoice reconciliation

## Core Tables & Views

1. **CORE.PURCHASES** - Purchase order base table
2. **CORE.SUPPLIER_INVOICES** - Supplier invoice records
3. **CORE.PURCHASE_ORDER_TOTALS** - PO aggregation view
4. **CORE.PURCHASE_ORDERS_AND_INVOICES** - PO-to-invoice reconciliation view
5. **CORE.PURCHASES_WITH_WEATHER** - Orders enriched with weather data

## Environment Variables

```
PURCHASES_DIR - Path to purchase data folder (default: ./ETL/Data/Monthly PO Data)
PG_HOST - PostgreSQL host (default: 127.0.0.1)
PG_PORT - PostgreSQL port (default: 8765)
PG_DB - PostgreSQL database (default: WestCoastImporters)
PG_USER - PostgreSQL user (default: jovyan)
PG_PASS - PostgreSQL password (default: postgres)
```

## Dependencies

```
snowflake-connector-python
psycopg2
```

## Usage

```bash
python BlueForty.py
```

## Tech Stack

- **Data Warehouse** - Snowflake
- **Source Database** - PostgreSQL
- **Language** - Python
- **Data Formats** - CSV, XML

## License

MIT
