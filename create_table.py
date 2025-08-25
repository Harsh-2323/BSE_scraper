import psycopg2

DB_NAME = "bse_scraper"
DB_USER = "postgres"
DB_PASSWORD = "root123"
DB_HOST = "localhost"
DB_PORT = "5432"

schema_sql = """
CREATE TABLE IF NOT EXISTS issues (
    id SERIAL PRIMARY KEY,
    security_name VARCHAR(512) NOT NULL,
    exchange_platform VARCHAR(128),
    type_of_issue VARCHAR(64),
    type_of_issue_long VARCHAR(128),
    issue_status VARCHAR(128),
    security_type VARCHAR(64),
    start_date DATE,
    end_date DATE,
    offer_price_raw VARCHAR(64),
    price_min FLOAT,
    price_max FLOAT,
    face_value VARCHAR(64),
    list_url TEXT,
    detail_url TEXT,
    payload JSONB,
    CONSTRAINT uq_issue_identity UNIQUE (security_name, start_date, end_date, detail_url)
);

CREATE INDEX IF NOT EXISTS ix_issue_dates ON issues (start_date, end_date);
"""

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(schema_sql)
    cur.close()
    conn.close()
    print("✅ Table 'issues' created successfully in", DB_NAME)

if __name__ == "__main__":
    main()
