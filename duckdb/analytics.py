import duckdb
import pandas as pd

# Cấu hình hiển thị cho console (nếu chạy trực tiếp)
pd.set_option('display.max_rows', 20)
pd.set_option('display.width', 1000)

# ĐƯỜNG DẪN DATA
DATALAKE_PATH = "s3://warehouse/processed_trades/**/*.parquet"


def get_connection():
    """Thiết lập kết nối DuckDB với MinIO"""
    try:
        con = duckdb.connect(database=':memory:')
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL aws; LOAD aws;")

        # Cấu hình Credential MinIO
        con.execute("""
            SET s3_region='us-east-1';
            SET s3_url_style='path';
            SET s3_endpoint='minio:9000';
            SET s3_access_key_id='minioadmin';
            SET s3_secret_access_key='minioadmin';
            SET s3_use_ssl=false;
        """)
        return con
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return None


def fetch_symbol_list(con):
    """Lấy danh sách các mã có trong Datalake"""
    if not con: return []
    try:
        query = f"SELECT DISTINCT symbol FROM read_parquet('{DATALAKE_PATH}')"
        df = con.execute(query).df()
        return sorted(df['symbol'].tolist())
    except:
        return []


def fetch_market_data(con, symbol, limit=200):
    """Lấy dữ liệu lịch sử giá/volume của một mã cụ thể"""
    if not con: return pd.DataFrame()
    try:
        # Query lấy dữ liệu chi tiết cho Chart & Tape
        query = f"""
        SELECT event_time, price, volume 
        FROM read_parquet('{DATALAKE_PATH}')
        WHERE symbol = '{symbol}'
        ORDER BY event_time DESC
        LIMIT {limit}
        """
        df = con.execute(query).df()

        if not df.empty:
            df['event_time'] = pd.to_datetime(df['event_time'])
            df = df.sort_values('event_time')
            # Ép kiểu để tránh lỗi tính toán
            df['price'] = pd.to_numeric(df['price'])
            df['volume'] = pd.to_numeric(df['volume'])

        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()


def fetch_leaderboard(con):
    """(Optional) Logic cũ của analytics.py: Lấy Top 15 Volume"""
    if not con: return pd.DataFrame()
    try:
        query = f"""
        SELECT 
            replace(symbol, 'BINANCE:', '') as Ticker,
            CASE WHEN symbol LIKE 'BINANCE:%' THEN 'CRYPTO' ELSE 'STOCK' END as Type,
            COUNT(*) as Tx_Count,
            AVG(price) as Avg_Price,
            SUM(volume) as Total_Volume,
            MAX(event_time) as Last_Update
        FROM read_parquet('{DATALAKE_PATH}')
        WHERE try_cast(event_time as TIMESTAMP) >= (
            SELECT MAX(try_cast(event_time as TIMESTAMP)) - INTERVAL 30 MINUTE 
            FROM read_parquet('{DATALAKE_PATH}')
        )
        GROUP BY 1, 2
        ORDER BY Total_Volume DESC
        LIMIT 15
        """
        return con.execute(query).df()
    except:
        return pd.DataFrame()


if __name__ == "__main__":
    import time

    print(">>> DuckDB Analytics Service Started...")
    con = get_connection()
    while True:
        df = fetch_leaderboard(con)
        print("\n" + "=" * 60)
        print(f"MARKET LEADERBOARD AT {time.strftime('%H:%M:%S')}")
        print("=" * 60)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No data found...")
        time.sleep(5)