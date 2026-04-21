"""
Seed demo cho DWBase: DIM + FACT + cube (agg), doc lap voi IDBase (require_idbase=False).
"""
import random
from datetime import date

import pymssql

from dw_builder import ensure_dw_cube_tables, ensure_dw_schema

CITIES = [
    ("HN", "Ha Noi", "Mien Bac"),
    ("HCM", "TP.HCM", "Mien Nam"),
    ("DN", "Da Nang", "Mien Trung"),
    ("HP", "Hai Phong", "Mien Bac"),
    ("CT", "Can Tho", "Mien Nam"),
]

CUSTOMER_TYPES = ["buu_dien", "du_lich", "ca_hai"]


def _conn(server: str, user: str, password: str, port: int, db: str) -> pymssql.Connection:
    return pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database=db,
        autocommit=True,
    )


def clear_dw_seed_tables(
    cursor: pymssql.Cursor,
) -> None:
    """Xoa du lieu cu (fact -> cube -> dim) de chay seed sach."""
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.agg_sales_city_month', N'U') IS NOT NULL TRUNCATE TABLE dbo.agg_sales_city_month;
        IF OBJECT_ID(N'dbo.agg_sales_product_month', N'U') IS NOT NULL TRUNCATE TABLE dbo.agg_sales_product_month;
        IF OBJECT_ID(N'dbo.agg_inventory_city_product', N'U') IS NOT NULL TRUNCATE TABLE dbo.agg_inventory_city_product;
        IF OBJECT_ID(N'dbo.agg_inventory_store_product', N'U') IS NOT NULL TRUNCATE TABLE dbo.agg_inventory_store_product;
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.fact_don_hang', N'U') IS NOT NULL DELETE FROM dbo.fact_don_hang;
        IF OBJECT_ID(N'dbo.fact_kho_hang', N'U') IS NOT NULL DELETE FROM dbo.fact_kho_hang;
        IF OBJECT_ID(N'dbo.dim_cua_hang', N'U') IS NOT NULL DELETE FROM dbo.dim_cua_hang;
        IF OBJECT_ID(N'dbo.dim_khach_hang', N'U') IS NOT NULL DELETE FROM dbo.dim_khach_hang;
        IF OBJECT_ID(N'dbo.dim_san_pham', N'U') IS NOT NULL DELETE FROM dbo.dim_san_pham;
        IF OBJECT_ID(N'dbo.dim_thanh_pho', N'U') IS NOT NULL DELETE FROM dbo.dim_thanh_pho;
        IF OBJECT_ID(N'dbo.dim_thoi_gian', N'U') IS NOT NULL DELETE FROM dbo.dim_thoi_gian;
        """
    )


def seed_dim_thoi_gian(cursor: pymssql.Cursor) -> None:
    """Full calendar 2022-01-01 .. 2024-12-31, giong logic dim hien co."""
    cursor.execute(
        """
        DECLARE @d DATE = '20220101';
        WHILE @d <= '20241231'
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM dbo.dim_thoi_gian WHERE full_date = @d)
            INSERT INTO dbo.dim_thoi_gian (
                date_key, full_date, day, month, quarter, year, day_of_week, is_weekend
            )
            VALUES (
                CAST(CONVERT(VARCHAR(8), @d, 112) AS INT),
                @d,
                DATEPART(DAY, @d),
                DATEPART(MONTH, @d),
                DATEPART(QUARTER, @d),
                DATEPART(YEAR, @d),
                DATEPART(WEEKDAY, @d),
                CASE WHEN DATEPART(WEEKDAY, @d) IN (1, 7) THEN 1 ELSE 0 END
            );
            SET @d = DATEADD(DAY, 1, @d);
        END
        """
    )


def seed_city_store(
    cursor: pymssql.Cursor,
    rng: random.Random,
) -> None:
    for code, name, region in CITIES:
        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dbo.dim_thanh_pho WHERE ma_thanh_pho = %s)
            INSERT INTO dbo.dim_thanh_pho (ma_thanh_pho, ten_thanh_pho, bang, dia_chi_vp)
            VALUES (%s, %s, %s, %s)
            """,
            (code, code, name, region, f"VP {name}"),
        )
    cursor.execute("SELECT city_key FROM dbo.dim_thanh_pho")
    city_keys = [row[0] for row in cursor.fetchall()]
    if not city_keys:
        raise RuntimeError("Khong co city_key trong dim_thanh_pho.")

    for i in range(1, 21):
        city_key = rng.choice(city_keys)
        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dbo.dim_cua_hang WHERE ma_cua_hang = %s)
            INSERT INTO dbo.dim_cua_hang (ma_cua_hang, so_dien_thoai, city_key)
            VALUES (%s, %s, %s)
            """,
            (f"STORE{i}", f"STORE{i}", f"090{i:07}", city_key),
        )


def seed_product(cursor: pymssql.Cursor, rng: random.Random) -> None:
    for i in range(1, 51):
        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dbo.dim_san_pham WHERE ma_mat_hang = %s)
            INSERT INTO dbo.dim_san_pham (ma_mat_hang, mo_ta, kich_co, trong_luong, gia_niem_yet)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                f"P{i}",
                f"P{i}",
                f"San pham {i}",
                rng.choice(["S", "M", "L"]),
                round(rng.uniform(0.5, 5.0), 2),
                rng.randint(100, 1000),
            ),
        )


def seed_customer(cursor: pymssql.Cursor, rng: random.Random) -> None:
    first = date(2022, 1, 1)
    cursor.execute("SELECT city_key FROM dbo.dim_thanh_pho")
    city_keys = [row[0] for row in cursor.fetchall()]
    if not city_keys:
        raise RuntimeError("Khong co city_key trong dim_thanh_pho.")

    for i in range(1, 1001):
        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dbo.dim_khach_hang WHERE ma_khach_hang = %s)
            INSERT INTO dbo.dim_khach_hang (ma_khach_hang, ten_khach_hang, city_key, ngay_dat_hang_dau_tien, customer_type)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                f"KH{i}",
                f"KH{i}",
                f"Khach {i}",
                rng.choice(city_keys),
                first,
                rng.choice(CUSTOMER_TYPES),
            ),
        )


def seed_fact_order(
    cursor: pymssql.Cursor,
    rng: random.Random,
    n: int = 100_000,
    batch: int = 5000,
) -> None:
    cursor.execute("SELECT date_key FROM dbo.dim_thoi_gian")
    date_keys = [r[0] for r in cursor.fetchall()]
    if not date_keys:
        raise RuntimeError("dim_thoi_gian rong.")
    cursor.execute("SELECT customer_key FROM dbo.dim_khach_hang")
    customer_keys = [r[0] for r in cursor.fetchall()]
    if not customer_keys:
        raise RuntimeError("dim_khach_hang rong.")
    cursor.execute("SELECT product_key FROM dbo.dim_san_pham")
    product_keys = [r[0] for r in cursor.fetchall()]
    if not product_keys:
        raise RuntimeError("dim_san_pham rong.")
    cursor.execute("SELECT store_key FROM dbo.dim_cua_hang")
    store_keys = [r[0] for r in cursor.fetchall()]
    if not store_keys:
        raise RuntimeError("dim_cua_hang rong.")

    insert_sql = """
        INSERT INTO dbo.fact_don_hang
        (date_key, customer_key, product_key, store_key, ma_don_hang, so_luong_dat, gia_dat)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    buf = []
    special_order_lines = []
    for i in range(n):
        order_code = f"ORDER{i}"
        date_key = rng.choice(date_keys)
        customer_key = rng.choice(customer_keys)
        base_product_key = rng.choice(product_keys)
        store_key = rng.choice(store_keys)
        buf.append(
            (
                date_key,
                customer_key,
                base_product_key,
                store_key,
                order_code,
                rng.randint(1, 10),
                rng.randint(100, 1000),
            )
        )
        if i <= 20 and len(product_keys) > 1:
            alt_candidates = [pk for pk in product_keys if pk != base_product_key]
            alt_product_key = rng.choice(alt_candidates)
            special_order_lines.append(
                (
                    date_key,
                    customer_key,
                    alt_product_key,
                    store_key,
                    order_code,
                    rng.randint(1, 10),
                    rng.randint(100, 1000),
                )
            )
        if len(buf) >= batch:
            cursor.executemany(insert_sql, buf)
            buf = []
            if (i + 1) % (batch * 4) == 0:
                print(f"fact_don_hang: inserted ~{i + 1}")
    if buf:
        cursor.executemany(insert_sql, buf)
    if special_order_lines:
        cursor.executemany(insert_sql, special_order_lines)
        print(f"fact_don_hang: added {len(special_order_lines)} extra lines for ORDER0..ORDER20.")
    print(f"fact_don_hang: hoan tat {n} dong.")


def seed_inventory(cursor: pymssql.Cursor) -> None:
    cursor.execute(
        """
        INSERT INTO dbo.fact_kho_hang (date_key, store_key, product_key, so_luong_ton)
        SELECT
            20240101,
            s.store_key,
            p.product_key,
            ABS(CHECKSUM(NEWID())) % 501
        FROM dbo.dim_cua_hang s
        CROSS JOIN dbo.dim_san_pham p
        """
    )


def build_cubes_from_facts(cursor: pymssql.Cursor) -> None:
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.agg_sales_city_month', N'U') IS NOT NULL TRUNCATE TABLE dbo.agg_sales_city_month;
        IF OBJECT_ID(N'dbo.agg_sales_product_month', N'U') IS NOT NULL TRUNCATE TABLE dbo.agg_sales_product_month;
        IF OBJECT_ID(N'dbo.agg_inventory_city_product', N'U') IS NOT NULL TRUNCATE TABLE dbo.agg_inventory_city_product;
        IF OBJECT_ID(N'dbo.agg_inventory_store_product', N'U') IS NOT NULL TRUNCATE TABLE dbo.agg_inventory_store_product;
        """
    )
    cursor.execute(
        """
        INSERT INTO dbo.agg_sales_city_month (city_key, year, month, total_quantity, total_sales)
        SELECT
            c.city_key,
            t.year,
            t.month,
            SUM(f.so_luong_dat),
            SUM(f.tong_tien)
        FROM dbo.fact_don_hang f
        JOIN dbo.dim_thoi_gian t ON f.date_key = t.date_key
        JOIN dbo.dim_cua_hang s ON f.store_key = s.store_key
        JOIN dbo.dim_thanh_pho c ON s.city_key = c.city_key
        GROUP BY c.city_key, t.year, t.month
        """
    )
    cursor.execute(
        """
        INSERT INTO dbo.agg_sales_product_month (product_key, year, month, total_quantity, total_sales)
        SELECT
            f.product_key,
            t.year,
            t.month,
            SUM(f.so_luong_dat),
            SUM(f.tong_tien)
        FROM dbo.fact_don_hang f
        JOIN dbo.dim_thoi_gian t ON f.date_key = t.date_key
        GROUP BY f.product_key, t.year, t.month
        """
    )
    cursor.execute(
        """
        INSERT INTO dbo.agg_inventory_city_product (city_key, product_key, total_inventory)
        SELECT
            c.city_key,
            f.product_key,
            SUM(f.so_luong_ton)
        FROM dbo.fact_kho_hang f
        JOIN dbo.dim_cua_hang s ON f.store_key = s.store_key
        JOIN dbo.dim_thanh_pho c ON s.city_key = c.city_key
        GROUP BY c.city_key, f.product_key
        """
    )
    cursor.execute(
        """
        INSERT INTO dbo.agg_inventory_store_product (store_key, product_key, total_inventory)
        SELECT
            f.store_key,
            f.product_key,
            SUM(f.so_luong_ton)
        FROM dbo.fact_kho_hang f
        GROUP BY f.store_key, f.product_key
        """
    )


def run_all_dw_demo_seed(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    target_db: str = "DWBase",
    fact_order_rows: int = 100_000,
    random_seed: int = 42,
) -> None:
    """
    Tao schema DW + cube, xoa du lieu cu, seed dim/fact, rebuild cube agg.
    Khong can database IDBase.
    """
    ensure_dw_schema(
        server=server,
        user=user,
        password=password,
        port=port,
        require_idbase=False,
        target_db=target_db,
    )
    ensure_dw_cube_tables(
        server=server,
        user=user,
        password=password,
        port=port,
        require_idbase=False,
        target_db=target_db,
    )

    rng = random.Random(random_seed)
    conn = _conn(server, user, password, port, target_db)
    cursor = conn.cursor()

    clear_dw_seed_tables(cursor)

    seed_dim_thoi_gian(cursor)
    print("Da seed dim_thoi_gian.")

    seed_city_store(cursor, rng)
    print("Da seed dim_thanh_pho + dim_cua_hang.")

    seed_product(cursor, rng)
    print("Da seed dim_san_pham.")

    seed_customer(cursor, rng)
    print("Da seed dim_khach_hang.")

    print(f"Dang insert fact_don_hang ({fact_order_rows} dong)...")
    seed_fact_order(cursor, rng, n=fact_order_rows)

    seed_inventory(cursor)
    print("Da seed fact_kho_hang.")

    build_cubes_from_facts(cursor)
    print("Da build cac bang cube (agg).")

    conn.close()
