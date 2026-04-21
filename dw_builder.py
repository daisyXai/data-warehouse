import pymssql


def _ensure_idbase_exists(master_cursor, source_db: str) -> None:
    master_cursor.execute(
        f"""
        IF DB_ID(N'{source_db}') IS NULL
            THROW 50001, 'Khong tim thay IDBase. Hay merge IER truoc.', 1;
        """
    )


def ensure_dw_schema(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    source_db: str = "IDBase",
    target_db: str = "DWBase",
    require_idbase: bool = True,
) -> None:
    """
    Tao DB DW va cac bang DIM/FACT neu chua co (khong xoa du lieu cu).
    Neu require_idbase=False thi khong yeu cau IDBase (dung cho seed demo DW doc lap).
    """
    master_conn = pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database="master",
        autocommit=True,
    )
    master_cursor = master_conn.cursor()
    if require_idbase:
        _ensure_idbase_exists(master_cursor, source_db)
    master_cursor.execute(
        f"""
        IF DB_ID(N'{target_db}') IS NULL
            CREATE DATABASE [{target_db}]
        """
    )
    master_conn.close()

    conn = pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database=target_db,
        autocommit=True,
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.dim_thoi_gian', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.dim_thoi_gian (
                date_key INT NOT NULL PRIMARY KEY,
                full_date DATE NOT NULL UNIQUE,
                day INT NOT NULL,
                month INT NOT NULL,
                quarter INT NOT NULL,
                year INT NOT NULL,
                day_of_week INT NOT NULL,
                is_weekend BIT NOT NULL
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.dim_thanh_pho', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.dim_thanh_pho (
                city_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                ma_thanh_pho VARCHAR(20) NOT NULL UNIQUE,
                ten_thanh_pho NVARCHAR(200) NOT NULL,
                bang NVARCHAR(100) NULL,
                dia_chi_vp NVARCHAR(300) NOT NULL
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.dim_cua_hang', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.dim_cua_hang (
                store_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                ma_cua_hang VARCHAR(20) NOT NULL UNIQUE,
                so_dien_thoai VARCHAR(30) NULL,
                city_key INT NOT NULL,
                CONSTRAINT fk_dim_cua_hang_dim_thanh_pho
                    FOREIGN KEY (city_key) REFERENCES dbo.dim_thanh_pho(city_key)
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.dim_san_pham', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.dim_san_pham (
                product_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                ma_mat_hang VARCHAR(20) NOT NULL UNIQUE,
                mo_ta NVARCHAR(300) NOT NULL,
                kich_co NVARCHAR(50) NULL,
                trong_luong DECIMAL(10,2) NULL,
                gia_niem_yet DECIMAL(18,2) NOT NULL
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.dim_khach_hang', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.dim_khach_hang (
                customer_key INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                ma_khach_hang VARCHAR(20) NOT NULL UNIQUE,
                ten_khach_hang NVARCHAR(200) NOT NULL,
                city_key INT NOT NULL,
                ngay_dat_hang_dau_tien DATE NOT NULL,
                customer_type VARCHAR(20) NOT NULL
                    CHECK (customer_type IN ('buu_dien', 'du_lich', 'ca_hai', 'thuong')),
                CONSTRAINT fk_dim_khach_hang_dim_thanh_pho
                    FOREIGN KEY (city_key) REFERENCES dbo.dim_thanh_pho(city_key)
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.fact_don_hang', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.fact_don_hang (
                fact_order_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                date_key INT NOT NULL,
                customer_key INT NOT NULL,
                product_key INT NOT NULL,
                store_key INT NULL,
                ma_don_hang VARCHAR(20) NOT NULL,
                so_luong_dat INT NOT NULL,
                gia_dat DECIMAL(18,2) NOT NULL,
                tong_tien AS (so_luong_dat * gia_dat) PERSISTED,
                CONSTRAINT fk_fact_don_hang_dim_thoi_gian
                    FOREIGN KEY (date_key) REFERENCES dbo.dim_thoi_gian(date_key),
                CONSTRAINT fk_fact_don_hang_dim_khach_hang
                    FOREIGN KEY (customer_key) REFERENCES dbo.dim_khach_hang(customer_key),
                CONSTRAINT fk_fact_don_hang_dim_san_pham
                    FOREIGN KEY (product_key) REFERENCES dbo.dim_san_pham(product_key),
                CONSTRAINT fk_fact_don_hang_dim_cua_hang
                    FOREIGN KEY (store_key) REFERENCES dbo.dim_cua_hang(store_key)
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.fact_kho_hang', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.fact_kho_hang (
                fact_inventory_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                date_key INT NOT NULL,
                store_key INT NOT NULL,
                product_key INT NOT NULL,
                so_luong_ton INT NOT NULL,
                CONSTRAINT fk_fact_kho_hang_dim_thoi_gian
                    FOREIGN KEY (date_key) REFERENCES dbo.dim_thoi_gian(date_key),
                CONSTRAINT fk_fact_kho_hang_dim_cua_hang
                    FOREIGN KEY (store_key) REFERENCES dbo.dim_cua_hang(store_key),
                CONSTRAINT fk_fact_kho_hang_dim_san_pham
                    FOREIGN KEY (product_key) REFERENCES dbo.dim_san_pham(product_key)
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.dim_cua_hang', N'U') IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM sys.indexes
               WHERE name = N'ix_dim_cua_hang_city_store'
                 AND object_id = OBJECT_ID(N'dbo.dim_cua_hang')
           )
        BEGIN
            CREATE NONCLUSTERED INDEX ix_dim_cua_hang_city_store
            ON dbo.dim_cua_hang (city_key, store_key);
        END

        IF OBJECT_ID(N'dbo.dim_khach_hang', N'U') IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM sys.indexes
               WHERE name = N'ix_dim_khach_hang_city_key'
                 AND object_id = OBJECT_ID(N'dbo.dim_khach_hang')
           )
        BEGIN
            CREATE NONCLUSTERED INDEX ix_dim_khach_hang_city_key
            ON dbo.dim_khach_hang (city_key);
        END

        IF OBJECT_ID(N'dbo.fact_don_hang', N'U') IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM sys.indexes
               WHERE name = N'ix_fact_don_hang_date_store_product'
                 AND object_id = OBJECT_ID(N'dbo.fact_don_hang')
           )
        BEGIN
            CREATE NONCLUSTERED INDEX ix_fact_don_hang_date_store_product
            ON dbo.fact_don_hang (date_key, store_key, product_key)
            INCLUDE (customer_key, so_luong_dat, gia_dat, tong_tien);
        END

        IF OBJECT_ID(N'dbo.fact_don_hang', N'U') IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM sys.indexes
               WHERE name = N'ix_fact_don_hang_customer_date'
                 AND object_id = OBJECT_ID(N'dbo.fact_don_hang')
           )
        BEGIN
            CREATE NONCLUSTERED INDEX ix_fact_don_hang_customer_date
            ON dbo.fact_don_hang (customer_key, date_key);
        END

        IF OBJECT_ID(N'dbo.fact_don_hang', N'U') IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM sys.indexes
               WHERE name = N'ux_fact_don_hang_order_product'
                 AND object_id = OBJECT_ID(N'dbo.fact_don_hang')
           )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX ux_fact_don_hang_order_product
            ON dbo.fact_don_hang (ma_don_hang, product_key);
        END

        IF OBJECT_ID(N'dbo.fact_kho_hang', N'U') IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM sys.indexes
               WHERE name = N'ux_fact_kho_hang_date_store_product'
                 AND object_id = OBJECT_ID(N'dbo.fact_kho_hang')
           )
        BEGIN
            CREATE UNIQUE NONCLUSTERED INDEX ux_fact_kho_hang_date_store_product
            ON dbo.fact_kho_hang (date_key, store_key, product_key);
        END

        IF OBJECT_ID(N'dbo.fact_kho_hang', N'U') IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM sys.indexes
               WHERE name = N'ix_fact_kho_hang_store_product'
                 AND object_id = OBJECT_ID(N'dbo.fact_kho_hang')
           )
        BEGIN
            CREATE NONCLUSTERED INDEX ix_fact_kho_hang_store_product
            ON dbo.fact_kho_hang (store_key, product_key)
            INCLUDE (so_luong_ton, date_key);
        END
        """
    )

    conn.close()


def load_idb_into_dw(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    source_db: str = "IDBase",
    target_db: str = "DWBase",
) -> None:
    """
    Do du lieu tu IDBase vao DW: Dim dung MERGE (match thi update, khong match thi insert).
    Fact chi them dong neu chua ton tai (natural key), khong update.
    """
    ensure_dw_schema(
        server=server,
        user=user,
        password=password,
        port=port,
        source_db=source_db,
        target_db=target_db,
    )

    conn = pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database=target_db,
        autocommit=True,
    )
    cursor = conn.cursor()
    src = source_db

    # --- DIM: MERGE ---
    cursor.execute(
        f"""
        MERGE dbo.dim_thanh_pho AS T
        USING (
            SELECT ma_thanh_pho, ten_thanh_pho, bang, dia_chi_vp
            FROM [{src}].dbo.van_phong_dai_dien
        ) AS S
        ON T.ma_thanh_pho = S.ma_thanh_pho
        WHEN MATCHED THEN
            UPDATE SET
                T.ten_thanh_pho = S.ten_thanh_pho,
                T.bang = S.bang,
                T.dia_chi_vp = S.dia_chi_vp
        WHEN NOT MATCHED THEN
            INSERT (ma_thanh_pho, ten_thanh_pho, bang, dia_chi_vp)
            VALUES (S.ma_thanh_pho, S.ten_thanh_pho, S.bang, S.dia_chi_vp);
        """
    )

    cursor.execute(
        f"""
        MERGE dbo.dim_cua_hang AS T
        USING (
            SELECT ch.ma_cua_hang, ch.so_dien_thoai, tp.city_key
            FROM [{src}].dbo.cua_hang ch
            JOIN dbo.dim_thanh_pho tp ON tp.ma_thanh_pho = ch.ma_thanh_pho
        ) AS S
        ON T.ma_cua_hang = S.ma_cua_hang
        WHEN MATCHED THEN
            UPDATE SET
                T.so_dien_thoai = S.so_dien_thoai,
                T.city_key = S.city_key
        WHEN NOT MATCHED THEN
            INSERT (ma_cua_hang, so_dien_thoai, city_key)
            VALUES (S.ma_cua_hang, S.so_dien_thoai, S.city_key);
        """
    )

    cursor.execute(
        f"""
        MERGE dbo.dim_san_pham AS T
        USING (
            SELECT ma_mat_hang, mo_ta, kich_thuoc, trong_luong, gia
            FROM [{src}].dbo.mat_hang
        ) AS S
        ON T.ma_mat_hang = S.ma_mat_hang
        WHEN MATCHED THEN
            UPDATE SET
                T.mo_ta = S.mo_ta,
                T.kich_co = S.kich_thuoc,
                T.trong_luong = S.trong_luong,
                T.gia_niem_yet = S.gia
        WHEN NOT MATCHED THEN
            INSERT (ma_mat_hang, mo_ta, kich_co, trong_luong, gia_niem_yet)
            VALUES (S.ma_mat_hang, S.mo_ta, S.kich_thuoc, S.trong_luong, S.gia);
        """
    )

    cursor.execute(
        f"""
        MERGE dbo.dim_khach_hang AS T
        USING (
            SELECT
                kh.ma_khach_hang,
                kh.ten_kh AS ten_khach_hang,
                tp.city_key,
                kh.ngay_dat_hang_dau_tien,
                CASE
                    WHEN kbd.ma_khach_hang IS NOT NULL AND kdl.ma_khach_hang IS NOT NULL THEN 'ca_hai'
                    WHEN kbd.ma_khach_hang IS NOT NULL THEN 'buu_dien'
                    WHEN kdl.ma_khach_hang IS NOT NULL THEN 'du_lich'
                    ELSE 'thuong'
                END AS customer_type
            FROM [{src}].dbo.khach_hang kh
            JOIN dbo.dim_thanh_pho tp ON tp.ma_thanh_pho = kh.ma_thanh_pho
            LEFT JOIN [{src}].dbo.khach_hang_buu_dien kbd ON kbd.ma_khach_hang = kh.ma_khach_hang
            LEFT JOIN [{src}].dbo.khach_hang_du_lich kdl ON kdl.ma_khach_hang = kh.ma_khach_hang
        ) AS S
        ON T.ma_khach_hang = S.ma_khach_hang
        WHEN MATCHED THEN
            UPDATE SET
                T.ten_khach_hang = S.ten_khach_hang,
                T.city_key = S.city_key,
                T.ngay_dat_hang_dau_tien = S.ngay_dat_hang_dau_tien,
                T.customer_type = S.customer_type
        WHEN NOT MATCHED THEN
            INSERT (ma_khach_hang, ten_khach_hang, city_key, ngay_dat_hang_dau_tien, customer_type)
            VALUES (S.ma_khach_hang, S.ten_khach_hang, S.city_key, S.ngay_dat_hang_dau_tien, S.customer_type);
        """
    )

    cursor.execute(
        f"""
        MERGE dbo.dim_thoi_gian AS T
        USING (
            SELECT DISTINCT
                CAST(CONVERT(VARCHAR(8), d.full_date, 112) AS INT) AS date_key,
                d.full_date,
                DATEPART(DAY, d.full_date) AS day,
                DATEPART(MONTH, d.full_date) AS month,
                DATEPART(QUARTER, d.full_date) AS quarter,
                DATEPART(YEAR, d.full_date) AS year,
                DATEPART(WEEKDAY, d.full_date) AS day_of_week,
                CASE WHEN DATEPART(WEEKDAY, d.full_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
            FROM (
                SELECT CAST(ngay_dat_hang AS DATE) AS full_date FROM [{src}].dbo.don_dat_hang
                UNION
                SELECT CAST(thoi_gian_tao_ban_ghi AS DATE) FROM [{src}].dbo.mat_hang_duoc_luu_tru
            ) d
        ) AS S
        ON T.date_key = S.date_key
        WHEN MATCHED THEN
            UPDATE SET
                T.full_date = S.full_date,
                T.day = S.day,
                T.month = S.month,
                T.quarter = S.quarter,
                T.year = S.year,
                T.day_of_week = S.day_of_week,
                T.is_weekend = S.is_weekend
        WHEN NOT MATCHED THEN
            INSERT (date_key, full_date, day, month, quarter, year, day_of_week, is_weekend)
            VALUES (S.date_key, S.full_date, S.day, S.month, S.quarter, S.year, S.day_of_week, S.is_weekend);
        """
    )

    # --- FACT: chi insert neu chua co (natural key) ---
    cursor.execute(
        f"""
        INSERT INTO dbo.fact_don_hang (date_key, customer_key, product_key, store_key, ma_don_hang, so_luong_dat, gia_dat)
        SELECT
            CAST(CONVERT(VARCHAR(8), ddh.ngay_dat_hang, 112) AS INT) AS date_key,
            dkh.customer_key,
            dsp.product_key,
            city_store.store_key,
            ddh.ma_don AS ma_don_hang,
            mhdd.so_luong_dat,
            mhdd.gia_dat
        FROM [{src}].dbo.don_dat_hang ddh
        JOIN [{src}].dbo.mat_hang_duoc_dat mhdd ON mhdd.ma_don = ddh.ma_don
        JOIN dbo.dim_khach_hang dkh ON dkh.ma_khach_hang = ddh.ma_khach_hang
        JOIN dbo.dim_san_pham dsp ON dsp.ma_mat_hang = mhdd.ma_mat_hang
        OUTER APPLY (
            SELECT TOP 1 dch.store_key
            FROM dbo.dim_cua_hang dch
            WHERE dch.city_key = dkh.city_key
            ORDER BY dch.store_key
        ) AS city_store
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.fact_don_hang f
            WHERE f.ma_don_hang = ddh.ma_don
              AND f.product_key = dsp.product_key
        )
        """
    )

    cursor.execute(
        f"""
        INSERT INTO dbo.fact_kho_hang (date_key, store_key, product_key, so_luong_ton)
        SELECT
            CAST(CONVERT(VARCHAR(8), mhltr.thoi_gian_tao_ban_ghi, 112) AS INT) AS date_key,
            dch.store_key,
            dsp.product_key,
            mhltr.so_luong_trong_kho
        FROM [{src}].dbo.mat_hang_duoc_luu_tru mhltr
        JOIN dbo.dim_cua_hang dch ON dch.ma_cua_hang = mhltr.ma_cua_hang
        JOIN dbo.dim_san_pham dsp ON dsp.ma_mat_hang = mhltr.ma_mat_hang
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.fact_kho_hang fk
            WHERE fk.date_key = CAST(CONVERT(VARCHAR(8), mhltr.thoi_gian_tao_ban_ghi, 112) AS INT)
              AND fk.store_key = dch.store_key
              AND fk.product_key = dsp.product_key
        )
        """
    )

    conn.close()


def build_dw_from_idbase(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    source_db: str = "IDBase",
    target_db: str = "DWBase",
) -> None:
    """
    Tao DB moi dang DW (chi schema DIM/FACT), khong do du lieu.
    Dung ensure_dw_schema de co the chay nhieu lan ma khong xoa du lieu.
    """
    ensure_dw_schema(
        server=server,
        user=user,
        password=password,
        port=port,
        source_db=source_db,
        target_db=target_db,
        require_idbase=True,
    )


def ensure_dw_cube_tables(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    source_db: str = "IDBase",
    target_db: str = "DWBase",
    require_idbase: bool = True,
) -> None:
    """
    Tao cac bang cube (materialized aggregate) neu chua co.
    """
    ensure_dw_schema(
        server=server,
        user=user,
        password=password,
        port=port,
        source_db=source_db,
        target_db=target_db,
        require_idbase=require_idbase,
    )

    conn = pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database=target_db,
        autocommit=True,
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.agg_sales_city_month', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.agg_sales_city_month (
                city_key INT NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL,
                total_quantity INT NOT NULL,
                total_sales DECIMAL(18,2) NOT NULL,
                PRIMARY KEY (city_key, year, month)
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.agg_sales_city_day', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.agg_sales_city_day (
                city_key INT NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL,
                day INT NOT NULL,
                total_quantity INT NOT NULL,
                total_sales DECIMAL(18,2) NOT NULL,
                PRIMARY KEY (city_key, year, month, day)
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.agg_sales_product_month', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.agg_sales_product_month (
                product_key INT NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL,
                total_quantity INT NOT NULL,
                total_sales DECIMAL(18,2) NOT NULL,
                PRIMARY KEY (product_key, year, month)
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.agg_inventory_city_product', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.agg_inventory_city_product (
                city_key INT NOT NULL,
                product_key INT NOT NULL,
                total_inventory INT NOT NULL,
                PRIMARY KEY (city_key, product_key)
            )
        END
        """
    )
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.agg_inventory_store_product', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.agg_inventory_store_product (
                store_key INT NOT NULL,
                product_key INT NOT NULL,
                total_inventory INT NOT NULL,
                PRIMARY KEY (store_key, product_key)
            )
        END
        """
    )

    conn.close()


def load_dw_cube_data(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    source_db: str = "IDBase",
    target_db: str = "DWBase",
) -> None:
    """
    Nap du lieu cube vao DWBase theo co che upsert de tranh trung lap.
    """
    ensure_dw_cube_tables(
        server=server,
        user=user,
        password=password,
        port=port,
        source_db=source_db,
        target_db=target_db,
    )

    conn = pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database=target_db,
        autocommit=True,
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        MERGE dbo.agg_sales_city_month AS T
        USING (
            SELECT
                c.city_key,
                t.year,
                t.month,
                SUM(f.so_luong_dat) AS total_quantity,
                SUM(f.tong_tien) AS total_sales
            FROM dbo.fact_don_hang f
            JOIN dbo.dim_thoi_gian t ON f.date_key = t.date_key
            JOIN dbo.dim_cua_hang s ON f.store_key = s.store_key
            JOIN dbo.dim_thanh_pho c ON s.city_key = c.city_key
            GROUP BY c.city_key, t.year, t.month
        ) AS S
        ON T.city_key = S.city_key AND T.year = S.year AND T.month = S.month
        WHEN MATCHED THEN
            UPDATE SET
                T.total_quantity = S.total_quantity,
                T.total_sales = S.total_sales
        WHEN NOT MATCHED THEN
            INSERT (city_key, year, month, total_quantity, total_sales)
            VALUES (S.city_key, S.year, S.month, S.total_quantity, S.total_sales);
        """
    )

    cursor.execute(
        """
        MERGE dbo.agg_sales_product_month AS T
        USING (
            SELECT
                f.product_key,
                t.year,
                t.month,
                SUM(f.so_luong_dat) AS total_quantity,
                SUM(f.tong_tien) AS total_sales
            FROM dbo.fact_don_hang f
            JOIN dbo.dim_thoi_gian t ON f.date_key = t.date_key
            GROUP BY f.product_key, t.year, t.month
        ) AS S
        ON T.product_key = S.product_key AND T.year = S.year AND T.month = S.month
        WHEN MATCHED THEN
            UPDATE SET
                T.total_quantity = S.total_quantity,
                T.total_sales = S.total_sales
        WHEN NOT MATCHED THEN
            INSERT (product_key, year, month, total_quantity, total_sales)
            VALUES (S.product_key, S.year, S.month, S.total_quantity, S.total_sales);
        """
    )
    cursor.execute(
        """
        MERGE dbo.agg_sales_city_day AS T
        USING (
            SELECT
                c.city_key,
                t.year,
                t.month,
                t.day,
                SUM(f.so_luong_dat) AS total_quantity,
                SUM(f.tong_tien) AS total_sales
            FROM dbo.fact_don_hang f
            JOIN dbo.dim_thoi_gian t ON f.date_key = t.date_key
            JOIN dbo.dim_cua_hang s ON f.store_key = s.store_key
            JOIN dbo.dim_thanh_pho c ON s.city_key = c.city_key
            GROUP BY c.city_key, t.year, t.month, t.day
        ) AS S
        ON T.city_key = S.city_key
           AND T.year = S.year
           AND T.month = S.month
           AND T.day = S.day
        WHEN MATCHED THEN
            UPDATE SET
                T.total_quantity = S.total_quantity,
                T.total_sales = S.total_sales
        WHEN NOT MATCHED THEN
            INSERT (city_key, year, month, day, total_quantity, total_sales)
            VALUES (S.city_key, S.year, S.month, S.day, S.total_quantity, S.total_sales);
        """
    )

    cursor.execute(
        """
        MERGE dbo.agg_inventory_city_product AS T
        USING (
            SELECT
                c.city_key,
                f.product_key,
                SUM(f.so_luong_ton) AS total_inventory
            FROM dbo.fact_kho_hang f
            JOIN dbo.dim_cua_hang s ON f.store_key = s.store_key
            JOIN dbo.dim_thanh_pho c ON s.city_key = c.city_key
            GROUP BY c.city_key, f.product_key
        ) AS S
        ON T.city_key = S.city_key AND T.product_key = S.product_key
        WHEN MATCHED THEN
            UPDATE SET
                T.total_inventory = S.total_inventory
        WHEN NOT MATCHED THEN
            INSERT (city_key, product_key, total_inventory)
            VALUES (S.city_key, S.product_key, S.total_inventory);
        """
    )

    cursor.execute(
        """
        MERGE dbo.agg_inventory_store_product AS T
        USING (
            SELECT
                f.store_key,
                f.product_key,
                SUM(f.so_luong_ton) AS total_inventory
            FROM dbo.fact_kho_hang f
            GROUP BY f.store_key, f.product_key
        ) AS S
        ON T.store_key = S.store_key AND T.product_key = S.product_key
        WHEN MATCHED THEN
            UPDATE SET
                T.total_inventory = S.total_inventory
        WHEN NOT MATCHED THEN
            INSERT (store_key, product_key, total_inventory)
            VALUES (S.store_key, S.product_key, S.total_inventory);
        """
    )

    conn.close()
