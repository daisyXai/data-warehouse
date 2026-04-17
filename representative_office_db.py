import pymssql


def init_and_seed_representative_office_db(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    database_name: str = "RepresentativeOfficeDB",
) -> None:
    """
    Tao CSDL van phong dai dien va seed du lieu mau.
    """
    conn = pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database="master",
        autocommit=True,
    )
    cursor = conn.cursor()

    cursor.execute(
        f"""
        IF DB_ID(N'{database_name}') IS NULL
            CREATE DATABASE [{database_name}]
        """
    )
    conn.close()

    db_conn = pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database=database_name,
        autocommit=True,
    )
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        """
        IF OBJECT_ID(N'dbo.khach_hang', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.khach_hang (
                ma_kh VARCHAR(20) NOT NULL PRIMARY KEY,
                ten_kh NVARCHAR(200) NOT NULL,
                ma_thanh_pho VARCHAR(20) NOT NULL,
                ngay_dat_hang_dau_tien DATE NOT NULL
            )
        END
        """
    )

    db_cursor.execute(
        """
        IF OBJECT_ID(N'dbo.khach_hang_du_lich', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.khach_hang_du_lich (
                ma_kh VARCHAR(20) NOT NULL PRIMARY KEY,
                huong_dan_vien_du_lich NVARCHAR(200) NOT NULL,
                thoi_gian DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT fk_khach_hang_du_lich_khach_hang
                    FOREIGN KEY (ma_kh) REFERENCES dbo.khach_hang(ma_kh)
            )
        END
        """
    )

    db_cursor.execute(
        """
        IF OBJECT_ID(N'dbo.khach_hang_buu_dien', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.khach_hang_buu_dien (
                ma_kh VARCHAR(20) NOT NULL PRIMARY KEY,
                dia_chi_buu_dien NVARCHAR(300) NOT NULL,
                thoi_gian DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT fk_khach_hang_buu_dien_khach_hang
                    FOREIGN KEY (ma_kh) REFERENCES dbo.khach_hang(ma_kh)
            )
        END
        """
    )

    

    db_conn.close()
