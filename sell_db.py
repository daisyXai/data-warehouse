import pymssql


def init_sell_db(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    database_name: str = "SellDB",
) -> None:
    """
    Tao CSDL ban hang va cac bang lien quan.
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
        IF OBJECT_ID(N'dbo.van_phong_dai_dien', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.van_phong_dai_dien (
                ma_thanh_pho VARCHAR(20) NOT NULL PRIMARY KEY,
                ten_thanh_pho NVARCHAR(200) NOT NULL,
                dia_chi_vp NVARCHAR(300) NOT NULL,
                bang NVARCHAR(100) NULL,
                thoi_gian DATETIME2 NOT NULL DEFAULT SYSDATETIME()
            )
        END
        """
    )

    db_cursor.execute(
        """
        IF OBJECT_ID(N'dbo.cua_hang', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.cua_hang (
                ma_cua_hang VARCHAR(20) NOT NULL PRIMARY KEY,
                ma_thanh_pho VARCHAR(20) NOT NULL,
                so_dien_thoai VARCHAR(30) NULL,
                thoi_gian DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT fk_cua_hang_van_phong_dai_dien
                    FOREIGN KEY (ma_thanh_pho) REFERENCES dbo.van_phong_dai_dien(ma_thanh_pho)
            )
        END
        """
    )

    db_cursor.execute(
        """
        IF OBJECT_ID(N'dbo.mat_hang', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.mat_hang (
                ma_mh VARCHAR(20) NOT NULL PRIMARY KEY,
                mo_ta NVARCHAR(300) NOT NULL,
                kich_co NVARCHAR(50) NULL,
                trong_luong DECIMAL(10,2) NULL,
                gia DECIMAL(18,2) NOT NULL,
                thoi_gian DATETIME2 NOT NULL DEFAULT SYSDATETIME()
            )
        END
        """
    )

    db_cursor.execute(
        """
        IF OBJECT_ID(N'dbo.mat_hang_duoc_luu_tru', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.mat_hang_duoc_luu_tru (
                ma_cua_hang VARCHAR(20) NOT NULL,
                ma_mat_hang VARCHAR(20) NOT NULL,
                so_luong_trong_kho INT NOT NULL CHECK (so_luong_trong_kho >= 0),
                thoi_gian DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT pk_mat_hang_duoc_luu_tru PRIMARY KEY (ma_cua_hang, ma_mat_hang),
                CONSTRAINT fk_mat_hang_duoc_luu_tru_cua_hang
                    FOREIGN KEY (ma_cua_hang) REFERENCES dbo.cua_hang(ma_cua_hang),
                CONSTRAINT fk_mat_hang_duoc_luu_tru_mat_hang
                    FOREIGN KEY (ma_mat_hang) REFERENCES dbo.mat_hang(ma_mh)
            )
        END
        """
    )

    db_cursor.execute(
        """
        IF OBJECT_ID(N'dbo.don_dat_hang', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.don_dat_hang (
                ma_don VARCHAR(20) NOT NULL PRIMARY KEY,
                ngay_dat_hang DATE NOT NULL,
                ma_khach_hang VARCHAR(20) NOT NULL
            )
        END
        """
    )

    db_cursor.execute(
        """
        IF OBJECT_ID(N'dbo.mat_hang_duoc_dat', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.mat_hang_duoc_dat (
                ma_don VARCHAR(20) NOT NULL,
                ma_mat_hang VARCHAR(20) NOT NULL,
                so_luong_dat INT NOT NULL CHECK (so_luong_dat > 0),
                gia_dat DECIMAL(18,2) NOT NULL CHECK (gia_dat >= 0),
                thoi_gian DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT pk_mat_hang_duoc_dat PRIMARY KEY (ma_don, ma_mat_hang),
                CONSTRAINT fk_mat_hang_duoc_dat_don_dat_hang
                    FOREIGN KEY (ma_don) REFERENCES dbo.don_dat_hang(ma_don),
                CONSTRAINT fk_mat_hang_duoc_dat_mat_hang
                    FOREIGN KEY (ma_mat_hang) REFERENCES dbo.mat_hang(ma_mh)
            )
        END
        """
    )

    db_conn.close()
