import pymssql


def merge_ier_to_idbase(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    source_sell_db: str = "SellDB",
    source_rep_db: str = "RepresentativeOfficeDB",
    target_db: str = "IDBase",
) -> None:
    """
    Merge 2 IER (SellDB + RepresentativeOfficeDB) thanh 1 IDB moi.
    Viec doi ten field theo yeu cau reflection chi thuc hien tai IDBase.
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

    # Drop theo thu tu FK de co the chay lai merge nhieu lan.
    cursor.execute(
        """
        IF OBJECT_ID(N'dbo.mat_hang_duoc_dat', N'U') IS NOT NULL DROP TABLE dbo.mat_hang_duoc_dat;
        IF OBJECT_ID(N'dbo.mat_hang_duoc_luu_tru', N'U') IS NOT NULL DROP TABLE dbo.mat_hang_duoc_luu_tru;
        IF OBJECT_ID(N'dbo.don_dat_hang', N'U') IS NOT NULL DROP TABLE dbo.don_dat_hang;
        IF OBJECT_ID(N'dbo.khach_hang_buu_dien', N'U') IS NOT NULL DROP TABLE dbo.khach_hang_buu_dien;
        IF OBJECT_ID(N'dbo.khach_hang_du_lich', N'U') IS NOT NULL DROP TABLE dbo.khach_hang_du_lich;
        IF OBJECT_ID(N'dbo.khach_hang', N'U') IS NOT NULL DROP TABLE dbo.khach_hang;
        IF OBJECT_ID(N'dbo.cua_hang', N'U') IS NOT NULL DROP TABLE dbo.cua_hang;
        IF OBJECT_ID(N'dbo.mat_hang', N'U') IS NOT NULL DROP TABLE dbo.mat_hang;
        IF OBJECT_ID(N'dbo.van_phong_dai_dien', N'U') IS NOT NULL DROP TABLE dbo.van_phong_dai_dien;
        """
    )

    cursor.execute(
        """
        CREATE TABLE dbo.van_phong_dai_dien (
            ma_thanh_pho VARCHAR(20) NOT NULL PRIMARY KEY,
            ten_thanh_pho NVARCHAR(200) NOT NULL,
            dia_chi_vp NVARCHAR(300) NOT NULL,
            bang NVARCHAR(100) NULL,
            thoi_gian_tao_ban_ghi DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE dbo.khach_hang (
            ma_khach_hang VARCHAR(20) NOT NULL PRIMARY KEY,
            ten_kh NVARCHAR(200) NOT NULL,
            ma_thanh_pho VARCHAR(20) NOT NULL,
            ngay_dat_hang_dau_tien DATE NOT NULL,
            CONSTRAINT fk_khach_hang_van_phong_dai_dien
                FOREIGN KEY (ma_thanh_pho) REFERENCES dbo.van_phong_dai_dien(ma_thanh_pho)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE dbo.khach_hang_du_lich (
            ma_khach_hang VARCHAR(20) NOT NULL PRIMARY KEY,
            huong_dan_vien_du_lich NVARCHAR(200) NOT NULL,
            thoi_gian_tao_ban_ghi DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            CONSTRAINT fk_khach_hang_du_lich_khach_hang
                FOREIGN KEY (ma_khach_hang) REFERENCES dbo.khach_hang(ma_khach_hang)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE dbo.khach_hang_buu_dien (
            ma_khach_hang VARCHAR(20) NOT NULL PRIMARY KEY,
            dia_chi_buu_dien NVARCHAR(300) NOT NULL,
            thoi_gian_tao_ban_ghi DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            CONSTRAINT fk_khach_hang_buu_dien_khach_hang
                FOREIGN KEY (ma_khach_hang) REFERENCES dbo.khach_hang(ma_khach_hang)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE dbo.cua_hang (
            ma_cua_hang VARCHAR(20) NOT NULL PRIMARY KEY,
            ma_thanh_pho VARCHAR(20) NOT NULL,
            so_dien_thoai VARCHAR(30) NULL,
            thoi_gian_tao_ban_ghi DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            CONSTRAINT fk_cua_hang_van_phong_dai_dien
                FOREIGN KEY (ma_thanh_pho) REFERENCES dbo.van_phong_dai_dien(ma_thanh_pho)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE dbo.mat_hang (
            ma_mat_hang VARCHAR(20) NOT NULL PRIMARY KEY,
            mo_ta NVARCHAR(300) NOT NULL,
            kich_thuoc NVARCHAR(50) NULL,
            trong_luong DECIMAL(10,2) NULL,
            gia DECIMAL(18,2) NOT NULL,
            thoi_gian_tao_ban_ghi DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE dbo.mat_hang_duoc_luu_tru (
            ma_cua_hang VARCHAR(20) NOT NULL,
            ma_mat_hang VARCHAR(20) NOT NULL,
            so_luong_trong_kho INT NOT NULL CHECK (so_luong_trong_kho >= 0),
            thoi_gian_tao_ban_ghi DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            CONSTRAINT pk_mat_hang_duoc_luu_tru PRIMARY KEY (ma_cua_hang, ma_mat_hang),
            CONSTRAINT fk_mat_hang_duoc_luu_tru_cua_hang
                FOREIGN KEY (ma_cua_hang) REFERENCES dbo.cua_hang(ma_cua_hang),
            CONSTRAINT fk_mat_hang_duoc_luu_tru_mat_hang
                FOREIGN KEY (ma_mat_hang) REFERENCES dbo.mat_hang(ma_mat_hang)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE dbo.don_dat_hang (
            ma_don VARCHAR(20) NOT NULL PRIMARY KEY,
            ngay_dat_hang DATE NOT NULL,
            ma_khach_hang VARCHAR(20) NOT NULL,
            CONSTRAINT fk_don_dat_hang_khach_hang
                FOREIGN KEY (ma_khach_hang) REFERENCES dbo.khach_hang(ma_khach_hang)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE dbo.mat_hang_duoc_dat (
            ma_don VARCHAR(20) NOT NULL,
            ma_mat_hang VARCHAR(20) NOT NULL,
            so_luong_dat INT NOT NULL CHECK (so_luong_dat > 0),
            gia_dat DECIMAL(18,2) NOT NULL CHECK (gia_dat >= 0),
            thoi_gian_tao_ban_ghi DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            CONSTRAINT pk_mat_hang_duoc_dat PRIMARY KEY (ma_don, ma_mat_hang),
            CONSTRAINT fk_mat_hang_duoc_dat_don_dat_hang
                FOREIGN KEY (ma_don) REFERENCES dbo.don_dat_hang(ma_don),
            CONSTRAINT fk_mat_hang_duoc_dat_mat_hang
                FOREIGN KEY (ma_mat_hang) REFERENCES dbo.mat_hang(ma_mat_hang)
        )
        """
    )

    # Van phong dai dien tu SellDB.
    cursor.execute(
        f"""
        INSERT INTO dbo.van_phong_dai_dien (ma_thanh_pho, ten_thanh_pho, dia_chi_vp, bang, thoi_gian_tao_ban_ghi)
        SELECT s.ma_thanh_pho, s.ten_thanh_pho, s.dia_chi_vp, s.bang, s.thoi_gian
        FROM [{source_sell_db}].dbo.van_phong_dai_dien s
        """
    )

    # Bo sung ma_thanh_pho bi thieu (co o khach_hang nhung chua co o van_phong_dai_dien).
    cursor.execute(
        f"""
        INSERT INTO dbo.van_phong_dai_dien (ma_thanh_pho, ten_thanh_pho, dia_chi_vp, bang, thoi_gian_tao_ban_ghi)
        SELECT DISTINCT r.ma_thanh_pho, r.ma_thanh_pho, N'chua_cap_nhat', NULL, SYSDATETIME()
        FROM [{source_rep_db}].dbo.khach_hang r
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.van_phong_dai_dien v
            WHERE v.ma_thanh_pho = r.ma_thanh_pho
        )
        """
    )

    cursor.execute(
        f"""
        INSERT INTO dbo.khach_hang (ma_khach_hang, ten_kh, ma_thanh_pho, ngay_dat_hang_dau_tien)
        SELECT ma_kh, ten_kh, ma_thanh_pho, ngay_dat_hang_dau_tien
        FROM [{source_rep_db}].dbo.khach_hang
        """
    )
    cursor.execute(
        f"""
        INSERT INTO dbo.khach_hang_du_lich (ma_khach_hang, huong_dan_vien_du_lich, thoi_gian_tao_ban_ghi)
        SELECT ma_kh, huong_dan_vien_du_lich, thoi_gian
        FROM [{source_rep_db}].dbo.khach_hang_du_lich
        """
    )
    cursor.execute(
        f"""
        INSERT INTO dbo.khach_hang_buu_dien (ma_khach_hang, dia_chi_buu_dien, thoi_gian_tao_ban_ghi)
        SELECT ma_kh, dia_chi_buu_dien, thoi_gian
        FROM [{source_rep_db}].dbo.khach_hang_buu_dien
        """
    )
    cursor.execute(
        f"""
        INSERT INTO dbo.cua_hang (ma_cua_hang, ma_thanh_pho, so_dien_thoai, thoi_gian_tao_ban_ghi)
        SELECT ma_cua_hang, ma_thanh_pho, so_dien_thoai, thoi_gian
        FROM [{source_sell_db}].dbo.cua_hang
        """
    )
    cursor.execute(
        f"""
        INSERT INTO dbo.mat_hang (ma_mat_hang, mo_ta, kich_thuoc, trong_luong, gia, thoi_gian_tao_ban_ghi)
        SELECT ma_mh, mo_ta, kich_co, trong_luong, gia, thoi_gian
        FROM [{source_sell_db}].dbo.mat_hang
        """
    )
    cursor.execute(
        f"""
        INSERT INTO dbo.mat_hang_duoc_luu_tru (ma_cua_hang, ma_mat_hang, so_luong_trong_kho, thoi_gian_tao_ban_ghi)
        SELECT ma_cua_hang, ma_mat_hang, so_luong_trong_kho, thoi_gian
        FROM [{source_sell_db}].dbo.mat_hang_duoc_luu_tru
        """
    )
    cursor.execute(
        f"""
        INSERT INTO dbo.don_dat_hang (ma_don, ngay_dat_hang, ma_khach_hang)
        SELECT ma_don, ngay_dat_hang, ma_khach_hang
        FROM [{source_sell_db}].dbo.don_dat_hang
        """
    )
    cursor.execute(
        f"""
        INSERT INTO dbo.mat_hang_duoc_dat (ma_don, ma_mat_hang, so_luong_dat, gia_dat, thoi_gian_tao_ban_ghi)
        SELECT ma_don, ma_mat_hang, so_luong_dat, gia_dat, thoi_gian
        FROM [{source_sell_db}].dbo.mat_hang_duoc_dat
        """
    )

    conn.close()
