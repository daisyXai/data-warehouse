"""
Standalone seed — hai CSDL tach biet:
- RepresentativeOfficeDB: khach_hang, khach_hang_du_lich, khach_hang_buu_dien (moi 20 dong, ma_kh).
- SellDB: van_phong_dai_dien (bo sung), cua_hang, mat_hang, mat_hang_duoc_luu_tru,
  don_dat_hang, mat_hang_duoc_dat (moi 20 dong).

ma_kh o RepDB = ma_khach_hang trong don_dat_hang SellDB (cung gia tri KH_SEED_xx) de merge/idbase.

Chay: python seed_data.py
"""
from __future__ import annotations

import pymssql

SERVER = "127.0.0.1"
USER = "sa"
PASSWORD = "YourStrong!Pass123"
PORT = 1434
DATABASE_REP = "RepresentativeOfficeDB"
DATABASE_SELL = "SellDB"

SEED_PREFIX_KH = "KH_SEED_"
SEED_PREFIX_CH = "CH_SEED_"
SEED_PREFIX_MH = "MH_SEED_"
SEED_PREFIX_DH = "DH_SEED_"

ROWS_KH = [
    ("01", "Nguyen Van An", "TP_HCM", "2024-01-15"),
    ("02", "Tran Thi Binh", "HN", "2024-02-03"),
    ("03", "Le Hoang Cuong", "DN", "2024-03-22"),
    ("04", "Pham Minh Duc", "HP", "2024-04-10"),
    ("05", "Hoang Thi Em", "CT", "2024-05-01"),
    ("06", "Vu Quoc Giang", "BD", "2024-05-18"),
    ("07", "Dang Thi Hoa", "VT", "2024-06-07"),
    ("08", "Bui Van Hung", "QN", "2024-06-25"),
    ("09", "Do Thi Lan", "HUE", "2024-07-12"),
    ("10", "Ngo Van Minh", "LA", "2024-07-30"),
    ("11", "Ly Thi Ngoc", "AG", "2024-08-05"),
    ("12", "Truong Van Phuc", "KG", "2024-08-19"),
    ("13", "Chu Thi Quyen", "TV", "2024-09-02"),
    ("14", "Mai Van Son", "PY", "2024-09-14"),
    ("15", "Ton Thi Tam", "NT", "2024-10-01"),
    ("16", "Huynh Van Tuan", "GL", "2024-10-11"),
    ("17", "Phan Thi Uyen", "DL", "2024-10-28"),
    ("18", "Lam Van Vinh", "QB", "2024-11-05"),
    ("19", "Trieu Thi Xuan", "NA", "2024-11-20"),
    ("20", "Quach Van Yen", "TH", "2024-12-01"),
]

HUONG_DAN = [
    "Nguyen Thi Mai",
    "Tran Van Hai",
    "Le Thi Huong",
    "Pham Quoc Tuan",
    "Hoang Thi Lan",
    "Vu Minh Chau",
    "Dang Van Khoa",
    "Bui Thi Nga",
    "Do Van Phong",
    "Ngo Thi Hanh",
    "Ly Quang Minh",
    "Truong Thi Cuc",
    "Chu Van Dung",
    "Mai Thi Loan",
    "Ton Van Thinh",
    "Huynh Thi Phuong",
    "Phan Van Binh",
    "Lam Thi Dao",
    "Trieu Van Nam",
    "Quach Thi Vy",
]

DIA_CHI_BUU_DIEN = [
    "12 Nguyen Hue, Q1, TP.HCM",
    "45 Hang Bac, Hoan Kiem, Ha Noi",
    "8 Bach Dang, Hai Chau, Da Nang",
    "22 Minh Khai, Hong Bang, Hai Phong",
    "5 Nguyen Van Cu, Ninh Kieu, Can Tho",
    "17 Dai lo Binh Duong, Thu Dau Mot",
    "9 Le Hong Phong, Vung Tau",
    "33 Tran Phu, Nha Trang, Khanh Hoa",
    "11 Le Loi, TP Hue",
    "7 Nguyen Trai, Ben Tre",
    "14 Tran Hung Dao, Long Xuyen, An Giang",
    "20 Nguyen Dinh Chieu, Rach Gia, Kien Giang",
    "6 Vo Van Kiet, My Tho, Tien Giang",
    "10 Hung Vuong, Phan Thiet, Binh Thuan",
    "4 Le Duan, Pleiku, Gia Lai",
    "18 Nguyen Tat Thanh, Buon Ma Thuot, Dak Lak",
    "25 Nguyen Chi Thanh, Da Lat, Lam Dong",
    "9 Ly Thuong Kiet, Dong Hoi, Quang Binh",
    "12 Tran Phu, Vinh, Nghe An",
    "8 Le Loi, Thanh Hoa",
]

SDT_CUA_HANG = [f"028{3800000 + i:07d}" for i in range(20)]

MAT_HANG_ROWS: list[tuple[str, str | None, float | None, float]] = [
    ("Tui xach da PU cao cap", "30x25x12 cm", 0.85, 890000),
    ("Vali keo ABS size S", "55x38x22 cm", 3.20, 2150000),
    ("Balo laptop 15.6 inch", "45x32x18 cm", 1.10, 650000),
    ("Vi nam da that", "11x9x2 cm", 0.15, 450000),
    ("Tui tote canvas in logo", "40x35x10 cm", 0.40, 180000),
    ("Cap da cong so", "38x28x8 cm", 1.25, 1200000),
    ("Tui dung my pham chong nuoc", "25x18x12 cm", 0.35, 220000),
    ("Vali size M polycarbonate", "65x45x28 cm", 4.10, 2890000),
    ("Balo du lich 40L", "52x35x22 cm", 1.45, 750000),
    ("Tui clutch di tiec", "24x15x5 cm", 0.28, 520000),
    ("Tui messenger vai bo", "32x26x10 cm", 0.55, 320000),
    ("Cap xach nu cong so", "35x27x12 cm", 0.95, 980000),
    ("Tui gym chong nuoc", "48x28x25 cm", 0.70, 290000),
    ("Vali keo size L", "75x50x30 cm", 5.80, 3490000),
    ("Balo hoc sinh phan quang", "42x30x15 cm", 0.60, 195000),
    ("Tui boc vali stretch", "fit size M", 0.25, 150000),
    ("Vi passport RFID", "14x10x1 cm", 0.12, 280000),
    ("Tui zip chong soc laptop", "36x26x4 cm", 0.45, 410000),
    ("Balo mini day rut", "28x35x12 cm", 0.38, 165000),
    ("Set 3 tui packing cube", "combo", 0.55, 275000),
]

SO_LUONG_KHO = [120, 85, 200, 45, 300, 60, 150, 40, 95, 110, 180, 55, 220, 35, 90, 75, 130, 160, 50, 140]

NGAY_DAT_HANG = [
    "2024-02-01",
    "2024-02-18",
    "2024-03-05",
    "2024-03-20",
    "2024-04-08",
    "2024-04-25",
    "2024-05-12",
    "2024-05-28",
    "2024-06-10",
    "2024-06-22",
    "2024-07-03",
    "2024-07-19",
    "2024-08-01",
    "2024-08-14",
    "2024-09-02",
    "2024-09-17",
    "2024-10-05",
    "2024-10-21",
    "2024-11-08",
    "2024-11-25",
]

SO_LUONG_DAT = [2, 1, 3, 1, 4, 1, 2, 1, 2, 1, 1, 2, 3, 1, 2, 1, 1, 2, 1, 3]
GIA_DAT = [
    870000,
    2150000,
    620000,
    450000,
    175000,
    1180000,
    220000,
    2890000,
    730000,
    520000,
    310000,
    960000,
    285000,
    3490000,
    190000,
    148000,
    275000,
    400000,
    160000,
    265000,
]

VAN_PHONG_BY_CITY: dict[str, tuple[str, str]] = {
    "TP_HCM": ("Thanh pho Ho Chi Minh", "45 Le Duan, Q1, TP.HCM"),
    "HN": ("Ha Noi", "1 Ba Trieu, Hoan Kiem, Ha Noi"),
    "DN": ("Da Nang", "34 Bach Dang, Hai Chau, Da Nang"),
    "HP": ("Hai Phong", "18 Minh Khai, Hong Bang, Hai Phong"),
    "CT": ("Can Tho", "55 Nguyen Van Cu, Ninh Kieu, Can Tho"),
    "BD": ("Binh Duong", "1 Dai lo Binh Duong, Thu Dau Mot"),
    "VT": ("Ba Ria - Vung Tau", "45 Le Hong Phong, Vung Tau"),
    "QN": ("Khanh Hoa", "2 Tran Phu, Nha Trang"),
    "HUE": ("Thua Thien Hue", "16 Le Loi, TP Hue"),
    "LA": ("Long An", "10 Nguyen Trai, Tan An"),
    "AG": ("An Giang", "5 Tran Hung Dao, Long Xuyen"),
    "KG": ("Kien Giang", "12 Nguyen Trung Truc, Rach Gia"),
    "TV": ("Tien Giang", "8 Vo Van Kiet, My Tho"),
    "PY": ("Binh Thuan", "20 Nguyen Tat Thanh, Phan Thiet"),
    "NT": ("Ninh Thuan", "7 Thong Nhat, Phan Rang"),
    "GL": ("Gia Lai", "2 Hung Vuong, Pleiku"),
    "DL": ("Lam Dong", "1 Nguyen Chi Thanh, Da Lat"),
    "QB": ("Quang Binh", "9 Ly Thuong Kiet, Dong Hoi"),
    "NA": ("Nghe An", "12 Tran Phu, Vinh"),
    "TH": ("Thanh Hoa", "8 Le Loi, TP Thanh Hoa"),
}


def _connect(db: str) -> pymssql.Connection:
    return pymssql.connect(
        server=SERVER,
        user=USER,
        password=PASSWORD,
        port=PORT,
        database=db,
        autocommit=True,
    )


def _ensure_van_phong_sell(cur: pymssql.Cursor) -> None:
    """SellDB: dbo.van_phong_dai_dien (sell_db.py — cot thoi_gian mac dinh)."""
    for ma_tp, (ten, dia_chi) in VAN_PHONG_BY_CITY.items():
        cur.execute(
            "SELECT 1 FROM dbo.van_phong_dai_dien WHERE ma_thanh_pho = %s",
            (ma_tp,),
        )
        if cur.fetchone() is None:
            cur.execute(
                """
                INSERT INTO dbo.van_phong_dai_dien (ma_thanh_pho, ten_thanh_pho, dia_chi_vp, bang)
                VALUES (%s, %s, %s, NULL)
                """,
                (ma_tp, ten, dia_chi),
            )


def seed_representative_office_db() -> None:
    """Schema representative_office_db.py: ma_kh, khong co van_phong."""
    conn = _connect(DATABASE_REP)
    cur = conn.cursor()

    cur.execute(
        f"DELETE FROM dbo.khach_hang_du_lich WHERE ma_kh LIKE N'{SEED_PREFIX_KH}%'"
    )
    cur.execute(
        f"DELETE FROM dbo.khach_hang_buu_dien WHERE ma_kh LIKE N'{SEED_PREFIX_KH}%'"
    )
    cur.execute(f"DELETE FROM dbo.khach_hang WHERE ma_kh LIKE N'{SEED_PREFIX_KH}%'")

    sql_kh = """
        INSERT INTO dbo.khach_hang (ma_kh, ten_kh, ma_thanh_pho, ngay_dat_hang_dau_tien)
        VALUES (%s, %s, %s, %s)
    """
    sql_dl = """
        INSERT INTO dbo.khach_hang_du_lich (ma_kh, huong_dan_vien_du_lich)
        VALUES (%s, %s)
    """
    sql_bd = """
        INSERT INTO dbo.khach_hang_buu_dien (ma_kh, dia_chi_buu_dien)
        VALUES (%s, %s)
    """

    for i, (suffix, ten, tp, ngay) in enumerate(ROWS_KH):
        ma_kh = f"{SEED_PREFIX_KH}{suffix}"
        cur.execute(sql_kh, (ma_kh, ten, tp, ngay))
        cur.execute(sql_dl, (ma_kh, HUONG_DAN[i]))
        cur.execute(sql_bd, (ma_kh, DIA_CHI_BUU_DIEN[i]))

    conn.close()
    print(
        f"[{DATABASE_REP}] 20 khach_hang, 20 khach_hang_du_lich, 20 khach_hang_buu_dien "
        f"(ma_kh: {SEED_PREFIX_KH}01..20)."
    )


def seed_sell_db() -> None:
    """Schema sell_db.py: mat_hang.ma_mh, mat_hang.kich_co; FK noi bo SellDB."""
    conn = _connect(DATABASE_SELL)
    cur = conn.cursor()

    cur.execute(
        f"DELETE FROM dbo.mat_hang_duoc_dat WHERE ma_don LIKE N'{SEED_PREFIX_DH}%'"
    )
    cur.execute(f"DELETE FROM dbo.don_dat_hang WHERE ma_don LIKE N'{SEED_PREFIX_DH}%'")
    cur.execute(
        f"DELETE FROM dbo.mat_hang_duoc_luu_tru WHERE ma_cua_hang LIKE N'{SEED_PREFIX_CH}%'"
    )
    cur.execute(f"DELETE FROM dbo.mat_hang WHERE ma_mh LIKE N'{SEED_PREFIX_MH}%'")
    cur.execute(f"DELETE FROM dbo.cua_hang WHERE ma_cua_hang LIKE N'{SEED_PREFIX_CH}%'")

    _ensure_van_phong_sell(cur)

    sql_ch = """
        INSERT INTO dbo.cua_hang (ma_cua_hang, ma_thanh_pho, so_dien_thoai)
        VALUES (%s, %s, %s)
    """
    sql_mh = """
        INSERT INTO dbo.mat_hang (ma_mh, mo_ta, kich_co, trong_luong, gia)
        VALUES (%s, %s, %s, %s, %s)
    """
    sql_mh_lt = """
        INSERT INTO dbo.mat_hang_duoc_luu_tru (ma_cua_hang, ma_mat_hang, so_luong_trong_kho)
        VALUES (%s, %s, %s)
    """
    sql_dh = """
        INSERT INTO dbo.don_dat_hang (ma_don, ngay_dat_hang, ma_khach_hang)
        VALUES (%s, %s, %s)
    """
    sql_mh_d = """
        INSERT INTO dbo.mat_hang_duoc_dat (ma_don, ma_mat_hang, so_luong_dat, gia_dat)
        VALUES (%s, %s, %s, %s)
    """

    for i, (suffix, _ten, tp, _ngay) in enumerate(ROWS_KH):
        ma_kh = f"{SEED_PREFIX_KH}{suffix}"
        ma_ch = f"{SEED_PREFIX_CH}{suffix}"
        cur.execute(sql_ch, (ma_ch, tp, SDT_CUA_HANG[i]))

        mo_ta, kich_co, tl, gia = MAT_HANG_ROWS[i]
        ma_mh = f"{SEED_PREFIX_MH}{suffix}"
        cur.execute(sql_mh, (ma_mh, mo_ta, kich_co, tl, gia))

        cur.execute(sql_mh_lt, (ma_ch, ma_mh, SO_LUONG_KHO[i]))

        ma_don = f"{SEED_PREFIX_DH}{suffix}"
        cur.execute(sql_dh, (ma_don, NGAY_DAT_HANG[i], ma_kh))
        cur.execute(
            sql_mh_d,
            (ma_don, ma_mh, SO_LUONG_DAT[i], float(GIA_DAT[i])),
        )

    conn.close()
    print(
        f"[{DATABASE_SELL}] 20 cua_hang, 20 mat_hang (ma_mh), 20 mat_hang_duoc_luu_tru, "
        "20 don_dat_hang (ma_khach_hang = ma_kh ben RepDB), 20 mat_hang_duoc_dat."
    )


def main() -> None:
    seed_representative_office_db()
    seed_sell_db()
    print("Xong seed hai CSDL.")


if __name__ == "__main__":
    main()
