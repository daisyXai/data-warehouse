import signal
import time
from collections import defaultdict

import pymssql

from dw_builder import (
    build_dw_from_idbase,
    ensure_dw_cube_tables,
    load_dw_cube_data,
    load_idb_into_dw,
)
from dw_seed_demo import run_all_dw_demo_seed
from idbase_merge import merge_ier_to_idbase
from representative_office_db import init_and_seed_representative_office_db
from sell_db import init_sell_db
### su dung kwarg de bat keyword gen_seed_data === True hoac false de dua vao do de chayj ham tao seed data .
import argparse
### khởi tạo 1 fastAPi server 
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
app = FastAPI()

DB_SERVER = "127.0.0.1"
DB_USER = "sa"
DB_PASSWORD = "YourStrong!Pass123"
DB_PORT = 1434
DW_DB = "DWBase"


def _query_dw(sql: str, params: tuple = ()) -> list[dict]:
    conn = pymssql.connect(
        server=DB_SERVER,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        database=DW_DB,
        autocommit=True,
    )
    cursor = conn.cursor(as_dict=True)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    conn.close()
    return rows


def _pivot_rows(rows: list[dict], row_key: str, col_key: str, value_key: str) -> dict:
    row_names = sorted({str(r[row_key]) for r in rows})
    col_names = sorted({str(r[col_key]) for r in rows})
    matrix = defaultdict(dict)
    for r in rows:
        matrix[str(r[row_key])][str(r[col_key])] = float(r[value_key] or 0)

    data = []
    for row_name in row_names:
        rec = {"row": row_name}
        for col_name in col_names:
            rec[col_name] = matrix[row_name].get(col_name, 0)
        data.append(rec)

    return {"columns": ["row"] + col_names, "rows": data}


@app.get("/", response_class=HTMLResponse)
def read_root() -> str:
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>OLAP Demo Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; background: #f5f7fb; }
    .panel { background: white; border-radius: 10px; padding: 16px; margin-bottom: 12px; box-shadow: 0 2px 8px rgba(0,0,0,.06); }
    .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    select, input, button { padding: 8px; border: 1px solid #d0d7de; border-radius: 8px; }
    button { cursor: pointer; background: #2563eb; color: white; border: none; }
    button.secondary { background: #64748b; }
    .table-wrap { margin-top: 12px; border: 1px solid #e2e8f0; border-radius: 12px; overflow: auto; max-height: 65vh; background: #fff; }
    table { width: 100%; border-collapse: separate; border-spacing: 0; min-width: 900px; }
    th, td { border-bottom: 1px solid #e5e7eb; padding: 10px 12px; text-align: left; font-size: 14px; }
    th { background: #f1f5f9; position: sticky; top: 0; z-index: 1; }
    tbody tr:nth-child(even) td { background: #fafcff; }
    tbody tr:hover td { background: #eef6ff; }
    td.num { text-align: right; font-variant-numeric: tabular-nums; }
    .pivot-badge { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #dbeafe; color: #1e40af; font-size: 12px; margin-left: 8px; }
    .pivot-table th { background: linear-gradient(180deg, #1e3a8a, #1d4ed8); color: #fff; border-bottom-color: #1d4ed8; }
    .pivot-row-key { font-weight: 600; color: #0f172a; background: #f8fafc !important; }
    .muted { color: #475569; font-size: 14px; }
  </style>
</head>
<body>
  <h2>OLAP Demo Dashboard</h2>
  <div class="panel">
    <div class="row">
      <label>Cube:</label>
      <select id="cubeType">
        <option value="sales">Sales Cube</option>
        <option value="inventory">Inventory Cube</option>
      </select>
      <button onclick="refreshCube()">Refresh Cube Data</button>
    </div>
  </div>

  <div class="panel">
    <div class="row">
      <label>Time Level:</label>
      <select id="timeLevel">
        <option value="year">Year</option>
        <option value="month">Month</option>
        <option value="day">Day</option>
      </select>
      <button class="secondary" onclick="drillDown()">Drill Down</button>
      <button class="secondary" onclick="rollUp()">Roll Up</button>
    </div>
    <div class="row" style="margin-top:10px;">
      <input id="city" placeholder="Thanh pho (slice)" />
      <input id="product" placeholder="San pham (dice)" />
      <input id="customer" placeholder="Khach hang (dice)" />
      <button onclick="loadData()">Apply Slice / Dice</button>
      <button class="secondary" onclick="togglePivot()">Pivot</button>
    </div>
    <p class="muted">Flow: Sales theo Year -> Drill Down -> Slice city -> Dice product -> Pivot.</p>
  </div>

  <div class="panel">
    <div id="meta"></div>
    <div class="row" style="margin: 8px 0;">
      <button class="secondary" onclick="prevPage()">Prev</button>
      <button class="secondary" onclick="nextPage()">Next</button>
      <label>Page size:</label>
      <select id="pageSize" onchange="changePageSize()">
        <option value="25">25</option>
        <option value="50">50</option>
        <option value="100" selected>100</option>
      </select>
      <span id="pageInfo" class="muted"></span>
    </div>
    <div class="table-wrap">
      <table id="resultTable"></table>
    </div>
  </div>

<script>
let pivot = false;
let levels = ["year", "month", "day"];
let currentPage = 1;
let currentTotalPages = 1;

function drillDown() {
  const el = document.getElementById("timeLevel");
  let idx = levels.indexOf(el.value);
  if (idx < levels.length - 1) el.value = levels[idx + 1];
  loadData();
}

function rollUp() {
  const el = document.getElementById("timeLevel");
  let idx = levels.indexOf(el.value);
  if (idx > 0) el.value = levels[idx - 1];
  loadData();
}

function togglePivot() {
  pivot = !pivot;
  currentPage = 1;
  loadData();
}

function prevPage() {
  if (currentPage > 1) {
    currentPage -= 1;
    loadData();
  }
}

function nextPage() {
  if (currentPage < currentTotalPages) {
    currentPage += 1;
    loadData();
  }
}

function changePageSize() {
  currentPage = 1;
  loadData();
}

async function refreshCube() {
  const res = await fetch("/api/refresh-cube-data", { method: "POST" });
  const data = await res.json();
  alert(data.message);
}

function renderTable(payload) {
  const table = document.getElementById("resultTable");
  const columns = payload.columns || [];
  const rows = payload.rows || [];
  if (!columns.length) {
    table.className = "";
    table.innerHTML = "<tr><td>Khong co du lieu.</td></tr>";
    return;
  }

  const isPivot = columns[0] === "row";
  table.className = isPivot ? "pivot-table" : "";

  let html = "<thead><tr>" + columns.map(c => `<th>${c}</th>`).join("") + "</tr></thead><tbody>";
  for (const r of rows) {
    html += "<tr>" + columns.map((c, idx) => {
      const val = r[c] ?? "";
      const isNum = typeof val === "number";
      const niceVal = isNum ? val.toLocaleString("en-US", { maximumFractionDigits: 2 }) : val;
      const cls = [
        isNum ? "num" : "",
        isPivot && idx === 0 ? "pivot-row-key" : "",
      ].filter(Boolean).join(" ");
      return `<td class="${cls}">${niceVal}</td>`;
    }).join("") + "</tr>";
  }
  html += "</tbody>";
  table.innerHTML = html;
}

async function loadData() {
  try {
    const cube = document.getElementById("cubeType").value;
    const level = document.getElementById("timeLevel").value;
    const city = document.getElementById("city").value;
    const product = document.getElementById("product").value;
    const customer = document.getElementById("customer").value;
    const pageSize = document.getElementById("pageSize").value;
    const qs = new URLSearchParams({
      level,
      pivot: String(pivot),
      city,
      product,
      customer,
      page: String(currentPage),
      page_size: String(pageSize)
    });
    const res = await fetch(`/api/${cube}?` + qs.toString());
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`API ${res.status}: ${errText}`);
    }
    const data = await res.json();
    currentPage = data.page || currentPage;
    currentTotalPages = data.total_pages || 1;
    const pivotBadge = pivot ? '<span class="pivot-badge">Pivot Mode</span>' : '';
    document.getElementById("meta").innerHTML = `<b>${cube.toUpperCase()}</b> | level=${level} | total_rows=${data.total_rows ?? 0} ${pivotBadge}`;
    document.getElementById("pageInfo").innerText = `Page ${currentPage}/${currentTotalPages}`;
    renderTable(data);
  } catch (err) {
    document.getElementById("meta").innerHTML = `<span style="color:#b91c1c;">Loi tai API: ${err.message}</span>`;
    document.getElementById("resultTable").innerHTML = "";
  }
}

loadData();
</script>
</body>
</html>
"""


@app.post("/api/refresh-cube-data")
def refresh_cube_data() -> dict:
    load_dw_cube_data(server=DB_SERVER, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
    return {"message": "Da refresh cube data thanh cong."}


@app.get("/api/sales")
def sales_data(
    level: str = Query("year", pattern="^(year|month|day)$"),
    city: str = "",
    product: str = "",
    customer: str = "",
    pivot: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=100),
) -> dict:
    group_cols = ["t.year"]
    select_cols = ["t.year AS year"]
    if level in ("month", "day"):
        group_cols.append("t.month")
        select_cols.append("t.month AS month")
    if level == "day":
        group_cols.append("t.day")
        select_cols.append("t.day AS day")

    select_cols.extend(
        [
            "c.ten_thanh_pho AS city",
            "p.mo_ta AS product",
            "kh.ten_kh AS customer",
            "SUM(f.so_luong_dat) AS total_quantity",
            "SUM(f.tong_tien) AS total_sales",
        ]
    )
    group_cols.extend(["c.ten_thanh_pho", "p.mo_ta", "kh.ten_kh"])

    order_cols = ["year"]
    if level in ("month", "day"):
        order_cols.append("month")
    if level == "day":
        order_cols.append("day")
    order_cols.extend(["city", "product", "customer"])

    base_sql = f"""
        SELECT {", ".join(select_cols)}
        FROM dbo.fact_don_hang f
        JOIN dbo.dim_thoi_gian t ON f.date_key = t.date_key
        JOIN dbo.dim_cua_hang s ON f.store_key = s.store_key
        JOIN dbo.dim_thanh_pho c ON s.city_key = c.city_key
        JOIN dbo.dim_san_pham p ON f.product_key = p.product_key
        JOIN dbo.dim_khach_hang kh ON f.customer_key = kh.customer_key
        WHERE (%s = '' OR c.ten_thanh_pho LIKE %s)
          AND (%s = '' OR p.mo_ta LIKE %s)
          AND (%s = '' OR kh.ten_kh LIKE %s)
        GROUP BY {", ".join(group_cols)}
    """
    params = (
        city,
        f"%{city}%",
        product,
        f"%{product}%",
        customer,
        f"%{customer}%",
    )
    rows = _query_dw(
        base_sql,
        params,
    )
    if pivot:
        pivoted = _pivot_rows(rows, "product", "city", "total_sales")
        total_rows = len(pivoted["rows"])
        total_pages = (total_rows + page_size - 1) // page_size if total_rows else 1
        page = min(page, total_pages)
        start = (page - 1) * page_size
        end = start + page_size
        pivoted["rows"] = pivoted["rows"][start:end]
        pivoted["page"] = page
        pivoted["page_size"] = page_size
        pivoted["total_rows"] = total_rows
        pivoted["total_pages"] = total_pages
        return pivoted

    total_rows = len(rows)
    total_pages = (total_rows + page_size - 1) // page_size if total_rows else 1
    page = min(page, total_pages)
    offset = (page - 1) * page_size
    paged_sql = f"""
        SELECT * FROM ({base_sql}) q
        ORDER BY {", ".join(order_cols)}
        OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
    """
    paged_rows = _query_dw(
        paged_sql,
        (
            *params,
            offset,
            page_size,
        ),
    )
    return {
        "columns": list(paged_rows[0].keys()) if paged_rows else [],
        "rows": paged_rows,
        "page": page,
        "page_size": page_size,
        "total_rows": total_rows,
        "total_pages": total_pages,
    }


@app.get("/api/inventory")
def inventory_data(
    level: str = Query("month", pattern="^(year|month|day)$"),
    city: str = "",
    product: str = "",
    customer: str = "",
    pivot: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=100),
) -> dict:
    del customer, level
    sql = """
        SELECT
            c.ten_thanh_pho AS city,
            s.ma_cua_hang AS store,
            p.mo_ta AS product,
            SUM(f.so_luong_ton) AS total_inventory
        FROM dbo.fact_kho_hang f
        JOIN dbo.dim_cua_hang s ON f.store_key = s.store_key
        JOIN dbo.dim_thanh_pho c ON s.city_key = c.city_key
        JOIN dbo.dim_san_pham p ON f.product_key = p.product_key
        WHERE (%s = '' OR c.ten_thanh_pho LIKE %s)
          AND (%s = '' OR p.mo_ta LIKE %s)
        GROUP BY c.ten_thanh_pho, s.ma_cua_hang, p.mo_ta
        ORDER BY c.ten_thanh_pho, s.ma_cua_hang, p.mo_ta
    """
    rows = _query_dw(sql, (city, f"%{city}%", product, f"%{product}%"))
    if pivot:
        pivoted = _pivot_rows(rows, "product", "city", "total_inventory")
        total_rows = len(pivoted["rows"])
        total_pages = (total_rows + page_size - 1) // page_size if total_rows else 1
        page = min(page, total_pages)
        start = (page - 1) * page_size
        end = start + page_size
        pivoted["rows"] = pivoted["rows"][start:end]
        pivoted["page"] = page
        pivoted["page_size"] = page_size
        pivoted["total_rows"] = total_rows
        pivoted["total_pages"] = total_pages
        return pivoted
    total_rows = len(rows)
    total_pages = (total_rows + page_size - 1) // page_size if total_rows else 1
    page = min(page, total_pages)
    start = (page - 1) * page_size
    end = start + page_size
    paged_rows = rows[start:end]
    return {
        "columns": list(paged_rows[0].keys()) if paged_rows else [],
        "rows": paged_rows,
        "page": page,
        "page_size": page_size,
        "total_rows": total_rows,
        "total_pages": total_pages,
    }


RUNNING = True


def _handle_stop_signal(signum, frame):
    del signum, frame
    global RUNNING
    RUNNING = False


def wait_for_sql_server(
    server: str,
    user: str,
    password: str,
    port: int,
    retries: int = 30,
    delay_sec: int = 2,
) -> None:
    for attempt in range(1, retries + 1):
        try:
            conn = pymssql.connect(
                server=server,
                user=user,
                password=password,
                port=port,
                database="master",
                autocommit=True,
            )
            conn.close()
            print("Da ket noi duoc SQL Server.")
            return
        except Exception as ex:
            print(f"Thu {attempt}/{retries}: chua ket noi duoc SQL Server ({ex}).")
            time.sleep(delay_sec)

    raise RuntimeError("Khong the ket noi SQL Server sau nhieu lan thu.")

def check_merge_assumptions() -> None:
    conn = pymssql.connect(
        server="127.0.0.1",
        user="sa",
        password="YourStrong!Pass123",
        port=1434,
        database="master",
        autocommit=True,
    )
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    print("Check ket noi SQL Server: PASS")
    ### STEp 1viet theo kieu các hàm check rồi print pass hoặc fail ra .<Trong th này tất cả đều pass.>
    ##### B1 :Giai quyet cac truong hop khi merge data :
    ## 1 : Đồng nghĩa (Ko xuat hien trong DB) (Các field đều exact map chứ ko có đồng nghĩa) <Sửa cái thời gian đi . Nó xuất hiện khác nhau ở nhiều nơi>
    ## 2 : Giải quyết xung đột kiểu dữ liệu(Không tồn tại , Các field đều cùng thuộc tính và cùng tên , Và đã được resolve thành 1 quan hệ cụ thể nếu có .)
    ## 3 : Giải quyết xung đột khóa ( không xuất hiện trường hợp Customer(name FK) , Customer(id (FK)))(Mã Thành phố 1 bên là khóa chính 1 bên là khóa ngoại nhưng ko được coi là khóa ngoại .)
    ## 4: Giải quyết xung đột về lực lượng (Không xuất hiện trường hợp Customer với Order là 1 -n nhưng bên csdl kia lại là 1 -1 )
    ## 5: Giải quyết xung đột của thực thể yếu  (Không xuất hiện trường hợp Customer là thực thể mạnh bên(Thực thể có khóa chính) A nhưng Customer lại là thực thể yếu bên B )
    ## 6:Giải quyết xung đột trên thực thể kiểu con (Không có kiểu bên A thì Customer và TravelCustomer là quan hệ cha con nhưng sang bảng B thì ngược lại . )
    ### B2 : Trộn các thực thể <Không tồn tại vì các thực thể đều rất rõ nghĩa và ko dư thừa hay trùng . > 
    ## trong bài này trong Csdl representative_office_db.py có 3 thực thể : Customer , TravelCustomer , PostalCustomer tuy nhiên đã được chia theo case 3 : Trộn EER bằng quan hệ loại con trong đó Customer là cha của 2 thằng còn lại.
    ### B3 : Trộn các quan hệ <Không cần xử lý vì các quan hệ đã thống nhất không bị lệch chiều hoặc thiếu quan hệ . > 
    
    ### STEp 2 : chuyển đổi dữ liệu <Không cần vì các dữ liệu bọn em khi sinh ra ngày từ đầu đã chuẩn format . ko xuất hiện trường hợp delivery_date : 10/30/2025 và delivery_date : 30-10-2025> 
    ### Step 3 : Tích hợp dữ liệu .  <Giữ nguyên và không gộp Customer .>
    ### à có cần tạo ra 1 database mới chứa cả 2 ko nhỉ .Có nó chính là IDB
    ### STEP 4 : xác định bảng fact và bảng dim 
    ### step 5 : Viết logic để đổ dữ liệu từ bảng IDB vào DW(data warehouse ) . 
    #### <<Cần phải merge CustomerPostal và CustomerTravel vào Customer gọi là 1 bảng DimCustomer .>>
    
    conn.close()


def main(
    gen_seed_data: bool = False,
    should_merge_data: bool = False,
    should_build_dw: bool = False,
    should_load_idb_to_dw: bool = False,
    should_build_dw_cubes: bool = False,
    should_load_dw_cube_data: bool = False,
    should_seed_dw_demo: bool = False,
) -> None:
    server = "127.0.0.1"
    user = "sa"
    password = "YourStrong!Pass123"
    port = 1434

    signal.signal(signal.SIGINT, _handle_stop_signal)
    signal.signal(signal.SIGTERM, _handle_stop_signal)

    wait_for_sql_server(server=server, user=user, password=password, port=port)

    init_sell_db(server=server, user=user, password=password, port=port)
    init_and_seed_representative_office_db(
        server=server, user=user, password=password, port=port
    )

    print("Da tao CSDL ban hang + CSDL van phong dai dien va seed du lieu.")
    if gen_seed_data:
        print("Da bat flag gen_seed_data=True.")
    check_merge_assumptions()

    if should_merge_data:
        merge_ier_to_idbase(server=server, user=user, password=password, port=port)
        print("Da merge 2 IER vao IDBase.")

    if should_build_dw:
        build_dw_from_idbase(server=server, user=user, password=password, port=port)
        print("Da tao schema DW (DIM/FACT), chua do du lieu.")

    if should_load_idb_to_dw:
        load_idb_into_dw(server=server, user=user, password=password, port=port)
        print("Da do du lieu tu IDBase vao DW (Dim MERGE, Fact insert neu chua co).")

    if should_build_dw_cubes:
        ensure_dw_cube_tables(server=server, user=user, password=password, port=port)
        print("Da tao cac bang cube trong DWBase.")

    if should_load_dw_cube_data:
        load_dw_cube_data(server=server, user=user, password=password, port=port)
        print("Da nap du lieu cube vao DWBase (upsert, khong trung lap).")

    if should_seed_dw_demo:
        run_all_dw_demo_seed(server=server, user=user, password=password, port=port)
        print("Da chay seed demo: DIM + FACT + cube trong DWBase (khong can IDBase).")

    print("Ung dung dang chay. Nhan Ctrl+C de dung.")
    while RUNNING:
        time.sleep(1)

    print("Da dung ung dung.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tao CSDL ban hang va van phong dai dien.")
    parser.add_argument("--gen-seed-data", action="store_true", help="Tao du lieu mau.")
    
    ### neu ko thay nhet tham so vao khi chay thi vao loi 
##       them 1 flag la merge_data la true hoac false
    parser.add_argument("--merge-data", action="store_true", help="Merge 2 IER thanh IDBase.")
    parser.add_argument(
        "--build-dw",
        action="store_true",
        help="Tao DB moi dang DW gom cac bang DIM/FACT.",
    )
    parser.add_argument(
        "--load-dw-data",
        action="store_true",
        help="Do du lieu tu IDBase vao DW: Dim MERGE (update/insert), Fact chi them neu chua ton tai.",
    )
    parser.add_argument(
        "--build-dw-cubes",
        action="store_true",
        help="Tao cac bang cube tong hop trong DWBase.",
    )
    parser.add_argument(
        "--load-dw-cube-data",
        action="store_true",
        help="Nap du lieu vao cac bang cube trong DWBase.",
    )
    parser.add_argument(
        "--seed-dw-demo",
        action="store_true",
        help="Seed demo DWBase: dim/fact + agg cube (xoa du lieu DW cu trong bang seed, khong can IDBase).",
    )
    parser.add_argument(
        "--run-api",
        action="store_true",
        help="Chay FastAPI dashboard OLAP.",
    )
    args = parser.parse_args()
    if args.run_api:
        import uvicorn  # pyright: ignore[reportMissingImports]

        uvicorn.run("app:app", host="0.0.0.0", port=7999, reload=False)
        raise SystemExit(0)
    main(
        gen_seed_data=args.gen_seed_data,
        should_merge_data=args.merge_data,
        should_build_dw=args.build_dw,
        should_load_idb_to_dw=args.load_dw_data,
        should_build_dw_cubes=args.build_dw_cubes,
        should_load_dw_cube_data=args.load_dw_cube_data,
        should_seed_dw_demo=args.seed_dw_demo,
    )

