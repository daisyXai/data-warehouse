# OLAP Demo - KhoDuLieu B1

Project nay demo full flow tu CSDL nghiep vu den dashboard OLAP:

- `SellDB` + `RepresentativeOfficeDB` (2 nguon du lieu ban dau)
- Merge thanh `IDBase` (integration database)
- Build `DWBase` (DIM/FACT + cube aggregate)
- Xem tren FastAPI dashboard (FE HTML/CSS + API query)

## 1) Cau truc file chinh

- `app.py`: entrypoint chinh + FastAPI server + dashboard FE.
- `sell_db.py`: tao schema CSDL ban hang (`SellDB`).
- `representative_office_db.py`: tao + seed CSDL van phong dai dien (`RepresentativeOfficeDB`).
- `idbase_merge.py`: merge 2 IER thanh `IDBase`.
- `dw_builder.py`: tao DW schema, load IDB -> DW, tao/load cube.
- `dw_seed_demo.py`: seed demo truc tiep vao `DWBase` (dim/fact/cube) de test nhanh.
- `docker-compose.yml`: SQL Server container.

## 2) Yeu cau moi truong

- Python 3.12+
- Docker + Docker Compose
- SQL Server container tu `docker-compose.yml`
- Virtual env `.venv`

Cai package:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Neu chua co `requirements.txt` day du, can dam bao it nhat co:

- `pymssql`
- `fastapi`
- `uvicorn`

## 3) Khoi dong SQL Server

```bash
docker compose up -d
```

SQL Server mac dinh trong source:

- host: `127.0.0.1`
- port: `1434`
- user: `sa`
- password: `YourStrong!Pass123`

## 4) Cac cach chay quan trong

Tat ca lenh deu chay tai root project.

### A. Tao 2 CSDL nguon (SellDB + RepresentativeOfficeDB)

```bash
python app.py
```

### B. Merge 2 IER thanh IDBase

```bash
python app.py --merge-data
```

### C. Tao schema DW (DIM/FACT)

```bash
python app.py --build-dw
```

### D. Do du lieu tu IDBase vao DW

```bash
python app.py --load-dw-data
```

### E. Tao bang cube aggregate

```bash
python app.py --build-dw-cubes
```

### F. Nap du lieu cube

```bash
python app.py --load-dw-cube-data
```

### G. Seed demo nhanh DW (khong can IDBase)

Lenh nay se seed DIM + FACT + cube truc tiep vao `DWBase`.

```bash
python app.py --seed-dw-demo
```

### H. Chay dashboard FastAPI

```bash
python app.py --run-api
```

Mo trinh duyet:

- http://localhost:8000

## 5) Flow chay de demo nhanh cho nguoi moi

Neu muon co data dep de show dashboard:

```bash
python app.py --seed-dw-demo
python app.py --run-api
```

Neu muon dung dung flow day du IER -> IDB -> DW:

```bash
python app.py --merge-data --build-dw --load-dw-data --build-dw-cubes --load-dw-cube-data
python app.py --run-api
```

## 6) Huong dan su dung FE (dashboard)

Tai trang `/`:

1. Chon cube:
   - `Sales Cube`
   - `Inventory Cube`
2. Dung filter (slice/dice):
   - Thanh pho
   - San pham
   - Khach hang
3. Dung thao tac OLAP:
   - `Drill Down`: Year -> Month -> Day
   - `Roll Up`: Day -> Month -> Year
   - `Pivot`: xoay bang (hang/cot)
4. Dung pagination:
   - `Prev` / `Next`
   - `Page size` (toi da 100 dong/trang)
5. Neu cube chua moi:
   - bam `Refresh Cube Data`

## 7) API chinh (de test Postman/browser)

- `POST /api/refresh-cube-data`
- `GET /api/sales`
  - params: `level`, `city`, `product`, `customer`, `pivot`, `page`, `page_size`
- `GET /api/inventory`
  - params: `city`, `product`, `pivot`, `page`, `page_size`

Vi du:

```text
/api/sales?level=month&city=Ha%20Noi&pivot=false&page=1&page_size=100
/api/sales?level=year&pivot=true&page=1&page_size=50
/api/inventory?city=Da%20Nang&product=San%20pham%201&pivot=false&page=1&page_size=100
```

## 8) Luu y khi su dung

- Moi lan chay `--seed-dw-demo` se lam moi du lieu demo trong DW.
- Neu FE hien loi API, xem log terminal dang chay `--run-api`.
- Neu doi code FE/API ma thay doi khong len trang, restart server + hard refresh browser.

  10.1.1.223
