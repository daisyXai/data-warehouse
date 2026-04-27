import signal
import time
from typing import Optional
from collections import defaultdict

import pymssql

from dw_builder import (
    build_dw_from_idbase,
    build_dw_metadata,
    ensure_dw_cube_tables,
    load_dw_cube_data,
    load_idb_into_dw,
)
from dw_seed_demo import run_all_dw_demo_seed
from idbase_merge import merge_ier_to_idbase
from olap_engine import get_sales_olap_service, invalidate_sales_olap_cache
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
  <title>Cube Q&A Dashboard</title>
  <style>
    :root {
      --bg: #f8fafc;
      --card: #ffffff;
      --line: #e2e8f0;
      --text: #0f172a;
      --muted: #475569;
      --primary: #2563eb;
      --secondary: #475569;
    }
    body { font-family: Arial, sans-serif; margin: 20px; background: var(--bg); color: var(--text); }
    .panel { background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 16px; margin-bottom: 14px; box-shadow: 0 2px 8px rgba(15,23,42,.05); }
    .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    select, input, button { padding: 8px 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; }
    input, select { background: #fff; min-width: 160px; }
    button { cursor: pointer; background: var(--primary); color: #fff; border: none; }
    button.secondary { background: var(--secondary); }
    button:disabled { opacity: .65; cursor: not-allowed; }
    .q-grid { display: grid; gap: 8px; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); }
    .q-btn { text-align: left; line-height: 1.35; padding: 10px 12px; }
    .q-btn.active { outline: 2px solid #93c5fd; background: #1d4ed8; }
    .table-wrap { margin-top: 12px; border: 1px solid var(--line); border-radius: 12px; overflow: auto; max-height: 66vh; background: #fff; }
    table { width: 100%; border-collapse: separate; border-spacing: 0; min-width: 900px; }
    th, td { border-bottom: 1px solid #e5e7eb; padding: 10px 12px; text-align: left; font-size: 14px; }
    th { background: #f1f5f9; position: sticky; top: 0; z-index: 1; }
    tbody tr:nth-child(even) td { background: #fafcff; }
    tbody tr:hover td { background: #eef6ff; }
    td.num { text-align: right; font-variant-numeric: tabular-nums; }
    .muted { color: var(--muted); font-size: 13px; }
    .badge { display: inline-block; padding: 4px 8px; border-radius: 999px; background: #dbeafe; color: #1e40af; font-size: 12px; margin-left: 6px; }
  </style>
</head>
<body>
  <h2>Cube Q&A Dashboard</h2>
  <div class="panel">
    <div class="row">
      <label>Data source:</label>
      <span class="badge">4 aggregate cube tables</span>
      <button onclick="refreshCube()">Refresh Cube Data</button>
    </div>
    <p class="muted">Click a question below to run immediately. Fill optional filters then click Run Selected Question.</p>
  </div>

  <div class="panel">
    <div class="q-grid" id="questionGrid"></div>
    <div class="row" id="dynamicFilters" style="margin-top:12px;">
      <button onclick="runSelectedQuestion()">Run Selected Question</button>
    </div>
    <p class="muted">Q2, Q5, Q8 and Q9 use cube-oriented approximations because aggregate tables do not store full order-line details.</p>
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
const QUESTIONS = [
  "1) Find stores with city/state/phone plus sold products (description, size, weight, list price).",
  "2) Find customer orders with customer name and order date (cube approximation by customer + month).",
  "3) Find stores (with city/phone and order date) selling a product ordered by a specific customer.",
  "4) Find representative-office address, city, state for stores holding a product above a given inventory level.",
  "5) For each customer order, list products with store code, city and stores that can sell those products (cube approximation).",
  "6) Find city and state where a given customer lives.",
  "7) Find inventory level of a specific product across all stores in a specific city.",
  "8) Find products, ordered quantity, customer, store and city of an order (cube approximation by customer/product).",
  "9) Find travel customers, postal customers and customers belonging to both types.",
  "10) (agg_sales_city_day) How does sales change over time.",
  "11) (agg_sales_city_month) Thanh pho nao co doanh thu cao nhat theo tung thang.",
  "12) (agg_sales_product_month) San pham nao ban chay theo thoi gian.",
  "13) (agg_inventory_city_product) Thanh pho nao dang ton kho cao nhat.",
  "14) (agg_inventory_store_product) Cua hang nao dang ton kho cao nhat."
];

let selectedQuestion = 1;
let currentPage = 1;
let currentTotalPages = 1;
const TIME_LEVEL_ORDER = ["year", "month", "day"];
let q10CurrentLevel = "year";
let q10PendingOperation = "slice_dice";
let q11CurrentLevel = "year";
let q11PendingOperation = "slice_dice";
let q12CurrentLevel = "year";
let q12PendingOperation = "slice_dice";
let q13CurrentLevel = "year";
let q13PendingOperation = "slice_dice";
let q14CurrentLevel = "year";
let q14PendingOperation = "slice_dice";
const OLAP_PAGE_SIZE = 200;
const olapFrontendState = {
  10: { rawRows: null, path: [], filterKey: "{}", filters: {} },
  11: { rawRows: null, path: [], filterKey: "{}", filters: {} },
  12: { rawRows: null, path: [], filterKey: "{}", filters: {} },
  13: { rawRows: null, path: [], filterKey: "{}", filters: {} },
  14: { rawRows: null, path: [], filterKey: "{}", filters: {} },
};
const FILTER_FIELDS = {
  store_code: {
    label: "Store code",
    placeholder: "Store code (e.g. CH001)",
    type: "text",
  },
  customer_code: {
    label: "Customer code",
    placeholder: "Customer code (e.g. KH001)",
    type: "text",
  },
  order_code: {
    label: "Order code",
    placeholder: "Order code (e.g. DH001)",
    type: "text",
  },
  product_code: {
    label: "Product code",
    placeholder: "Product code (e.g. MH001)",
    type: "text",
  },
  city_name: {
    label: "City name",
    placeholder: "City name",
    type: "text",
  },
  min_inventory: {
    label: "Min inventory",
    placeholder: "Min inventory (Q4)",
    type: "number",
    min: "0",
  },
  customer_type: {
    label: "Customer type",
    type: "select",
    options: [
      { value: "", text: "All customer types" },
      { value: "buu_dien", text: "buu_dien" },
      { value: "du_lich", text: "du_lich" },
      { value: "ca_hai", text: "ca_hai" },
    ],
  },
  olap_operation: {
    label: "OLAP operation",
    type: "select",
    options: [
      { value: "roll_up", text: "roll up" },
      { value: "drill_down", text: "drill down" },
      { value: "slice_dice", text: "slice & dice" },
      { value: "pivot", text: "pivot" },
    ],
  },
  time_level: {
    label: "Time level",
    type: "select",
    options: [
      { value: "year", text: "year" },
      { value: "month", text: "month" },
      { value: "day", text: "day" },
    ],
  },
};
const QUESTION_FILTERS = {
  1: ["store_code", "city_name", "product_code"],
  2: ["customer_code", "order_code"],
  3: ["customer_code", "product_code"],
  4: ["min_inventory", "product_code"],
  5: ["customer_code", "order_code", "product_code"],
  6: ["customer_code"],
  7: ["city_name", "product_code"],
  8: ["customer_code", "order_code", "product_code"],
  9: ["customer_type"],
  10: ["olap_operation", "time_level", "city_name"],
  11: ["olap_operation", "time_level", "city_name"],
  12: ["olap_operation", "time_level", "product_code"],
  13: ["olap_operation", "city_name", "product_code"],
  14: ["olap_operation", "store_code", "product_code"],
};

function initQuestionButtons() {
  const host = document.getElementById("questionGrid");
  host.innerHTML = QUESTIONS.map((q, i) => (
    `<button class="q-btn ${i === 0 ? "active" : ""}" id="qbtn-${i + 1}" onclick="quickRun(${i + 1})">${q}</button>`
  )).join("");
}

function setSelectedQuestion(qid) {
  selectedQuestion = qid;
  if (qid === 10) {
    q10CurrentLevel = "year";
    q10PendingOperation = "slice_dice";
    olapFrontendState[10].path = [];
  }
  if (qid === 11) {
    q11CurrentLevel = "year";
    q11PendingOperation = "slice_dice";
    olapFrontendState[11].path = [];
  }
  if (qid === 12) {
    q12CurrentLevel = "year";
    q12PendingOperation = "slice_dice";
    olapFrontendState[12].path = [];
  }
  if (qid === 13) {
    q13CurrentLevel = "year";
    q13PendingOperation = "slice_dice";
    olapFrontendState[13].path = [];
  }
  if (qid === 14) {
    q14CurrentLevel = "year";
    q14PendingOperation = "slice_dice";
    olapFrontendState[14].path = [];
  }
  for (let i = 1; i <= QUESTIONS.length; i++) {
    document.getElementById(`qbtn-${i}`).classList.toggle("active", i === qid);
  }
  renderFiltersForQuestion();
  currentPage = 1;
}

function quickRun(qid) {
  setSelectedQuestion(qid);
  runSelectedQuestion();
}

function prevPage() {
  if (currentPage > 1) {
    currentPage -= 1;
    runSelectedQuestion();
  }
}

function nextPage() {
  if (currentPage < currentTotalPages) {
    currentPage += 1;
    runSelectedQuestion();
  }
}

function changePageSize() {
  currentPage = 1;
  runSelectedQuestion();
}

function renderFiltersForQuestion() {
  const host = document.getElementById("dynamicFilters");
  const filterKeys = QUESTION_FILTERS[selectedQuestion] || [];
  if (selectedQuestion === 10) {
    const q10State = olapFrontendState[10];
    const q10Crumbs = ["All time", ...q10State.path.map((p) => p.label)];
    const q10Breadcrumb = q10Crumbs.map((label, idx) =>
      `<a href="#" onclick="olapBreadcrumbClick(10, ${idx}); return false;">${label}</a>`
    ).join(" &gt; ");
    const q10CityValue = q10State.filters.city_name || "";
    host.innerHTML = `
      <input data-filter-key="city_name" type="text" placeholder="City name" value="${q10CityValue}" />
      <span class="muted">Current level: <b>${q10CurrentLevel}</b></span>
      <span class="muted">Path: ${q10Breadcrumb}</span>
      <span class="muted">Click a row to drill down</span>
      <button class="secondary" onclick="resetOlapPath(10)">Reset</button>
    `;
    return;
  }
  if (selectedQuestion === 11) {
    const q11State = olapFrontendState[11];
    const q11Crumbs = ["All time", ...q11State.path.map((p) => p.label)];
    const q11Breadcrumb = q11Crumbs.map((label, idx) =>
      `<a href="#" onclick="olapBreadcrumbClick(11, ${idx}); return false;">${label}</a>`
    ).join(" &gt; ");
    const q11CityValue = q11State.filters.city_name || "";
    host.innerHTML = `
      <input data-filter-key="city_name" type="text" placeholder="City name" value="${q11CityValue}" />
      <span class="muted">Current level: <b>${q11CurrentLevel}</b></span>
      <span class="muted">Path: ${q11Breadcrumb}</span>
      <span class="muted">Click a row to drill down</span>
      <button class="secondary" onclick="resetOlapPath(11)">Reset</button>
    `;
    return;
  }
  if (selectedQuestion === 12) {
    const q12State = olapFrontendState[12];
    const q12Crumbs = ["All time", ...q12State.path.map((p) => p.label)];
    const q12Breadcrumb = q12Crumbs.map((label, idx) =>
      `<a href="#" onclick="olapBreadcrumbClick(12, ${idx}); return false;">${label}</a>`
    ).join(" &gt; ");
    const q12ProductValue = q12State.filters.product_code || "";
    host.innerHTML = `
      <input data-filter-key="product_code" type="text" placeholder="Product code (e.g. MH001)" value="${q12ProductValue}" />
      <span class="muted">Current level: <b>${q12CurrentLevel}</b></span>
      <span class="muted">Path: ${q12Breadcrumb}</span>
      <span class="muted">Click a row to drill down</span>
      <button class="secondary" onclick="resetOlapPath(12)">Reset</button>
    `;
    return;
  }
  if (selectedQuestion === 13) {
    const q13State = olapFrontendState[13];
    const q13Crumbs = ["All time", ...q13State.path.map((p) => p.label)];
    const q13Breadcrumb = q13Crumbs.map((label, idx) =>
      `<a href="#" onclick="olapBreadcrumbClick(13, ${idx}); return false;">${label}</a>`
    ).join(" &gt; ");
    const q13CityValue = q13State.filters.city_name || "";
    const q13ProductValue = q13State.filters.product_code || "";
    host.innerHTML = `
      <input data-filter-key="city_name" type="text" placeholder="City name" value="${q13CityValue}" />
      <input data-filter-key="product_code" type="text" placeholder="Product code (e.g. MH001)" value="${q13ProductValue}" />
      <span class="muted">Current level: <b>${q13CurrentLevel}</b></span>
      <span class="muted">Path: ${q13Breadcrumb}</span>
      <span class="muted">Click a row to drill down</span>
      <button class="secondary" onclick="resetOlapPath(13)">Reset</button>
    `;
    return;
  }
  if (selectedQuestion === 14) {
    const q14State = olapFrontendState[14];
    const q14Crumbs = ["All time", ...q14State.path.map((p) => p.label)];
    const q14Breadcrumb = q14Crumbs.map((label, idx) =>
      `<a href="#" onclick="olapBreadcrumbClick(14, ${idx}); return false;">${label}</a>`
    ).join(" &gt; ");
    const q14StoreValue = q14State.filters.store_code || "";
    const q14ProductValue = q14State.filters.product_code || "";
    host.innerHTML = `
      <input data-filter-key="store_code" type="text" placeholder="Store code (e.g. CH001)" value="${q14StoreValue}" />
      <input data-filter-key="product_code" type="text" placeholder="Product code (e.g. MH001)" value="${q14ProductValue}" />
      <span class="muted">Current level: <b>${q14CurrentLevel}</b></span>
      <span class="muted">Path: ${q14Breadcrumb}</span>
      <span class="muted">Click a row to drill down</span>
      <button class="secondary" onclick="resetOlapPath(14)">Reset</button>
    `;
    return;
  }
  const inputsHtml = filterKeys.map((key) => {
    const cfg = FILTER_FIELDS[key];
    if (!cfg) {
      return "";
    }
    if (cfg.type === "select") {
      const optionsHtml = (cfg.options || []).map(
        (opt) => `<option value="${opt.value}">${opt.text}</option>`
      ).join("");
      return `<select data-filter-key="${key}">${optionsHtml}</select>`;
    }
    const minAttr = cfg.min ? ` min="${cfg.min}"` : "";
    return `<input data-filter-key="${key}" type="${cfg.type}" placeholder="${cfg.placeholder}"${minAttr} />`;
  }).join("");
  const hint = filterKeys.length
    ? ""
    : '<span class="muted">Question nay khong yeu cau bo loc dau vao.</span>';
  host.innerHTML = `${inputsHtml}${hint}<button onclick="runSelectedQuestion()">Run Selected Question</button>`;
}

function runQ10AtCurrentLevel() {
  q10PendingOperation = "slice_dice";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ10DrillDown() {
  if (q10CurrentLevel === "day") {
    return;
  }
  q10PendingOperation = "drill_down";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ10RollUp() {
  if (q10CurrentLevel === "year") {
    return;
  }
  q10PendingOperation = "roll_up";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ11AtCurrentLevel() {
  q11PendingOperation = "slice_dice";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ11DrillDown() {
  if (q11CurrentLevel === "day") {
    return;
  }
  q11PendingOperation = "drill_down";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ11RollUp() {
  if (q11CurrentLevel === "year") {
    return;
  }
  q11PendingOperation = "roll_up";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ12AtCurrentLevel() {
  q12PendingOperation = "slice_dice";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ12DrillDown() {
  if (q12CurrentLevel === "day") {
    return;
  }
  q12PendingOperation = "drill_down";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ12RollUp() {
  if (q12CurrentLevel === "year") {
    return;
  }
  q12PendingOperation = "roll_up";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ13AtCurrentLevel() {
  q13PendingOperation = "slice_dice";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ13DrillDown() {
  if (q13CurrentLevel === "day") {
    return;
  }
  q13PendingOperation = "drill_down";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ13RollUp() {
  if (q13CurrentLevel === "year") {
    return;
  }
  q13PendingOperation = "roll_up";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ14AtCurrentLevel() {
  q14PendingOperation = "slice_dice";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ14DrillDown() {
  if (q14CurrentLevel === "day") {
    return;
  }
  q14PendingOperation = "drill_down";
  currentPage = 1;
  runSelectedQuestion();
}

function runQ14RollUp() {
  if (q14CurrentLevel === "year") {
    return;
  }
  q14PendingOperation = "roll_up";
  currentPage = 1;
  runSelectedQuestion();
}

async function refreshCube() {
  const res = await fetch("/api/refresh-cube-data", { method: "POST" });
  const data = await res.json();
  for (const qid of [10, 11, 12, 13, 14]) {
    olapFrontendState[qid].rawRows = null;
    olapFrontendState[qid].path = [];
  }
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
  const isOlapInteractive = (selectedQuestion >= 10 && selectedQuestion <= 14) && !isPivot;
  const currentLevelByQuestion = {
    10: q10CurrentLevel,
    11: q11CurrentLevel,
    12: q12CurrentLevel,
    13: q13CurrentLevel,
    14: q14CurrentLevel,
  };
  const canDrillMore = isOlapInteractive && currentLevelByQuestion[selectedQuestion] !== "day";
  table.className = isPivot ? "pivot-table" : "";

  let html = "<thead><tr>" + columns.map(c => `<th>${c}</th>`).join("") + "</tr></thead><tbody>";
  rows.forEach((r, rowIdx) => {
    const rowClickAttr = canDrillMore ? ` onclick="handleOlapRowClick(${rowIdx})" style="cursor:pointer;" title="Click to drill down"` : "";
    html += `<tr${rowClickAttr}>` + columns.map((c, idx) => {
      const val = r[c] ?? "";
      const isNum = typeof val === "number";
      const niceVal = isNum ? val.toLocaleString("en-US", { maximumFractionDigits: 2 }) : val;
      const cls = [
        isNum ? "num" : "",
        isPivot && idx === 0 ? "pivot-row-key" : "",
      ].filter(Boolean).join(" ");
      return `<td class="${cls}">${niceVal}</td>`;
    }).join("") + "</tr>";
  });
  html += "</tbody>";
  table.innerHTML = html;
}

function collectFilters() {
  const filters = {
    page: String(currentPage),
    page_size: document.getElementById("pageSize").value,
  };
  const inputEls = document.querySelectorAll("#dynamicFilters [data-filter-key]");
  for (const el of inputEls) {
    const key = el.getAttribute("data-filter-key");
    if (!key) {
      continue;
    }
    const value = el.value.trim();
    if (value !== "") {
      filters[key] = value;
    }
  }
  if (selectedQuestion === 10) {
    filters.time_level = q10CurrentLevel;
    filters.olap_operation = q10PendingOperation;
  }
  if (selectedQuestion === 11) {
    filters.time_level = q11CurrentLevel;
    filters.olap_operation = q11PendingOperation;
  }
  if (selectedQuestion === 12) {
    filters.time_level = q12CurrentLevel;
    filters.olap_operation = q12PendingOperation;
  }
  if (selectedQuestion === 13) {
    filters.time_level = q13CurrentLevel;
    filters.olap_operation = q13PendingOperation;
  }
  if (selectedQuestion === 14) {
    filters.time_level = q14CurrentLevel;
    filters.olap_operation = q14PendingOperation;
  }
  return filters;
}

function getOlapLevelFromPath(path) {
  if (path.length === 0) return "year";
  if (path.length === 1) return "month";
  return "day";
}

function aggregateQ10(rows, level) {
  const grouped = new Map();
  for (const r of rows) {
    const year = Number(r.year);
    const month = Number(r.month);
    const day = Number(r.day);
    const key = level === "year" ? `${year}` : level === "month" ? `${year}-${month}` : `${year}-${month}-${day}`;
    const rec = grouped.get(key) || { year, month, day, total_quantity: 0, total_sales: 0 };
    rec.total_quantity += Number(r.total_quantity || 0);
    rec.total_sales += Number(r.total_sales || 0);
    grouped.set(key, rec);
  }
  const out = Array.from(grouped.values()).sort((a, b) =>
    a.year - b.year || (a.month || 0) - (b.month || 0) || (a.day || 0) - (b.day || 0)
  );
  if (level !== "day") {
    let prev = null;
    for (const rec of out) {
      rec.previous_period_sales = prev;
      rec.sales_change = rec.total_sales - (prev ?? 0);
      rec.sales_change_pct = prev && prev !== 0 ? Number((((rec.total_sales - prev) / prev) * 100).toFixed(2)) : null;
      prev = rec.total_sales;
    }
  }
  if (level === "year") return out.map(({ year, total_quantity, total_sales, previous_period_sales, sales_change, sales_change_pct }) => ({ year, total_quantity, total_sales, previous_period_sales, sales_change, sales_change_pct }));
  if (level === "month") return out.map(({ year, month, total_quantity, total_sales, previous_period_sales, sales_change, sales_change_pct }) => ({ year, month, total_quantity, total_sales, previous_period_sales, sales_change, sales_change_pct }));
  return out.map(({ year, month, day, total_quantity, total_sales }) => ({ year, month, day, total_quantity, total_sales }));
}

function aggregateQ11(rows, level) {
  const grouped = new Map();
  for (const r of rows) {
    const year = Number(r.year);
    const month = Number(r.month);
    const day = Number(r.day);
    const city = String(r.city || "");
    const keyPrefix = level === "year" ? `${year}` : level === "month" ? `${year}-${month}` : `${year}-${month}-${day}`;
    const key = `${keyPrefix}|${city}`;
    const rec = grouped.get(key) || { year, month, day, city, total_quantity: 0, total_sales: 0 };
    rec.total_quantity += Number(r.total_quantity || 0);
    rec.total_sales += Number(r.total_sales || 0);
    grouped.set(key, rec);
  }
  const winnerByPeriod = new Map();
  for (const rec of grouped.values()) {
    const periodKey = level === "year" ? `${rec.year}` : level === "month" ? `${rec.year}-${rec.month}` : `${rec.year}-${rec.month}-${rec.day}`;
    const current = winnerByPeriod.get(periodKey);
    if (!current || rec.total_sales > current.total_sales || (rec.total_sales === current.total_sales && rec.city < current.city)) {
      winnerByPeriod.set(periodKey, rec);
    }
  }
  const out = Array.from(winnerByPeriod.values()).sort((a, b) =>
    a.year - b.year || (a.month || 0) - (b.month || 0) || (a.day || 0) - (b.day || 0)
  );
  if (level === "year") return out.map(({ year, city, total_quantity, total_sales }) => ({ year, city, total_quantity, total_sales }));
  if (level === "month") return out.map(({ year, month, city, total_quantity, total_sales }) => ({ year, month, city, total_quantity, total_sales }));
  return out.map(({ year, month, day, city, total_quantity, total_sales }) => ({ year, month, day, city, total_quantity, total_sales }));
}

function aggregateQ12(rows, level) {
  const grouped = new Map();
  for (const r of rows) {
    const year = Number(r.year);
    const month = Number(r.month);
    const day = Number(r.day);
    const product_code = String(r.product_code || "");
    const product_description = String(r.product_description || "");
    const keyPrefix = level === "year" ? `${year}` : level === "month" ? `${year}-${month}` : `${year}-${month}-${day}`;
    const key = `${keyPrefix}|${product_code}`;
    const rec = grouped.get(key) || { year, month, day, product_code, product_description, total_quantity: 0, total_sales: 0 };
    rec.total_quantity += Number(r.total_quantity || 0);
    rec.total_sales += Number(r.total_sales || 0);
    grouped.set(key, rec);
  }
  const winnerByPeriod = new Map();
  for (const rec of grouped.values()) {
    const periodKey = level === "year" ? `${rec.year}` : level === "month" ? `${rec.year}-${rec.month}` : `${rec.year}-${rec.month}-${rec.day}`;
    const current = winnerByPeriod.get(periodKey);
    if (
      !current ||
      rec.total_quantity > current.total_quantity ||
      (rec.total_quantity === current.total_quantity && rec.total_sales > current.total_sales) ||
      (rec.total_quantity === current.total_quantity && rec.total_sales === current.total_sales && rec.product_code < current.product_code)
    ) {
      winnerByPeriod.set(periodKey, rec);
    }
  }
  const out = Array.from(winnerByPeriod.values()).sort((a, b) =>
    a.year - b.year || (a.month || 0) - (b.month || 0) || (a.day || 0) - (b.day || 0)
  );
  if (level === "year") return out.map(({ year, product_code, product_description, total_quantity, total_sales }) => ({ year, product_code, product_description, total_quantity, total_sales }));
  if (level === "month") return out.map(({ year, month, product_code, product_description, total_quantity, total_sales }) => ({ year, month, product_code, product_description, total_quantity, total_sales }));
  return out.map(({ year, month, day, product_code, product_description, total_quantity, total_sales }) => ({ year, month, day, product_code, product_description, total_quantity, total_sales }));
}

function aggregateQ13(rows, level) {
  const grouped = new Map();
  for (const r of rows) {
    const year = Number(r.year);
    const month = Number(r.month);
    const day = Number(r.day);
    const city = String(r.city || "");
    const keyPrefix = level === "year" ? `${year}` : level === "month" ? `${year}-${month}` : `${year}-${month}-${day}`;
    const key = `${keyPrefix}|${city}`;
    const rec = grouped.get(key) || { year, month, day, city, total_inventory: 0 };
    rec.total_inventory += Number(r.total_inventory || 0);
    grouped.set(key, rec);
  }
  const winnerByPeriod = new Map();
  for (const rec of grouped.values()) {
    const periodKey = level === "year" ? `${rec.year}` : level === "month" ? `${rec.year}-${rec.month}` : `${rec.year}-${rec.month}-${rec.day}`;
    const current = winnerByPeriod.get(periodKey);
    if (!current || rec.total_inventory > current.total_inventory || (rec.total_inventory === current.total_inventory && rec.city < current.city)) {
      winnerByPeriod.set(periodKey, rec);
    }
  }
  const out = Array.from(winnerByPeriod.values()).sort((a, b) =>
    a.year - b.year || (a.month || 0) - (b.month || 0) || (a.day || 0) - (b.day || 0)
  );
  if (level === "year") return out.map(({ year, city, total_inventory }) => ({ year, city, total_inventory }));
  if (level === "month") return out.map(({ year, month, city, total_inventory }) => ({ year, month, city, total_inventory }));
  return out.map(({ year, month, day, city, total_inventory }) => ({ year, month, day, city, total_inventory }));
}

function aggregateQ14(rows, level) {
  const grouped = new Map();
  for (const r of rows) {
    const year = Number(r.year);
    const month = Number(r.month);
    const day = Number(r.day);
    const store_code = String(r.store_code || "");
    const city = String(r.city || "");
    const keyPrefix = level === "year" ? `${year}` : level === "month" ? `${year}-${month}` : `${year}-${month}-${day}`;
    const key = `${keyPrefix}|${store_code}|${city}`;
    const rec = grouped.get(key) || { year, month, day, store_code, city, total_inventory: 0 };
    rec.total_inventory += Number(r.total_inventory || 0);
    grouped.set(key, rec);
  }
  const winnerByPeriod = new Map();
  for (const rec of grouped.values()) {
    const periodKey = level === "year" ? `${rec.year}` : level === "month" ? `${rec.year}-${rec.month}` : `${rec.year}-${rec.month}-${rec.day}`;
    const current = winnerByPeriod.get(periodKey);
    if (!current || rec.total_inventory > current.total_inventory || (rec.total_inventory === current.total_inventory && rec.store_code < current.store_code)) {
      winnerByPeriod.set(periodKey, rec);
    }
  }
  const out = Array.from(winnerByPeriod.values()).sort((a, b) =>
    a.year - b.year || (a.month || 0) - (b.month || 0) || (a.day || 0) - (b.day || 0)
  );
  if (level === "year") return out.map(({ year, store_code, city, total_inventory }) => ({ year, store_code, city, total_inventory }));
  if (level === "month") return out.map(({ year, month, store_code, city, total_inventory }) => ({ year, month, store_code, city, total_inventory }));
  return out.map(({ year, month, day, store_code, city, total_inventory }) => ({ year, month, day, store_code, city, total_inventory }));
}

function applyOlapContext(rows, path, filters) {
  return rows.filter((r) => {
    if (filters.city_name && !String(r.city || "").toLowerCase().includes(filters.city_name.toLowerCase())) {
      return false;
    }
    if (filters.product_code && String(r.product_code || "") !== filters.product_code) {
      return false;
    }
    if (filters.store_code && String(r.store_code || "") !== filters.store_code) {
      return false;
    }
    if (path[0] && Number(r.year) !== Number(path[0].year)) return false;
    if (path[1] && Number(r.month) !== Number(path[1].month)) return false;
    return true;
  });
}

function getOlapFiltersFromInputs(questionId) {
  const filterOf = (key) => {
    const input = document.querySelector(`#dynamicFilters [data-filter-key="${key}"]`);
    return input ? input.value.trim() : "";
  };
  if (questionId === 10 || questionId === 11) {
    return { city_name: filterOf("city_name") };
  }
  if (questionId === 12) {
    return { product_code: filterOf("product_code") };
  }
  if (questionId === 13) {
    return { city_name: filterOf("city_name"), product_code: filterOf("product_code") };
  }
  if (questionId === 14) {
    return { store_code: filterOf("store_code"), product_code: filterOf("product_code") };
  }
  return {};
}

async function loadAllRowsForOlapQuestion(questionId, filters) {
  let page = 1;
  let totalPages = 1;
  const rows = [];
  do {
    const baseParams = {
      page: String(page),
      page_size: String(OLAP_PAGE_SIZE),
      time_level: "day",
      olap_operation: "slice_dice",
    };
    const params = { ...baseParams, ...filters };
    const qs = new URLSearchParams(params);
    const res = await fetch(`/api/question/${questionId}?` + qs.toString());
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`API ${res.status}: ${errText}`);
    }
    const data = await res.json();
    rows.push(...(data.rows || []));
    totalPages = Number(data.total_pages || 1);
    page += 1;
  } while (page <= totalPages);
  return rows;
}

async function ensureOlapRawRows(questionId, filters) {
  const state = olapFrontendState[questionId];
  const filterKey = JSON.stringify(filters || {});
  if (state.rawRows && state.filterKey === filterKey) {
    state.filters = { ...(filters || {}) };
    return;
  }
  state.rawRows = await loadAllRowsForOlapQuestion(questionId, filters || {});
  state.filterKey = filterKey;
  state.filters = { ...(filters || {}) };
  state.path = [];
}

function olapBreadcrumbClick(questionId, crumbIndex) {
  const state = olapFrontendState[questionId];
  state.path = state.path.slice(0, Math.max(crumbIndex - 1, -1) + 1);
  currentPage = 1;
  runSelectedQuestion();
}

function resetOlapPath(questionId) {
  olapFrontendState[questionId].path = [];
  currentPage = 1;
  runSelectedQuestion();
}

function handleOlapRowClick(rowIndex) {
  if (selectedQuestion < 10 || selectedQuestion > 14) {
    return;
  }
  const state = olapFrontendState[selectedQuestion];
  const level = getOlapLevelFromPath(state.path);
  if (level === "day") {
    return;
  }
  const payload = window.__lastOlapPayload;
  if (!payload || !payload.rows || !payload.rows[rowIndex]) {
    return;
  }
  const row = payload.rows[rowIndex];
  if (level === "year") {
    state.path = [{ year: Number(row.year), label: String(row.year) }];
    if (selectedQuestion === 10) q10CurrentLevel = "month";
    if (selectedQuestion === 11) q11CurrentLevel = "month";
    if (selectedQuestion === 12) q12CurrentLevel = "month";
    if (selectedQuestion === 13) q13CurrentLevel = "month";
    if (selectedQuestion === 14) q14CurrentLevel = "month";
  } else if (level === "month") {
    state.path = [
      ...state.path.slice(0, 1),
      { month: Number(row.month), label: `M${String(row.month).padStart(2, "0")}` },
    ];
    if (selectedQuestion === 10) q10CurrentLevel = "day";
    if (selectedQuestion === 11) q11CurrentLevel = "day";
    if (selectedQuestion === 12) q12CurrentLevel = "day";
    if (selectedQuestion === 13) q13CurrentLevel = "day";
    if (selectedQuestion === 14) q14CurrentLevel = "day";
  }
  currentPage = 1;
  renderFiltersForQuestion();
  runSelectedQuestion();
}

async function runSelectedQuestion() {
  try {
    if (selectedQuestion >= 10 && selectedQuestion <= 14) {
      const filters = getOlapFiltersFromInputs(selectedQuestion);
      await ensureOlapRawRows(selectedQuestion, filters);
      const state = olapFrontendState[selectedQuestion];
      const level = getOlapLevelFromPath(state.path);
      if (selectedQuestion === 10) q10CurrentLevel = level;
      if (selectedQuestion === 11) q11CurrentLevel = level;
      if (selectedQuestion === 12) q12CurrentLevel = level;
      if (selectedQuestion === 13) q13CurrentLevel = level;
      if (selectedQuestion === 14) q14CurrentLevel = level;

      const contextualRows = applyOlapContext(state.rawRows || [], state.path, filters);
      let rows = [];
      if (selectedQuestion === 10) rows = aggregateQ10(contextualRows, level);
      if (selectedQuestion === 11) rows = aggregateQ11(contextualRows, level);
      if (selectedQuestion === 12) rows = aggregateQ12(contextualRows, level);
      if (selectedQuestion === 13) rows = aggregateQ13(contextualRows, level);
      if (selectedQuestion === 14) rows = aggregateQ14(contextualRows, level);
      const totalRows = rows.length;
      const pageSize = Number(document.getElementById("pageSize").value || "100");
      const totalPages = totalRows ? Math.ceil(totalRows / pageSize) : 1;
      currentPage = Math.min(currentPage, totalPages);
      const start = (currentPage - 1) * pageSize;
      const pagedRows = rows.slice(start, start + pageSize);
      const payload = {
        columns: pagedRows.length ? Object.keys(pagedRows[0]) : [],
        rows: pagedRows,
        page: currentPage,
        page_size: pageSize,
        total_rows: totalRows,
        total_pages: totalPages,
      };
      window.__lastOlapPayload = payload;
      document.getElementById("meta").innerHTML = `<b>Question ${selectedQuestion}</b> | total_rows=${payload.total_rows ?? 0}`;
      document.getElementById("pageInfo").innerText = `Page ${payload.page}/${payload.total_pages}`;
      renderFiltersForQuestion();
      renderTable(payload);
      return;
    }

    const qs = new URLSearchParams(collectFilters());
    const res = await fetch(`/api/question/${selectedQuestion}?` + qs.toString());
    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`API ${res.status}: ${errText}`);
    }
    const data = await res.json();
    if (selectedQuestion === 10) {
      if (q10PendingOperation === "drill_down") {
        const idx = TIME_LEVEL_ORDER.indexOf(q10CurrentLevel);
        q10CurrentLevel = TIME_LEVEL_ORDER[Math.min(idx + 1, TIME_LEVEL_ORDER.length - 1)];
      } else if (q10PendingOperation === "roll_up") {
        const idx = TIME_LEVEL_ORDER.indexOf(q10CurrentLevel);
        q10CurrentLevel = TIME_LEVEL_ORDER[Math.max(idx - 1, 0)];
      }
      q10PendingOperation = "slice_dice";
      renderFiltersForQuestion();
    }
    if (selectedQuestion === 11) {
      if (q11PendingOperation === "drill_down") {
        const idx = TIME_LEVEL_ORDER.indexOf(q11CurrentLevel);
        q11CurrentLevel = TIME_LEVEL_ORDER[Math.min(idx + 1, TIME_LEVEL_ORDER.length - 1)];
      } else if (q11PendingOperation === "roll_up") {
        const idx = TIME_LEVEL_ORDER.indexOf(q11CurrentLevel);
        q11CurrentLevel = TIME_LEVEL_ORDER[Math.max(idx - 1, 0)];
      }
      q11PendingOperation = "slice_dice";
      renderFiltersForQuestion();
    }
    if (selectedQuestion === 12) {
      if (q12PendingOperation === "drill_down") {
        const idx = TIME_LEVEL_ORDER.indexOf(q12CurrentLevel);
        q12CurrentLevel = TIME_LEVEL_ORDER[Math.min(idx + 1, TIME_LEVEL_ORDER.length - 1)];
      } else if (q12PendingOperation === "roll_up") {
        const idx = TIME_LEVEL_ORDER.indexOf(q12CurrentLevel);
        q12CurrentLevel = TIME_LEVEL_ORDER[Math.max(idx - 1, 0)];
      }
      q12PendingOperation = "slice_dice";
      renderFiltersForQuestion();
    }
    if (selectedQuestion === 13) {
      if (q13PendingOperation === "drill_down") {
        const idx = TIME_LEVEL_ORDER.indexOf(q13CurrentLevel);
        q13CurrentLevel = TIME_LEVEL_ORDER[Math.min(idx + 1, TIME_LEVEL_ORDER.length - 1)];
      } else if (q13PendingOperation === "roll_up") {
        const idx = TIME_LEVEL_ORDER.indexOf(q13CurrentLevel);
        q13CurrentLevel = TIME_LEVEL_ORDER[Math.max(idx - 1, 0)];
      }
      q13PendingOperation = "slice_dice";
      renderFiltersForQuestion();
    }
    if (selectedQuestion === 14) {
      if (q14PendingOperation === "drill_down") {
        const idx = TIME_LEVEL_ORDER.indexOf(q14CurrentLevel);
        q14CurrentLevel = TIME_LEVEL_ORDER[Math.min(idx + 1, TIME_LEVEL_ORDER.length - 1)];
      } else if (q14PendingOperation === "roll_up") {
        const idx = TIME_LEVEL_ORDER.indexOf(q14CurrentLevel);
        q14CurrentLevel = TIME_LEVEL_ORDER[Math.max(idx - 1, 0)];
      }
      q14PendingOperation = "slice_dice";
      renderFiltersForQuestion();
    }
    currentPage = data.page || currentPage;
    currentTotalPages = data.total_pages || 1;
    document.getElementById("meta").innerHTML = `<b>Question ${selectedQuestion}</b> | total_rows=${data.total_rows ?? 0}`;
    document.getElementById("pageInfo").innerText = `Page ${currentPage}/${currentTotalPages}`;
    renderTable(data);
  } catch (err) {
    document.getElementById("meta").innerHTML = `<span style="color:#b91c1c;">API error: ${err.message}</span>`;
    document.getElementById("resultTable").innerHTML = "";
  }
}

initQuestionButtons();
runSelectedQuestion();
</script>
</body>
</html>
"""


@app.post("/api/refresh-cube-data")
def refresh_cube_data() -> dict:
    load_dw_cube_data(server=DB_SERVER, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
    invalidate_sales_olap_cache()
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
            "kh.ten_khach_hang AS customer",
            "SUM(f.so_luong_dat) AS total_quantity",
            "SUM(f.tong_tien) AS total_sales",
        ]
    )
    group_cols.extend(["c.ten_thanh_pho", "p.mo_ta", "kh.ten_khach_hang"])

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
          AND (%s = '' OR kh.ten_khach_hang LIKE %s)
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


def _to_table_payload(rows: list[dict], page: int, page_size: int) -> dict:
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


@app.get("/api/question/{question_id}")
def quick_question(
    question_id: int,
    customer_code: str = "",
    order_code: str = "",
    store_code: str = "",
    product_code: str = "",
    city_name: str = "",
    customer_type: str = Query("", pattern="^(|buu_dien|du_lich|ca_hai)$"),
    olap_operation: str = Query("roll_up", pattern="^(roll_up|drill_down|slice_dice|pivot)$"),
    time_level: str = Query("month", pattern="^(year|month|day)$"),
    min_inventory: Optional[int] = Query(None, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=200),
) -> dict:
    min_inventory_value = min_inventory if min_inventory is not None else 0

    if question_id == 1:
        sql = """
            SELECT
                s.ma_cua_hang AS store_code,
                c.ten_thanh_pho AS city,
                c.bang AS state,
                s.so_dien_thoai AS phone,
                p.mo_ta AS product_description,
                p.kich_co AS size,
                p.trong_luong AS weight,
                p.gia_niem_yet AS list_price
            FROM dbo.agg_inventory_store_product aisp
            JOIN dbo.dim_cua_hang s ON s.store_key = aisp.store_key
            JOIN dbo.dim_thanh_pho c ON c.city_key = s.city_key
            JOIN dbo.dim_san_pham p ON p.product_key = aisp.product_key
            WHERE (%s = '' OR s.ma_cua_hang = %s)
              AND (%s = '' OR c.ten_thanh_pho LIKE %s)
              AND (%s = '' OR p.ma_mat_hang = %s)
            ORDER BY s.ma_cua_hang, p.mo_ta
        """
        rows = _query_dw(
            sql,
            (store_code, store_code, city_name, f"%{city_name}%", product_code, product_code),
        )
        return _to_table_payload(rows, page, page_size)

    if question_id == 2:
        sql = """
            SELECT
                f.ma_don_hang AS order_code,
                kh.ma_khach_hang AS customer_code,
                kh.ten_khach_hang AS customer_name,
                t.full_date AS order_date
            FROM dbo.fact_don_hang f
            JOIN dbo.dim_khach_hang kh ON kh.customer_key = f.customer_key
            JOIN dbo.dim_thoi_gian t ON t.date_key = f.date_key
            WHERE (%s = '' OR kh.ma_khach_hang = %s)
              AND (%s = '' OR f.ma_don_hang = %s)
            ORDER BY kh.ma_khach_hang, t.full_date, f.ma_don_hang
        """
        rows = _query_dw(sql, (customer_code, customer_code, order_code, order_code))
        return _to_table_payload(rows, page, page_size)

    if question_id == 3:
        sql = """
            SELECT DISTINCT
                kh.ma_khach_hang AS customer_code,
                kh.ten_khach_hang AS customer_name,
                p.ma_mat_hang AS product_code,
                t.full_date AS order_date,
                s.ma_cua_hang AS store_code,
                c.ten_thanh_pho AS city,
                s.so_dien_thoai AS phone
            FROM dbo.fact_don_hang f_customer
            JOIN dbo.dim_khach_hang kh ON kh.customer_key = f_customer.customer_key
            JOIN dbo.dim_san_pham p ON p.product_key = f_customer.product_key
            JOIN dbo.dim_thoi_gian t ON t.date_key = f_customer.date_key
            JOIN dbo.agg_inventory_store_product aisp ON aisp.product_key = p.product_key
            JOIN dbo.dim_cua_hang s ON s.store_key = aisp.store_key
            JOIN dbo.dim_thanh_pho c ON c.city_key = s.city_key
            WHERE (%s = '' OR kh.ma_khach_hang = %s)
              AND (%s = '' OR p.ma_mat_hang = %s)
            ORDER BY kh.ma_khach_hang, t.full_date, p.ma_mat_hang, s.ma_cua_hang
        """
        rows = _query_dw(sql, (customer_code, customer_code, product_code, product_code))
        return _to_table_payload(rows, page, page_size)

    if question_id == 4:
        sql = """
            SELECT
                c.dia_chi_vp AS representative_office_address,
                c.ten_thanh_pho AS city,
                c.bang AS state,
                s.ma_cua_hang AS store_code,
                p.ma_mat_hang AS product_code,
                p.mo_ta AS product_description,
                aisp.total_inventory
            FROM dbo.agg_inventory_store_product aisp
            JOIN dbo.dim_cua_hang s ON s.store_key = aisp.store_key
            JOIN dbo.dim_thanh_pho c ON c.city_key = s.city_key
            JOIN dbo.dim_san_pham p ON p.product_key = aisp.product_key
            WHERE aisp.total_inventory >= %s
              AND (%s = '' OR p.ma_mat_hang = %s)
            ORDER BY aisp.total_inventory DESC, s.ma_cua_hang
        """
        rows = _query_dw(sql, (min_inventory_value, product_code, product_code))
        return _to_table_payload(rows, page, page_size)

    if question_id == 5:
        sql = """
            WITH order_items AS (
                SELECT
                    f.ma_don_hang AS order_code,
                    kh.ma_khach_hang AS customer_code,
                    kh.ten_khach_hang AS customer_name,
                    p.product_key,
                    p.ma_mat_hang AS product_code,
                    p.mo_ta AS product_description
                FROM dbo.fact_don_hang f
                JOIN dbo.dim_khach_hang kh ON kh.customer_key = f.customer_key
                JOIN dbo.dim_san_pham p ON p.product_key = f.product_key
                WHERE (%s = '' OR kh.ma_khach_hang = %s)
                  AND (%s = '' OR f.ma_don_hang = %s)
                  AND (%s = '' OR p.ma_mat_hang = %s)
            )
            SELECT
                oi.order_code,
                oi.customer_code,
                oi.customer_name,
                oi.product_code,
                oi.product_description,
                s.ma_cua_hang AS store_code,
                c.ten_thanh_pho AS city,
                aisp.total_inventory AS inventory_available
            FROM order_items oi
            JOIN dbo.agg_inventory_store_product aisp ON aisp.product_key = oi.product_key
            JOIN dbo.dim_cua_hang s ON s.store_key = aisp.store_key
            JOIN dbo.dim_thanh_pho c ON c.city_key = s.city_key
            ORDER BY oi.order_code, oi.product_code, s.ma_cua_hang
        """
        rows = _query_dw(
            sql,
            (customer_code, customer_code, order_code, order_code, product_code, product_code),
        )
        return _to_table_payload(rows, page, page_size)

    if question_id == 6:
        sql = """
            SELECT
                kh.ma_khach_hang AS customer_code,
                kh.ten_khach_hang AS customer_name,
                c.ten_thanh_pho AS city,
                c.bang AS state
            FROM dbo.dim_khach_hang kh
            JOIN dbo.dim_thanh_pho c ON c.city_key = kh.city_key
            WHERE (%s = '' OR kh.ma_khach_hang = %s)
            ORDER BY kh.ma_khach_hang
        """
        rows = _query_dw(sql, (customer_code, customer_code))
        return _to_table_payload(rows, page, page_size)

    if question_id == 7:
        sql = """
            SELECT
                c.ten_thanh_pho AS city,
                p.ma_mat_hang AS product_code,
                p.mo_ta AS product_description,
                s.ma_cua_hang AS store_code,
                aisp.total_inventory
            FROM dbo.agg_inventory_store_product aisp
            JOIN dbo.dim_cua_hang s ON s.store_key = aisp.store_key
            JOIN dbo.dim_thanh_pho c ON c.city_key = s.city_key
            JOIN dbo.dim_san_pham p ON p.product_key = aisp.product_key
            WHERE (%s = '' OR c.ten_thanh_pho LIKE %s)
              AND (%s = '' OR p.ma_mat_hang = %s)
            ORDER BY c.ten_thanh_pho, p.ma_mat_hang, s.ma_cua_hang
        """
        rows = _query_dw(sql, (city_name, f"%{city_name}%", product_code, product_code))
        return _to_table_payload(rows, page, page_size)

    if question_id == 8:
        sql = """
            SELECT
                f.ma_don_hang AS order_code,
                kh.ma_khach_hang AS customer_code,
                kh.ten_khach_hang AS customer_name,
                p.ma_mat_hang AS product_code,
                p.mo_ta AS product_description,
                s.ma_cua_hang AS store_code,
                c.ten_thanh_pho AS city,
                f.so_luong_dat AS ordered_quantity,
                f.gia_dat AS ordered_price,
                f.tong_tien AS total_sales
            FROM dbo.fact_don_hang f
            JOIN dbo.dim_khach_hang kh ON kh.customer_key = f.customer_key
            JOIN dbo.dim_san_pham p ON p.product_key = f.product_key
            LEFT JOIN dbo.dim_cua_hang s ON s.store_key = f.store_key
            LEFT JOIN dbo.dim_thanh_pho c ON c.city_key = s.city_key
            WHERE (%s = '' OR kh.ma_khach_hang = %s)
              AND (%s = '' OR f.ma_don_hang = %s)
              AND (%s = '' OR p.ma_mat_hang = %s)
            ORDER BY f.ma_don_hang, p.ma_mat_hang
        """
        rows = _query_dw(
            sql,
            (customer_code, customer_code, order_code, order_code, product_code, product_code),
        )
        return _to_table_payload(rows, page, page_size)

    if question_id == 9:
        sql = """
            SELECT
                ma_khach_hang AS customer_code,
                ten_khach_hang AS customer_name,
                customer_type
            FROM dbo.dim_khach_hang
            WHERE customer_type IN ('du_lich', 'buu_dien', 'ca_hai')
              AND (%s = '' OR customer_type = %s)
            ORDER BY customer_type, ma_khach_hang
        """
        rows = _query_dw(sql, (customer_type, customer_type))
        return _to_table_payload(rows, page, page_size)

    if question_id == 10:
        sales_olap_service = get_sales_olap_service(
            server=DB_SERVER, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        rows = sales_olap_service.question_10(
            city_name=city_name,
            olap_operation=olap_operation,
            time_level=time_level,
        )
        return _to_table_payload(rows, page, page_size)

    if question_id == 11:
        sales_olap_service = get_sales_olap_service(
            server=DB_SERVER, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        rows = sales_olap_service.question_11(
            city_name=city_name,
            olap_operation=olap_operation,
            time_level=time_level,
        )
        return _to_table_payload(rows, page, page_size)

    if question_id == 12:
        sales_olap_service = get_sales_olap_service(
            server=DB_SERVER, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        rows = sales_olap_service.question_12(
            product_code=product_code,
            olap_operation=olap_operation,
            time_level=time_level,
        )
        return _to_table_payload(rows, page, page_size)

    if question_id == 13:
        sales_olap_service = get_sales_olap_service(
            server=DB_SERVER, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        rows = sales_olap_service.question_13(
            city_name=city_name,
            product_code=product_code,
            olap_operation=olap_operation,
            time_level=time_level,
        )
        return _to_table_payload(rows, page, page_size)

    if question_id == 14:
        sales_olap_service = get_sales_olap_service(
            server=DB_SERVER, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        rows = sales_olap_service.question_14(
            store_code=store_code,
            product_code=product_code,
            olap_operation=olap_operation,
            time_level=time_level,
        )
        return _to_table_payload(rows, page, page_size)

    return {"columns": [], "rows": [], "page": 1, "page_size": page_size, "total_rows": 0, "total_pages": 1}


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
    should_build_metadata: bool = False,
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

    if should_build_metadata:
        build_dw_metadata(server=server, user=user, password=password, port=port)
        print("Da tao bang metadata_tables, metadata_columns va seed metadata trong DWBase.")

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
    parser.add_argument(
        "--build-metadata",
        action="store_true",
        help="Tao metadata_tables + metadata_columns va seed cho 5 bang dim, 2 bang fact trong DWBase.",
    )
    args = parser.parse_args()
    #### specific add 

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
        should_build_metadata=args.build_metadata,
    )

