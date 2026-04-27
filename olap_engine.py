from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import pandas as pd
import pymssql


TIME_LEVEL_ORDER = ["year", "month", "day"]


@dataclass
class DwConnectionConfig:
    server: str
    user: str
    password: str
    port: int
    database: str = "DWBase"


class OlapSalesDataLoader:
    def __init__(self, config: DwConnectionConfig, ttl_sec: int = 300) -> None:
        self._config = config
        self._ttl_sec = ttl_sec
        self._lock = threading.Lock()
        self._cached_sales_city_day_df: pd.DataFrame | None = None
        self._loaded_sales_city_day_at: float = 0.0
        self._cached_sales_product_day_df: pd.DataFrame | None = None
        self._loaded_sales_product_day_at: float = 0.0
        self._cached_inventory_day_df: pd.DataFrame | None = None
        self._loaded_inventory_day_at: float = 0.0

    def invalidate_cache(self) -> None:
        with self._lock:
            self._cached_sales_city_day_df = None
            self._loaded_sales_city_day_at = 0.0
            self._cached_sales_product_day_df = None
            self._loaded_sales_product_day_at = 0.0
            self._cached_inventory_day_df = None
            self._loaded_inventory_day_at = 0.0

    def get_day_sales_df(self) -> pd.DataFrame:
        with self._lock:
            now = time.time()
            if (
                self._cached_sales_city_day_df is not None
                and (now - self._loaded_sales_city_day_at) <= self._ttl_sec
            ):
                return self._cached_sales_city_day_df.copy()

            conn = pymssql.connect(
                server=self._config.server,
                user=self._config.user,
                password=self._config.password,
                port=self._config.port,
                database=self._config.database,
                autocommit=True,
            )
            cursor = conn.cursor(as_dict=True)
            cursor.execute(
                """
                SELECT
                    a.year,
                    a.month,
                    a.day,
                    c.ten_thanh_pho AS city,
                    a.total_quantity,
                    a.total_sales
                FROM dbo.agg_sales_city_day a
                JOIN dbo.dim_thanh_pho c ON c.city_key = a.city_key
                """
            )
            rows = cursor.fetchall()
            conn.close()

            df = pd.DataFrame(rows)
            if df.empty:
                df = pd.DataFrame(columns=["year", "month", "day", "city", "total_quantity", "total_sales"])
            for col in ("year", "month", "day", "total_quantity", "total_sales"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            if "city" in df.columns:
                df["city"] = df["city"].fillna("").astype(str)
            df["period_month"] = df["year"].astype(int).astype(str) + "-" + df["month"].astype(int).astype(str).str.zfill(2)

            self._cached_sales_city_day_df = df
            self._loaded_sales_city_day_at = now
            return df.copy()

    def get_day_sales_product_df(self) -> pd.DataFrame:
        with self._lock:
            now = time.time()
            if (
                self._cached_sales_product_day_df is not None
                and (now - self._loaded_sales_product_day_at) <= self._ttl_sec
            ):
                return self._cached_sales_product_day_df.copy()

            conn = pymssql.connect(
                server=self._config.server,
                user=self._config.user,
                password=self._config.password,
                port=self._config.port,
                database=self._config.database,
                autocommit=True,
            )
            cursor = conn.cursor(as_dict=True)
            cursor.execute(
                """
                SELECT
                    t.year,
                    t.month,
                    t.day,
                    p.ma_mat_hang AS product_code,
                    p.mo_ta AS product_description,
                    SUM(f.so_luong_dat) AS total_quantity,
                    SUM(f.tong_tien) AS total_sales
                FROM dbo.fact_don_hang f
                JOIN dbo.dim_thoi_gian t ON t.date_key = f.date_key
                JOIN dbo.dim_san_pham p ON p.product_key = f.product_key
                GROUP BY
                    t.year,
                    t.month,
                    t.day,
                    p.ma_mat_hang,
                    p.mo_ta
                """
            )
            rows = cursor.fetchall()
            conn.close()

            df = pd.DataFrame(rows)
            if df.empty:
                df = pd.DataFrame(
                    columns=[
                        "year",
                        "month",
                        "day",
                        "product_code",
                        "product_description",
                        "total_quantity",
                        "total_sales",
                    ]
                )
            for col in ("year", "month", "day", "total_quantity", "total_sales"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            for col in ("product_code", "product_description"):
                if col in df.columns:
                    df[col] = df[col].fillna("").astype(str)
            df["period_month"] = (
                df["year"].astype(int).astype(str)
                + "-"
                + df["month"].astype(int).astype(str).str.zfill(2)
            )

            self._cached_sales_product_day_df = df
            self._loaded_sales_product_day_at = now
            return df.copy()

    def get_day_inventory_df(self) -> pd.DataFrame:
        with self._lock:
            now = time.time()
            if (
                self._cached_inventory_day_df is not None
                and (now - self._loaded_inventory_day_at) <= self._ttl_sec
            ):
                return self._cached_inventory_day_df.copy()

            conn = pymssql.connect(
                server=self._config.server,
                user=self._config.user,
                password=self._config.password,
                port=self._config.port,
                database=self._config.database,
                autocommit=True,
            )
            cursor = conn.cursor(as_dict=True)
            cursor.execute(
                """
                SELECT
                    t.year,
                    t.month,
                    t.day,
                    c.ten_thanh_pho AS city,
                    s.ma_cua_hang AS store_code,
                    p.ma_mat_hang AS product_code,
                    SUM(f.so_luong_ton) AS total_inventory
                FROM dbo.fact_kho_hang f
                JOIN dbo.dim_thoi_gian t ON t.date_key = f.date_key
                JOIN dbo.dim_cua_hang s ON s.store_key = f.store_key
                JOIN dbo.dim_thanh_pho c ON c.city_key = s.city_key
                JOIN dbo.dim_san_pham p ON p.product_key = f.product_key
                GROUP BY
                    t.year,
                    t.month,
                    t.day,
                    c.ten_thanh_pho,
                    s.ma_cua_hang,
                    p.ma_mat_hang
                """
            )
            rows = cursor.fetchall()
            conn.close()

            df = pd.DataFrame(rows)
            if df.empty:
                df = pd.DataFrame(
                    columns=[
                        "year",
                        "month",
                        "day",
                        "city",
                        "store_code",
                        "product_code",
                        "total_inventory",
                    ]
                )
            for col in ("year", "month", "day", "total_inventory"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            for col in ("city", "store_code", "product_code"):
                if col in df.columns:
                    df[col] = df[col].fillna("").astype(str)

            self._cached_inventory_day_df = df
            self._loaded_inventory_day_at = now
            return df.copy()


class SalesOlapProcessor:
    @staticmethod
    def apply_city_filter(df: pd.DataFrame, city_name: str) -> pd.DataFrame:
        if not city_name:
            return df
        return df[df["city"].str.contains(city_name, case=False, na=False)]

    @staticmethod
    def apply_product_code_filter(df: pd.DataFrame, product_code: str) -> pd.DataFrame:
        if not product_code:
            return df
        return df[df["product_code"] == product_code]

    @staticmethod
    def apply_store_code_filter(df: pd.DataFrame, store_code: str) -> pd.DataFrame:
        if not store_code:
            return df
        return df[df["store_code"] == store_code]

    @staticmethod
    def _effective_level(olap_operation: str, time_level: str) -> str:
        if olap_operation == "drill_down":
            return "month" if time_level == "year" else "day"
        if olap_operation == "roll_up":
            return "month" if time_level == "day" else "year"
        return time_level

    @staticmethod
    def _time_columns(level: str) -> list[str]:
        if level == "year":
            return ["year"]
        if level == "month":
            return ["year", "month"]
        return ["year", "month", "day"]

    @staticmethod
    def _to_records(df: pd.DataFrame) -> list[dict]:
        cleaned = df.where(pd.notnull(df), None)
        return cleaned.to_dict(orient="records")

    def q10(self, df: pd.DataFrame, city_name: str, olap_operation: str, time_level: str) -> list[dict]:
        filtered = self.apply_city_filter(df, city_name)

        if olap_operation == "pivot":
            month_sales = (
                filtered.groupby(["city", "period_month"], as_index=False)[["total_sales"]]
                .sum()
            )
            pivot_df = month_sales.pivot(index="city", columns="period_month", values="total_sales").fillna(0)
            pivot_df = pivot_df.sort_index(axis=0).sort_index(axis=1)
            pivot_df = pivot_df.reset_index().rename(columns={"city": "row"})
            return self._to_records(pivot_df)

        level = self._effective_level(olap_operation, time_level)
        dims = self._time_columns(level)
        grouped = filtered.groupby(dims, as_index=False)[["total_quantity", "total_sales"]].sum()
        grouped = grouped.sort_values(dims).reset_index(drop=True)

        if level in ("year", "month"):
            grouped["previous_period_sales"] = grouped["total_sales"].shift(1)
            grouped["sales_change"] = grouped["total_sales"] - grouped["previous_period_sales"].fillna(0)
            denom = grouped["previous_period_sales"].replace(0, pd.NA)
            grouped["sales_change_pct"] = ((grouped["sales_change"] / denom) * 100).round(2)

        return self._to_records(grouped)

    def q11(self, df: pd.DataFrame, city_name: str, olap_operation: str, time_level: str) -> list[dict]:
        filtered = self.apply_city_filter(df, city_name)

        if olap_operation == "pivot":
            month_sales = (
                filtered.groupby(["city", "period_month"], as_index=False)[["total_sales"]]
                .sum()
            )
            pivot_df = month_sales.pivot(index="city", columns="period_month", values="total_sales").fillna(0)
            pivot_df = pivot_df.sort_index(axis=0).sort_index(axis=1)
            pivot_df = pivot_df.reset_index().rename(columns={"city": "row"})
            return self._to_records(pivot_df)

        level = self._effective_level(olap_operation, time_level)
        dims = self._time_columns(level)
        grouped = (
            filtered.groupby([*dims, "city"], as_index=False)[["total_quantity", "total_sales"]]
            .sum()
            .sort_values([*dims, "total_sales", "city"], ascending=[*[True for _ in dims], False, True])
        )
        top_city = grouped.drop_duplicates(subset=dims, keep="first")
        top_city = top_city.sort_values(dims).reset_index(drop=True)
        return self._to_records(top_city)

    def q12(
        self,
        df: pd.DataFrame,
        product_code: str,
        olap_operation: str,
        time_level: str,
    ) -> list[dict]:
        filtered = self.apply_product_code_filter(df, product_code)

        if olap_operation == "pivot":
            month_qty = (
                filtered.groupby(["product_code", "period_month"], as_index=False)[["total_quantity"]]
                .sum()
            )
            pivot_df = month_qty.pivot(index="product_code", columns="period_month", values="total_quantity").fillna(0)
            pivot_df = pivot_df.sort_index(axis=0).sort_index(axis=1)
            pivot_df = pivot_df.reset_index().rename(columns={"product_code": "row"})
            return self._to_records(pivot_df)

        level = self._effective_level(olap_operation, time_level)
        dims = self._time_columns(level)
        grouped = (
            filtered.groupby([*dims, "product_code", "product_description"], as_index=False)[
                ["total_quantity", "total_sales"]
            ]
            .sum()
            .sort_values(
                [*dims, "total_quantity", "total_sales", "product_code"],
                ascending=[*[True for _ in dims], False, False, True],
            )
        )
        top_product = grouped.drop_duplicates(subset=dims, keep="first")
        top_product = top_product.sort_values(dims).reset_index(drop=True)
        return self._to_records(top_product)

    def q13(
        self,
        df: pd.DataFrame,
        city_name: str,
        product_code: str,
        olap_operation: str,
        time_level: str,
    ) -> list[dict]:
        filtered = self.apply_city_filter(df, city_name)
        filtered = self.apply_product_code_filter(filtered, product_code)

        if olap_operation == "pivot":
            pivot_source = (
                filtered.groupby(["city", "product_code"], as_index=False)[["total_inventory"]]
                .sum()
            )
            pivot_df = pivot_source.pivot(index="city", columns="product_code", values="total_inventory").fillna(0)
            pivot_df = pivot_df.sort_index(axis=0).sort_index(axis=1)
            pivot_df = pivot_df.reset_index().rename(columns={"city": "row"})
            return self._to_records(pivot_df)

        level = self._effective_level(olap_operation, time_level)
        dims = self._time_columns(level)
        grouped = (
            filtered.groupby([*dims, "city"], as_index=False)[["total_inventory"]]
            .sum()
            .sort_values([*dims, "total_inventory", "city"], ascending=[*[True for _ in dims], False, True])
        )
        top_city = grouped.drop_duplicates(subset=dims, keep="first")
        top_city = top_city.sort_values(dims).reset_index(drop=True)
        return self._to_records(top_city)

    def q14(
        self,
        df: pd.DataFrame,
        store_code: str,
        product_code: str,
        olap_operation: str,
        time_level: str,
    ) -> list[dict]:
        filtered = self.apply_store_code_filter(df, store_code)
        filtered = self.apply_product_code_filter(filtered, product_code)

        if olap_operation == "pivot":
            pivot_source = (
                filtered.groupby(["store_code", "product_code"], as_index=False)[["total_inventory"]]
                .sum()
            )
            pivot_df = pivot_source.pivot(index="store_code", columns="product_code", values="total_inventory").fillna(0)
            pivot_df = pivot_df.sort_index(axis=0).sort_index(axis=1)
            pivot_df = pivot_df.reset_index().rename(columns={"store_code": "row"})
            return self._to_records(pivot_df)

        level = self._effective_level(olap_operation, time_level)
        dims = self._time_columns(level)
        grouped = (
            filtered.groupby([*dims, "store_code", "city"], as_index=False)[["total_inventory"]]
            .sum()
            .sort_values(
                [*dims, "total_inventory", "store_code"],
                ascending=[*[True for _ in dims], False, True],
            )
        )
        top_store = grouped.drop_duplicates(subset=dims, keep="first")
        top_store = top_store.sort_values(dims).reset_index(drop=True)
        return self._to_records(top_store)


class SalesOlapService:
    def __init__(self, loader: OlapSalesDataLoader, processor: SalesOlapProcessor) -> None:
        self._loader = loader
        self._processor = processor

    def invalidate_cache(self) -> None:
        self._loader.invalidate_cache()

    def question_10(self, city_name: str, olap_operation: str, time_level: str) -> list[dict]:
        df = self._loader.get_day_sales_df()
        return self._processor.q10(df, city_name, olap_operation, time_level)

    def question_11(self, city_name: str, olap_operation: str, time_level: str) -> list[dict]:
        df = self._loader.get_day_sales_df()
        return self._processor.q11(df, city_name, olap_operation, time_level)

    def question_12(
        self,
        product_code: str,
        olap_operation: str,
        time_level: str,
    ) -> list[dict]:
        df = self._loader.get_day_sales_product_df()
        return self._processor.q12(df, product_code, olap_operation, time_level)

    def question_13(
        self,
        city_name: str,
        product_code: str,
        olap_operation: str,
        time_level: str,
    ) -> list[dict]:
        df = self._loader.get_day_inventory_df()
        return self._processor.q13(df, city_name, product_code, olap_operation, time_level)

    def question_14(
        self,
        store_code: str,
        product_code: str,
        olap_operation: str,
        time_level: str,
    ) -> list[dict]:
        df = self._loader.get_day_inventory_df()
        return self._processor.q14(df, store_code, product_code, olap_operation, time_level)


_sales_olap_service: SalesOlapService | None = None


def get_sales_olap_service(server: str, user: str, password: str, port: int) -> SalesOlapService:
    global _sales_olap_service
    if _sales_olap_service is None:
        cfg = DwConnectionConfig(server=server, user=user, password=password, port=port)
        _sales_olap_service = SalesOlapService(
            loader=OlapSalesDataLoader(cfg),
            processor=SalesOlapProcessor(),
        )
    return _sales_olap_service


def invalidate_sales_olap_cache() -> None:
    global _sales_olap_service
    if _sales_olap_service is not None:
        _sales_olap_service.invalidate_cache()
