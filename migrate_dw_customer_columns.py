"""
Migrate DWBase.dim_khach_hang column names:
- ma_kh -> ma_khach_hang
- ten_kh -> ten_khach_hang

Safe for running on existing DB with data.
Idempotent: if already migrated, script exits successfully.
"""

import argparse
import sys

import pymssql


def _connect(server: str, user: str, password: str, port: int, database: str) -> pymssql.Connection:
    return pymssql.connect(
        server=server,
        user=user,
        password=password,
        port=port,
        database=database,
        autocommit=False,
    )


def _table_exists(cursor: pymssql.Cursor, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = 'dbo' AND t.name = %s
        """,
        (table_name,),
    )
    return cursor.fetchone() is not None


def _column_exists(cursor: pymssql.Cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM sys.columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = 'dbo'
          AND t.name = %s
          AND c.name = %s
        """,
        (table_name, column_name),
    )
    return cursor.fetchone() is not None


def migrate_customer_columns(
    server: str = "127.0.0.1",
    user: str = "sa",
    password: str = "YourStrong!Pass123",
    port: int = 1434,
    database: str = "DWBase",
) -> None:
    conn = _connect(server, user, password, port, database)
    cursor = conn.cursor()

    try:
        if not _table_exists(cursor, "dim_khach_hang"):
            raise RuntimeError("Khong tim thay dbo.dim_khach_hang trong DB dich.")

        has_old_code = _column_exists(cursor, "dim_khach_hang", "ma_kh")
        has_old_name = _column_exists(cursor, "dim_khach_hang", "ten_kh")
        has_new_code = _column_exists(cursor, "dim_khach_hang", "ma_khach_hang")
        has_new_name = _column_exists(cursor, "dim_khach_hang", "ten_khach_hang")

        # Already migrated.
        if has_new_code and has_new_name and not has_old_code and not has_old_name:
            print("Schema da o trang thai moi. Khong can migrate.")
            conn.commit()
            return

        # Legacy schema, ready to rename.
        if has_old_code and has_old_name and not has_new_code and not has_new_name:
            cursor.execute("EXEC sp_rename 'dbo.dim_khach_hang.ma_kh', 'ma_khach_hang', 'COLUMN';")
            cursor.execute("EXEC sp_rename 'dbo.dim_khach_hang.ten_kh', 'ten_khach_hang', 'COLUMN';")
            conn.commit()
            print("Migrate thanh cong: ma_kh/ten_kh -> ma_khach_hang/ten_khach_hang.")
            return

        # Mixed state: stop to avoid damaging existing schema/data.
        raise RuntimeError(
            "Schema dang o trang thai trung gian (co ca cot cu va moi). "
            "Can xu ly thu cong truoc khi tiep tuc."
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate DWBase dim_khach_hang column names to standardized format."
    )
    parser.add_argument("--server", default="127.0.0.1")
    parser.add_argument("--user", default="sa")
    parser.add_argument("--password", default="YourStrong!Pass123")
    parser.add_argument("--port", type=int, default=1434)
    parser.add_argument("--database", default="DWBase")
    args = parser.parse_args()

    try:
        migrate_customer_columns(
            server=args.server,
            user=args.user,
            password=args.password,
            port=args.port,
            database=args.database,
        )
        return 0
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
