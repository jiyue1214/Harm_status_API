import os, sys
import sqlite3
from typing import Any, Callable, Dict, List
from ftplib import FTP

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

class DataExtractor:
    def __init__(self, ftp_url: str, db_path: str = "temp.db", table_name: str = "studies"):
        self.ftp_url = ftp_url
        self.db_path = db_path
        self.table_name = table_name
        
        self.ensure_local_copy()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

 # --------------------------- FTP Handling ---------------------------
    def ensure_local_copy(self):
        if not os.path.exists(self.db_path):
            print(f"[INFO] DB file not found locally. Downloading from FTP...")
            self._download_db_file()
        else:
            print(f"[INFO] Using cached DB file at {self.db_path}")

    def _download_db_file(self):
        try:
            stripped = self.ftp_url.replace("ftp://", "")
            host, *path_parts = stripped.split("/")
            file_path = "/" + "/".join(path_parts[:-1])
            filename = path_parts[-1]
            
            ftp = FTP(host, timeout=30)
            ftp.login()
            ftp.cwd(file_path)
            
            with open(self.db_path, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
            
            ftp.quit()
            print(f"Downloaded {filename} to {self.db_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to download DB from FTP: {e}")

# --------------------------- Query Builders ---------------------------
    def is_number(self, value):
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
        
    def build_where_clause(self,filter_str):
        """
        Takes a raw string like:
            "Harm_drop_rate!=0.8&Another_field>=5&Description~rare"
        Returns:
            SQL-safe WHERE clause
        """
        if not filter_str:
            return ""
    
        filters_str = filter_str.strip('"')
        conditions = []
        valid_ops = ["!=", ">=", "<=", "=", ">", "<", "~"]
    
        filter_parts = filters_str.split(";")

        for expr in filter_parts:
            expr = expr.strip()
            for op in valid_ops:
                if op in expr:
                    field, value = expr.split(op, 1)
                    field, value = field.strip(), value.strip()
    
                    if op in [">=", "<=", ">", "<"]:
                        conditions.append(f"{field} != 'NA'")
                    
                    if op == "~":
                        conditions.append(f"{field} LIKE '%{value}%'")
                    elif self.is_number(value):
                        conditions.append(f"{field} {op} {value}")
                    else:
                        conditions.append(f"{field} {op} '{value}'")
                    break
    
        return " AND ".join(conditions)


# --------------------------- Core Query Methods ---------------------------

    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Helper method to execute a SQL query and return results as a list of dictionaries."""
        try:
            cur = self.conn.cursor()
            cur.execute(query, params)
            results = cur.fetchall()
            return [dict(row) for row in results]
        except sqlite3.Error as e:
            raise RuntimeError(f"[DB ERROR] {e}")

    def extract_all(self) -> Dict:
        """Extract all rows from the table."""
        query = f"SELECT * FROM {self.table_name}"
        return self._execute_query(query,())
        
    def extract_by_column(self, column_name: str, value: Any) -> List[Dict[str, Any]]:
        """Extract rows where the column matches the given value.
        extractor.extract_by_column("chromosome", "1")
        """
        query = f"SELECT * FROM {self.table_name} WHERE {column_name} = ?"
        return self._execute_query(query, (value,))

    def extract_by_range(self, column_name: str, min_value: Any, max_value: Any) -> List[Dict[str, Any]]:
        """Extract rows where the column value is within a specified range.
        extractor.extract_by_range("position", 10000, 60000)
        """
        query = f"SELECT * FROM {self.table_name} WHERE {column_name} BETWEEN ? AND ?"
        return self._execute_query(query, (min_value, max_value))

    def extract_by_regex(self, column_name: str, pattern: str) -> List[Dict[str, Any]]:
        """
        Extract rows where the column matches the given regular expression.
        e.g. extractor.extract_by_regex("ref", "^[AC]$")
        """
        query = f"SELECT * FROM {self.table_name} WHERE {column_name} REGEXP ?"
        return self._execute_query(query, (pattern,))

    def extract_by_custom_function(self, column_name: str, func: Callable[[Any], bool]) -> List[Dict[str, Any]]:
        """
        Extract rows where the column values satisfy a custom function.
        e.g. print(extractor.extract_by_custom_function("position", lambda x: x % 2 == 0))
        """
        all_data = self._execute_query(f"SELECT * FROM {self.table_name}")
        return [row for row in all_data if func(row[column_name])]
    
    
    def extract_by_custom_query(self, conditions: dict) -> List[Dict[str, Any]]:
        """Extract rows using a raw SQL WHERE clause.
        e.g. query = "harmType_x != 'not_harm' AND exitcode > 1"
        """
        where_clause = self.build_where_clause(conditions)
        print(where_clause)
        query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"
        return self._execute_query(query,())
    
    def extract_columns(self,columns_name:list, where_clause: str) -> List[Dict[str, Any]]:
        """
        Extract a subset using a raw SQL WHERE clause
        """
        columns=" , ".join(name for name in columns_name)
        query=f"SELECT {columns} FROM {self.table_name} WHERE {where_clause}"
        return self._execute_query(query,())
