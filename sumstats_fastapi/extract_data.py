import os, sys
import sqlite3
from typing import Any, Callable, Dict, List
from ftplib import FTP
import sqlite3

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

class DataExtractor:
    def __init__(self, ftp_url: str, db_path: str = "temp.db", table_name: str = "studies"):
        self.ftp_url = ftp_url
        self.db_path = db_path
        self.table_name = table_name
        self._download_db_file()

    def _download_db_file(self):
        stripped = self.ftp_url.replace("ftp://", "")
        host, *path_parts = stripped.split("/")
        file_path = "/" + "/".join(path_parts[:-1])
        filename = path_parts[-1]

        ftp = FTP(host)
        ftp.login()
        ftp.cwd(file_path)

        with open(self.db_path, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)

        ftp.quit()
        
    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Helper method to execute a SQL query and return results as a list of dictionaries."""
        try:
            conn=sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in rows]
            cursor.close()
            conn.close()
            return data
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")  # Replace with logging if needed
            return []

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

    def extract_by_multiple_columns(self, **kwargs) -> List[Dict[str, Any]]:
        """Extract rows that match multiple column criteria (AND logic).
        extractor.extract_by_multiple_columns(chromosome="1", ref="A")
        """
        conditions = " AND ".join(f"{col} = ?" for col in kwargs.keys())
        query = f"SELECT * FROM {self.table_name} WHERE {conditions}"
        return self._execute_query(query, tuple(kwargs.values()))

    def extract_by_range(self, column_name: str, min_value: Any, max_value: Any) -> List[Dict[str, Any]]:
        """Extract rows where the column value is within a specified range.
        extractor.extract_by_range("position", 10000, 60000)
        """
        query = f"SELECT * FROM {self.table_name} WHERE {column_name} BETWEEN ? AND ?"
        return self._execute_query(query, (min_value, max_value))

    def extract_by_contains(self, column_name: str, substring: str) -> List[Dict[str, Any]]:
        """Extract rows where the column contains the given substring.
        extractor.extract_by_contains("gene", "BRCA")
        """
        query = f"SELECT * FROM {self.table_name} WHERE {column_name} LIKE ?"
        return self._execute_query(query, (f"%{substring}%",))

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
    
    def is_number(self, value):
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
        
    def build_where_clause(self, query_params):
        """
        Convert query parameters to a SQLite WHERE clause with values embedded.
        
        Parameters:
        - query_params: Dict with field names and values
        
        Returns:
        - where_clause string with values already incorporated
        """
        if not query_params:
            return ""
        
        conditions = []
        
        
        for key, value in query_params.items():
            # Check if value contains a comparison operator
            operators = [">", "<"]
            has_operator = any(op in str(key) for op in operators)
            
            if has_operator and isinstance(key, str):
                # Extract operator and actual value
                for op in operators:
                    if op in key:
                        new_key = key.split(op)[0].strip()
                        print("value:",value,",",len(value))
                        if len(value) == 0:
                            new_value = key.split(op)[1].strip()
                        else:
                            new_value = value

                        conditions.append(f"{new_key} != 'NA'")
                        # Handle string values with quotes
                        if self.is_number(new_value.strip()):
                            conditions.append(f"{new_key} {op} {new_value}")
                        else:
                            conditions.append(f"{new_key} {op} '{new_value}'")
                        break
            else:
                # Default to equality comparison
                if isinstance(value, str) and not self.is_number(value.strip()):
                    conditions.append(f"{key} = '{value}'")
                else:
                    conditions.append(f"{key} = {value}")
        
        where_clause = " AND ".join(conditions)
        return where_clause

    
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
