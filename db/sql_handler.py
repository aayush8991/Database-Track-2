import mysql.connector
import os
from dotenv import load_dotenv
from sqlalchemy import text
import pandas as pd

load_dotenv()

class SQLHandler:
    def __init__(self):
        self.config = {
            'host': os.getenv("SQL_HOST"),
            'port': int(os.getenv("SQL_PORT", 3306)),
            'user': os.getenv("SQL_USER"),
            'password': os.getenv("SQL_PASSWORD"),
            'database': os.getenv("SQL_DB_NAME")
        }
        self.table_name = "structured_data"
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor()
            self._create_base_table()
            print("[SQL] Connected to Remote Database successfully.")
        except mysql.connector.Error as err:
            print(f"[SQL Error] Connection failed: {err}")

    def _create_base_table(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255),
            timestamp DATETIME,
            sys_ingested_at DATETIME,
            INDEX (sys_ingested_at),
            INDEX (username)
        )
        """
        self.cursor.execute(query)
        self.conn.commit()
        self._refresh_schema_cache()

    def _refresh_schema_cache(self):
        self.cursor.execute(f"DESCRIBE {self.table_name}")
        self.existing_cols = {row[0] for row in self.cursor.fetchall()}

    def update_schema(self, schema_decisions):
        if not hasattr(self, 'existing_cols'):
            self._refresh_schema_cache()

        for field, decision in schema_decisions.items():
            if decision['target'] in ['SQL', 'BOTH'] and field not in self.existing_cols:
                sql_type = decision.get('sql_type', 'TEXT')
                is_unique = decision.get('is_unique', False)
                
                constraint = " UNIQUE" if is_unique else ""
                print(f"[SQL Handler] Evolving Schema: Adding column '{field}' as {sql_type}{constraint}")
                
                alter_query = f"ALTER TABLE {self.table_name} ADD COLUMN {field} {sql_type}{constraint}"
                try:
                    self.cursor.execute(alter_query)
                    self.existing_cols.add(field)
                except mysql.connector.Error as err:
                    print(f"Failed to add column {field}: {err}")
        
        self.conn.commit()

    def insert_batch(self, records):
        if not records:
            return

        if not hasattr(self, 'existing_cols'):
            self._refresh_schema_cache()

        valid_columns = self.existing_cols

        for record in records:
            filtered_rec = {k: v for k, v in record.items() if k in valid_columns}
            
            if not filtered_rec:
                continue

            columns = ', '.join(filtered_rec.keys())
            placeholders = ', '.join(['%s'] * len(filtered_rec))
            values = list(filtered_rec.values())
            
            sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
            
            try:
                self.cursor.execute(sql, values)
            except mysql.connector.Error as err:
                print(f"Insert Error: {err}")
        
        self.conn.commit()

    def insert_normalized_batch(self, tables_data):
        """
        Takes { 'root': [rows], 'root_orders': [rows] } and inserts into SQL.
        """
        try:
            with self.engine.connect() as conn:
                for table_name, rows in tables_data.items():
                    if not rows:
                        continue

                    import pandas as pd
                    df = pd.DataFrame(rows)
                    
                    # 1. Dynamic Table Creation
                    self._ensure_table_exists(conn, table_name, df)
                    
                    # 2. Insert
                    df.to_sql(table_name, con=conn, if_exists='append', index=False)
                    
                conn.commit()
        except Exception as e:
            print(f"[SQLHandler] Error inserting normalized batch: {e}")

    def _ensure_table_exists(self, conn, table_name, df):
        """Create table if not exists based on dataframe dtypes."""
        cols = []
        for col_name, dtype in df.dtypes.items():
            sql_type = "TEXT"
            if "int" in str(dtype): 
                sql_type = "INT"
            elif "float" in str(dtype): 
                sql_type = "FLOAT"
            elif "datetime" in str(dtype): 
                sql_type = "DATETIME"
            
            if col_name == "uuid":
                sql_type = "VARCHAR(36) PRIMARY KEY"
            elif col_name.endswith("_id") or col_name == "parent_id":
                sql_type = "VARCHAR(36)"

            cols.append(f"`{col_name}` {sql_type}")

        cols_str = ", ".join(cols)
        query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols_str})"
        
        try:
            from sqlalchemy import text
            conn.execute(text(query))
        except Exception as e:
            pass  # Table likely exists

    def close(self):
        if self.conn:
            self.conn.close()