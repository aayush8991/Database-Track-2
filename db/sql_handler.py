import mysql.connector
import os
from dotenv import load_dotenv
from sqlalchemy import text, create_engine
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
        self.engine = None
        self._init_engine()
        self.connect()

    def _init_engine(self):
        """Initialize SQLAlchemy engine for pandas operations with connection pooling."""
        try:
            connection_string = f"mysql+mysqlconnector://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            
            # Initialize with connection pooling configuration
            self.engine = create_engine(
                connection_string,
                pool_size=10,              # Max connections to keep in pool
                max_overflow=20,           # Max overflow beyond pool_size
                pool_recycle=3600,         # Recycle connections after 1 hour
                pool_pre_ping=True,        # Test connections before using them
                echo=False                 # Set to True for SQL debugging
            )
            print("[SQLHandler] Engine initialized with connection pooling (size=10, overflow=20)")
        except Exception as e:
            print(f"[SQL] Engine initialization failed: {e}")
            self.engine = None

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
                
                # MySQL requires VARCHAR with length for UNIQUE constraints on TEXT types
                if is_unique and sql_type == 'TEXT':
                    sql_type = 'VARCHAR(255)'
                
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
        if not self.engine:
            print("[SQLHandler] Engine not available, skipping normalized batch insert")
            return
        
        try:
            with self.engine.connect() as conn:
                for table_name, rows in tables_data.items():
                    if not rows:
                        continue

                    import pandas as pd
                    df = pd.DataFrame(rows)
                    
                    # 1. Dynamic Table Creation
                    self._ensure_table_exists(conn, table_name, df)
                    
                    # 2. Create strategic indexes for child tables
                    if table_name != "root" and "_" in table_name:
                        self._ensure_child_indexes(conn, table_name)
                    
                    # 3. Insert
                    df.to_sql(table_name, con=conn, if_exists='append', index=False)
                    
                conn.commit()
        except Exception as e:
            print(f"[SQLHandler] Error inserting normalized batch: {e}")
    
    def _ensure_child_indexes(self, conn, table_name):
        """Create indexes for foreign key columns in child tables."""
        try:
            # Index on foreign key for JOIN performance
            fk_col = "root_id"
            index_name = f"idx_{table_name}_{fk_col}"
            
            from sqlalchemy import text
            query = f"""
            CREATE INDEX IF NOT EXISTS `{index_name}` 
            ON `{table_name}` (`{fk_col}`)
            """
            conn.execute(text(query))
        except Exception as e:
            pass  # Index may already exist

    def _ensure_table_exists(self, conn, table_name, df):
        """Create table if not exists based on dataframe dtypes with proper constraints."""
        cols = []
        foreign_keys = []
        
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
            elif col_name.endswith("_id"):
                # Foreign key column - should not be null
                sql_type = "VARCHAR(36) NOT NULL"
                # Extract parent table name (e.g., "root_id" -> "root")
                parent_table = col_name[:-3]
                foreign_keys.append(
                    f"FOREIGN KEY (`{col_name}`) REFERENCES `{parent_table}`(`uuid`) ON DELETE CASCADE"
                )
            elif col_name == "parent_id":
                sql_type = "VARCHAR(36)"

            cols.append(f"`{col_name}` {sql_type}")

        # Add foreign key constraints
        cols.extend(foreign_keys)
        
        cols_str = ", ".join(cols)
        query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({cols_str})"
        
        try:
            from sqlalchemy import text
            conn.execute(text(query))
        except Exception as e:
            pass  # Table likely exists

    def reset_db(self):
        """Drop all tables to reset database for testing."""
        try:
            cursor = self.cursor
            
            # Disable foreign key checks temporarily
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")
            
            # Get all tables
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            
            # Drop each table
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                print(f"[SQL] Dropped table: {table}")
            
            # Re-enable foreign key checks
            cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            self.conn.commit()
            
            print("[SQL] Database reset complete")
        except Exception as e:
            print(f"[SQL] Reset failed: {e}")
    
    def create_tables_from_schema(self, schema_spec: dict):
        """
        Create tables with proper indexes and constraints based on schema specification.
        
        schema_spec format:
        {
            "root": {
                "columns": {"uuid": "VARCHAR(36)", "username": "VARCHAR(255)"},
                "primary_key": "uuid",
                "foreign_keys": [...],
                "indexes": [...]
            }
        }
        """
        for table_name, table_spec in schema_spec.items():
            self._create_single_table(table_name, table_spec)
    
    def _create_single_table(self, table_name: str, spec: dict):
        """Create a single table with all constraints and indexes."""
        
        columns = spec.get("columns", {})
        primary_key = spec.get("primary_key")
        foreign_keys = spec.get("foreign_keys", [])
        indexes = spec.get("indexes", [])
        unique_constraints = spec.get("unique_constraints", [])
        
        # Build CREATE TABLE statement
        create_stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
        
        col_defs = []
        
        # Add columns
        for col_name, col_type in columns.items():
            col_def = f"  `{col_name}` {col_type}"
            col_defs.append(col_def)
        
        # Add primary key constraint (if single)
        if primary_key and isinstance(primary_key, str):
            col_defs.append(f"  PRIMARY KEY (`{primary_key}`)")
        
        # Add composite primary key (for junction tables)
        elif primary_key and isinstance(primary_key, list):
            pk_cols = ", ".join([f"`{c}`" for c in primary_key])
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")
        
        # Add unique constraints
        for unique_cols in unique_constraints:
            if isinstance(unique_cols, list):
                uc_cols = ", ".join([f"`{c}`" for c in unique_cols])
            else:
                uc_cols = f"`{unique_cols}`"
            col_defs.append(f"  UNIQUE KEY ({uc_cols})")
        
        # Add foreign key constraints
        for fk in foreign_keys:
            fk_col = fk.get("column")
            fk_ref = fk.get("references")  # e.g., "root(uuid)"
            fk_def = f"  FOREIGN KEY (`{fk_col}`) REFERENCES {fk_ref} ON DELETE CASCADE"
            col_defs.append(fk_def)
        
        create_stmt += ",\n".join(col_defs)
        create_stmt += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        
        # Execute CREATE TABLE
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_stmt))
                conn.commit()
            print(f"✓ Created table `{table_name}`")
        except Exception as e:
            print(f"⚠ Failed to create table `{table_name}`: {e}")
        
        # Create additional indexes (beyond primary key)
        self._create_indexes(table_name, indexes)
    
    def _create_indexes(self, table_name: str, indexes: list):
        """Create indexes on the table."""
        
        for idx_spec in indexes:
            idx_type = idx_spec.get("type", "INDEX")
            idx_cols = idx_spec.get("columns", [])
            idx_name = idx_spec.get("name")
            
            # Skip primary key indexes (already created)
            if idx_type == "PRIMARY":
                continue
            
            # Auto-generate index name if not provided
            if not idx_name:
                idx_name = f"idx_{table_name}_{'_'.join(idx_cols)}"
            
            cols_str = ", ".join([f"`{c}`" for c in idx_cols])
            create_idx_stmt = f"CREATE INDEX `{idx_name}` ON `{table_name}` ({cols_str})"
            
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(create_idx_stmt))
                    conn.commit()
                print(f"  ✓ Created index `{idx_name}` on `{table_name}({cols_str})`")
            except Exception as e:
                # Index might already exist, that's okay
                if "already exists" not in str(e).lower():
                    print(f"  ⚠ Index creation failed: {e}")
    
    def show_table_schema(self, table_name: str) -> dict:
        """Show schema of a table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"DESCRIBE `{table_name}`"))
                schema = {}
                for row in result:
                    col_name = row[0]
                    col_type = row[1]
                    col_null = row[2]
                    col_key = row[3]
                    schema[col_name] = {
                        "type": col_type,
                        "null": col_null,
                        "key": col_key
                    }
                return schema
        except Exception as e:
            return {"error": str(e)}
    
    def show_table_indexes(self, table_name: str) -> list:
        """Show indexes on a table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SHOW INDEX FROM `{table_name}`"))
                indexes = []
                for row in result:
                    indexes.append({
                        "table": row[0],
                        "index_name": row[2],
                        "seq_in_index": row[3],
                        "column_name": row[4],
                        "unique": row[1] == 0  # 0 = unique
                    })
                return indexes
        except Exception as e:
            return [{"error": str(e)}]
    
    def verify_foreign_keys(self, table_name: str) -> dict:
        """Verify foreign key constraints on a table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, 
                           REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table_name}'
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """))
                fks = []
                for row in result:
                    fks.append({
                        "constraint": row[0],
                        "table": row[1],
                        "column": row[2],
                        "referenced_table": row[3],
                        "referenced_column": row[4]
                    })
                return {"table": table_name, "foreign_keys": fks}
        except Exception as e:
            return {"error": str(e)}

    def close(self):
        if self.conn:
            self.conn.close()