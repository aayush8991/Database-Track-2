#!/usr/bin/env python3
"""
Setup Script: Initialize Admin User & Dashboard Configuration

This script helps you:
1. Set up admin credentials (username/password)
2. Test database connections
3. Generate proper .env configuration
4. Initialize dashboard on first run

Usage:
    python3 setup_dashboard.py
"""

import os
import sys
import json
import secrets
import hashlib
from pathlib import Path
from dotenv import load_dotenv, set_key

# Colors for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text:^60}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.END}\n")

def print_success(text):
    print(f"{Colors.GREEN}✅ {text}{Colors.END}")

def print_error(text):
    print(f"{Colors.RED}❌ {text}{Colors.END}")

def print_info(text):
    print(f"{Colors.BLUE}ℹ️  {text}{Colors.END}")

def print_warning(text):
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")

def hash_password(password: str, salt: str) -> str:
    """Hash password using PBKDF2"""
    dk = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return dk.hex()

def verify_db_connection(env_file='.env'):
    """Test database connections"""
    print_header("Testing Database Connections")
    
    # Load environment
    load_dotenv(env_file)
    
    # Test MongoDB
    print_info("Testing MongoDB connection...")
    try:
        from pymongo import MongoClient
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        mongo_db = os.getenv('MONGO_DB_NAME', 'adaptive_db')
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.server_info()  # Trigger connection
        print_success(f"MongoDB connected: {mongo_db}")
    except Exception as e:
        print_error(f"MongoDB connection failed: {e}")
        print_warning("Make sure MongoDB is running, or update MONGO_URI in .env")
    
    # Test MySQL
    print_info("Testing MySQL connection...")
    try:
        import mysql.connector
        sql_host = os.getenv('SQL_HOST', 'localhost')
        sql_port = int(os.getenv('SQL_PORT', '3306'))
        sql_user = os.getenv('SQL_USER', 'root')
        sql_pass = os.getenv('SQL_PASSWORD', '')
        sql_db = os.getenv('SQL_DB_NAME', 'adaptive_db')
        
        conn = mysql.connector.connect(
            host=sql_host,
            port=sql_port,
            user=sql_user,
            password=sql_pass,
            database=sql_db,
            autocommit=True,
            connection_timeout=5
        )
        conn.close()
        print_success(f"MySQL connected: {sql_db} @ {sql_host}:{sql_port}")
    except Exception as e:
        print_error(f"MySQL connection failed: {e}")
        print_warning("Make sure MySQL is running, or update SQL_* variables in .env")

def setup_admin_credentials(env_file='.env'):
    """Interactive setup for admin credentials"""
    print_header("Admin Credentials Setup")
    
    print_info("Current admin setup options:")
    print("""
    1. Use default credentials (admin / admin) - ONLY for development
    2. Create custom admin credentials
    3. Use existing environment variable
    4. Skip (use defaults)
    """)
    
    choice = input(f"{Colors.CYAN}Enter choice (1-4): {Colors.END}").strip()
    
    if choice == '1':
        admin_user = 'admin'
        admin_pass = 'admin'
        print_warning("Using default credentials - UNSAFE for production!")
        print_info(f"Username: {admin_user}")
        print_info(f"Password: {admin_pass}")
    
    elif choice == '2':
        admin_user = input(f"{Colors.CYAN}Enter admin username (default: admin): {Colors.END}").strip() or 'admin'
        admin_pass = input(f"{Colors.CYAN}Enter admin password: {Colors.END}").strip()
        
        if not admin_pass:
            print_error("Password cannot be empty!")
            return setup_admin_credentials(env_file)
        
        confirm_pass = input(f"{Colors.CYAN}Confirm password: {Colors.END}").strip()
        if admin_pass != confirm_pass:
            print_error("Passwords don't match!")
            return setup_admin_credentials(env_file)
        
        print_success(f"Admin user '{admin_user}' configured")
    
    elif choice == '3':
        admin_pass = os.getenv('DASHBOARD_ADMIN_PASS', '')
        if not admin_pass:
            print_error("DASHBOARD_ADMIN_PASS not set in environment!")
            return setup_admin_credentials(env_file)
        admin_user = 'admin'
        print_success(f"Using DASHBOARD_ADMIN_PASS from environment")
    
    else:  # choice == '4' or invalid
        admin_user = 'admin'
        admin_pass = 'admin'
        print_info("Skipping custom setup, using defaults")
    
    # Save to .env
    set_key(env_file, 'DASHBOARD_ADMIN_PASS', admin_pass)
    set_key(env_file, 'DASHBOARD_ADMIN_USER', admin_user)
    
    print_success(f"Admin credentials saved to {env_file}")
    return admin_user, admin_pass

def setup_dashboard_config(env_file='.env'):
    """Configure dashboard settings"""
    print_header("Dashboard Configuration")
    
    # Token expiration
    token_exp = os.getenv('DASHBOARD_TOKEN_EXP', '3600')
    print_info(f"Current token expiration: {token_exp} seconds (1 hour)")
    
    custom_exp = input(f"{Colors.CYAN}Custom token expiration in seconds (or press Enter to keep): {Colors.END}").strip()
    if custom_exp:
        try:
            int(custom_exp)
            set_key(env_file, 'DASHBOARD_TOKEN_EXP', custom_exp)
            print_success(f"Token expiration set to {custom_exp} seconds")
        except ValueError:
            print_error("Invalid number!")
    
    # Generate secret if not present
    if not os.getenv('DASHBOARD_SECRET'):
        secret = secrets.token_urlsafe(32)
        set_key(env_file, 'DASHBOARD_SECRET', secret)
        print_success("Generated DASHBOARD_SECRET")
    
    # Port configuration
    port = input(f"{Colors.CYAN}Dashboard port (default 8000): {Colors.END}").strip() or '8000'
    set_key(env_file, 'DASHBOARD_PORT', port)
    print_success(f"Dashboard configured for port {port}")

def verify_assignment3_requirements():
    """Check if all Assignment 3 requirements are met"""
    print_header("Assignment 3 Requirements Check")
    
    from pathlib import Path
    
    requirements = {
        'web/dashboard.py': 'Dashboard API implementation',
        'web/auth.py': 'Authentication system',
        'web/static/index.html': 'Dashboard UI',
        'web/static/app.js': 'Client-side logic',
        'core/transaction_coordinator.py': 'Transaction coordination',
        'core/txn_wal.py': 'Write-Ahead Log for durability',
        'tests/test_acid_api.py': 'ACID test suite',
    }
    
    all_exist = True
    for file_path, description in requirements.items():
        if Path(file_path).exists():
            print_success(f"{description}: {file_path}")
        else:
            print_error(f"MISSING: {description}: {file_path}")
            all_exist = False
    
    if all_exist:
        print_success("\n✅ All required files present!")
    else:
        print_error("\n❌ Some files missing - check above!")
    
    return all_exist

def check_missing_endpoints():
    """Check for missing Assignment 3 endpoints"""
    print_header("Assignment 3 API Endpoints Check")
    
    import re
    
    dashboard_file = Path('web/dashboard.py')
    if not dashboard_file.exists():
        print_error("dashboard.py not found!")
        return
    
    content = dashboard_file.read_text()
    
    required_endpoints = {
        'POST /query': r'@app\.post\([\'"]\/query',
        'GET /api/schema/logical-entities': r'@app\.get\([\'"]\/api\/schema\/logical-entities',
        'POST /api/tools/acid/atomicity': r'@app\.post\([\'"]\/api\/tools\/acid\/atomicity',
        'POST /api/tools/acid/consistency': r'@app\.post\([\'"]\/api\/tools\/acid\/consistency',
        'POST /api/tools/acid/isolation': r'@app\.post\([\'"]\/api\/tools\/acid\/isolation',
        'POST /api/tools/acid/durability': r'@app\.post\([\'"]\/api\/tools\/acid\/durability',
        'POST /api/tools/acid/all': r'@app\.post\([\'"]\/api\/tools\/acid\/all',
        'GET /api/session-monitor': r'@app\.get\([\'"]\/api\/session-monitor',
        'GET /api/query-trace': r'@app\.get\([\'"]\/api\/query-trace',
    }
    
    missing = []
    for endpoint, pattern in required_endpoints.items():
        if re.search(pattern, content):
            print_success(f"✅ {endpoint}")
        else:
            print_error(f"❌ MISSING: {endpoint}")
            missing.append(endpoint)
    
    if missing:
        print_error(f"\n❌ Missing {len(missing)} endpoints!")
        print_warning("Check ASSIGNMENT3_CODE_ENHANCEMENTS.md for implementation guide")
    else:
        print_success(f"\n✅ All required endpoints present!")
    
    return len(missing) == 0

def main():
    """Main setup workflow"""
    print(f"{Colors.CYAN}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'Dashboard Setup Script':^60}{Colors.END}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'Assignment 3: Adaptive Database System':^60}{Colors.END}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'='*60}{Colors.END}\n")
    
    # Check if .env exists
    env_file = '.env'
    if not Path(env_file).exists():
        print_warning(f"{env_file} not found. Creating from template...")
        create_env_template(env_file)
    
    # Step 1: Check existing configuration
    print_header("Step 1: Configuration Check")
    load_dotenv(env_file)
    
    print_info("Current environment variables:")
    for key in ['MONGO_URI', 'SQL_HOST', 'MONGO_DB_NAME', 'SQL_DB_NAME']:
        val = os.getenv(key, 'NOT SET')
        # Mask sensitive data
        if 'PASS' in key or 'KEY' in key or 'TOKEN' in key:
            val = '***MASKED***'
        print(f"  {key}: {val}")
    
    # Step 2: Test database connections
    response = input(f"\n{Colors.CYAN}Test database connections? (y/n): {Colors.END}").strip().lower()
    if response == 'y':
        verify_db_connection(env_file)
    
    # Step 3: Admin setup
    response = input(f"\n{Colors.CYAN}Configure admin credentials? (y/n): {Colors.END}").strip().lower()
    if response == 'y':
        setup_admin_credentials(env_file)
    
    # Step 4: Dashboard config
    response = input(f"\n{Colors.CYAN}Configure dashboard settings? (y/n): {Colors.END}").strip().lower()
    if response == 'y':
        setup_dashboard_config(env_file)
    
    # Step 5: Check Assignment 3 requirements
    print_header("Step 5: Assignment 3 Verification")
    verify_assignment3_requirements()
    check_missing_endpoints()
    
    # Step 6: Summary
    print_header("Setup Complete!")
    print_success("Dashboard is ready to run!")
    print(f"\n{Colors.BOLD}Next steps:{Colors.END}")
    print(f"1. Verify all database connections are working")
    print(f"2. Start dashboard: uvicorn web.dashboard:app --port 8000")
    print(f"3. Open browser: http://localhost:8000")
    print(f"4. Login with:")
    print(f"   - Username: {os.getenv('DASHBOARD_ADMIN_USER', 'admin')}")
    print(f"   - Password: (as configured in setup)")
    print(f"\n{Colors.YELLOW}For more information:{Colors.END}")
    print(f"- Read: ASSIGNMENT3_ANALYSIS.md")
    print(f"- Read: ASSIGNMENT3_Q_AND_A.md")
    print(f"- Read: ASSIGNMENT3_CODE_ENHANCEMENTS.md")

def create_env_template(env_file='.env'):
    """Create .env template if it doesn't exist"""
    template = """# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017/
MONGO_DB_NAME=adaptive_db

# MySQL Configuration
SQL_HOST=localhost
SQL_PORT=3306
SQL_USER=root
SQL_PASSWORD=your_password
SQL_DB_NAME=adaptive_db

# Dashboard Configuration
DASHBOARD_ADMIN_PASS=admin
DASHBOARD_ADMIN_USER=admin
DASHBOARD_TOKEN_EXP=3600
DASHBOARD_SECRET=your-secret-key-here

# Optional API Keys
GROQ_API_KEY=your_api_key_here
STREAM_URL=http://127.0.0.1:8000/record/5000

# Dashboard Settings
DASHBOARD_PORT=8000
ACID_TEST_TIMEOUT_SEC=45
SESSION_IDLE_SECONDS=7200
"""
    
    with open(env_file, 'w') as f:
        f.write(template)
    
    print_success(f"Created .env template: {env_file}")
    print_warning("Update database credentials in .env before running!")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Setup cancelled by user{Colors.END}")
        sys.exit(0)
    except Exception as e:
        print_error(f"Setup failed: {e}")
        sys.exit(1)
