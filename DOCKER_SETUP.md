# Docker Setup Guide - MySQL & MongoDB

This guide helps you quickly start MySQL and MongoDB services in Docker after restarting your codespace.

## Quick Start

### Method 1: Using Shell Scripts (Recommended)

**First time only** - Make scripts executable:
```bash
chmod +x start_services.sh stop_services.sh
```

**Start services:**
```bash
./start_services.sh
```

**Stop services:**
```bash
./stop_services.sh
```

### Method 2: Using Docker Compose Directly

**Start services:**
```bash
docker-compose up -d
```

**Stop services:**
```bash
docker-compose down
```

**View logs:**
```bash
docker-compose logs -f
```

**Restart specific service:**
```bash
docker-compose restart mysql      # or mongodb
```

## Service Details

### MySQL
- **Container Name:** mysql-container
- **Port:** 3306
- **Root Password:** root
- **Database:** track_db
- **User:** dbuser
- **Password:** dbpassword
- **Connection String:** `mysql://dbuser:dbpassword@localhost:3306/track_db`

### MongoDB
- **Container Name:** mongodb-container
- **Port:** 27017
- **Username:** admin
- **Password:** password
- **Database:** track_db
- **Connection String:** `mongodb://admin:password@localhost:27017/track_db`

## Auto-Start on Codespace Restart

To automatically start services when your codespace restarts, follow these steps:

1. **Create `.devcontainer/devcontainer.json`** (if not exists):
```json
{
  "name": "Database Track",
  "image": "mcr.microsoft.com/devcontainers/python:3.11",
  "features": {
    "ghcr.io/devcontainers/features/docker-in-docker:2": {}
  },
  "postCreateCommand": "bash -c 'chmod +x start_services.sh && ./start_services.sh'",
  "postStartCommand": "./start_services.sh",
  "forwardPorts": [3306, 27017, 8000],
  "portsAttributes": {
    "3306": {"label": "MySQL", "onAutoForward": "notify"},
    "27017": {"label": "MongoDB", "onAutoForward": "notify"},
    "8000": {"label": "API", "onAutoForward": "notify"}
  }
}
```

2. **Or manually run after restart:**
```bash
./start_services.sh
```

## Useful Commands

**Check service status:**
```bash
docker-compose ps
```

**Access MySQL CLI:**
```bash
docker-compose exec mysql mysql -u root -p
# Enter password: root
```

**Access MongoDB CLI:**
```bash
docker-compose exec mongodb mongosh -u admin -p
# Enter password: password
```

**View container logs:**
```bash
docker-compose logs mysql      # MySQL logs
docker-compose logs mongodb    # MongoDB logs
```

**Clean up (remove volumes):**
```bash
docker-compose down -v
```

## Integration with Your Code

Update your database handlers to use these credentials:

**For SQL Handler:**
```python
from db.sql_handler import SQLHandler

handler = SQLHandler(
    host="localhost",
    user="dbuser",
    password="dbpassword",
    database="track_db",
    port=3306
)
```

**For Mongo Handler:**
```python
from db.mongo_handler import MongoHandler

handler = MongoHandler(
    uri="mongodb://admin:password@localhost:27017/track_db"
)
```

## Troubleshooting

**Port already in use?**
```bash
# Stop existing containers
docker-compose down
# Or check what's using the port
sudo lsof -i :3306  # MySQL
sudo lsof -i :27017 # MongoDB
```

**Container won't start?**
```bash
# Check logs
docker-compose logs mysql
docker-compose logs mongodb
```

**Want to reset databases?**
```bash
docker-compose down -v  # Removes volumes too
docker-compose up -d    # Fresh start
```
