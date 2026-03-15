#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================${NC}"
echo -e "${BLUE}Starting Docker Services${NC}"
echo -e "${BLUE}==================================${NC}\n"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${YELLOW}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo -e "${YELLOW}Docker daemon is not running. Attempting to start it...${NC}"
    sudo service docker start
    sleep 2
fi

# Start services
echo -e "${BLUE}Starting MySQL and MongoDB containers...${NC}\n"
docker-compose up -d

# Wait a moment for containers to start
sleep 3

# Check MySQL status
echo -e "${BLUE}Checking MySQL...${NC}"
if docker-compose exec -T mysql mysqladmin ping -h localhost &> /dev/null; then
    echo -e "${GREEN}✓ MySQL is running on localhost:3306${NC}"
    echo -e "  Credentials: root / root"
    echo -e "  Database: track_db"
else
    echo -e "${YELLOW}⚠ MySQL is still starting up...${NC}"
fi

# Check MongoDB status
echo -e "\n${BLUE}Checking MongoDB...${NC}"
if docker-compose exec -T mongodb mongosh --eval "db.adminCommand('ping')" &> /dev/null 2>&1; then
    echo -e "${GREEN}✓ MongoDB is running on localhost:27017${NC}"
    echo -e "  Username: admin | Password: password"
    echo -e "  Database: track_db"
else
    echo -e "${YELLOW}⚠ MongoDB is still starting up...${NC}"
fi

echo -e "\n${GREEN}==================================${NC}"
echo -e "${GREEN}Services are ready!${NC}"
echo -e "${GREEN}==================================${NC}\n"

echo -e "${YELLOW}Connection strings for your code:${NC}"
echo -e "  MySQL: mysql://dbuser:dbpassword@localhost:3306/track_db"
echo -e "  MongoDB: mongodb://admin:password@localhost:27017/track_db"
