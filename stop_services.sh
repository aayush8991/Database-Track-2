#!/bin/bash

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================${NC}"
echo -e "${BLUE}Stopping Docker Services${NC}"
echo -e "${BLUE}==================================${NC}\n"

docker-compose down

echo -e "\n${GREEN}✓ Services stopped successfully${NC}"
echo -e "${GREEN}==================================${NC}\n"
