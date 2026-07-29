# Docker Container Management Commands

Quick reference for Docker commands to stop, remove, and manage containers.

---

## 🛑 Stop and Remove Containers

### Remove All Containers (regardless of state)
```bash
# Remove all containers (both running and stopped)
docker rm -f $(docker ps -aq)
```

### Stop All Running Containers
```bash
# Stop all running containers gracefully
docker stop $(docker ps -q)
```

### Remove All Stopped Containers
```bash
# Remove all containers that are not running
docker rm $(docker ps -aq)
```

### Force Remove All Containers
```bash
# Forcefully remove all containers (running or stopped)
docker rm -f $(docker ps -aq)
```

---

## 🧹 Clean Up Everything

### Remove All Containers, Images, and Volumes
```bash
# Remove all containers
docker container prune -f

# Remove all images (unused)
docker image prune -a -f

# Remove all volumes
docker volume prune -f

# Remove everything (containers, images, volumes, networks)
docker system prune -a -f
```

### Remove Specific Container
```bash
# Remove by container name
docker rm -f <container_name>

# Remove by container ID
docker rm -f <container_id>
```

---

## 📊 View Container Status

### List All Containers
```bash
# List running containers
docker ps

# List all containers (running and stopped)
docker ps -a

# List only container IDs (useful for piping commands)
docker ps -aq
```

### View Container Logs
```bash
# View logs of a specific container
docker logs <container_name>

# Follow logs in real-time
docker logs -f <container_name>

# View last 100 lines
docker logs --tail 100 <container_name>
```

---

## 🔄 Stop/Start Operations

### Stop Specific Container
```bash
docker stop <container_name>
docker stop <container_id>
```

### Start Stopped Container
```bash
docker start <container_name>
docker start <container_id>
```

### Restart Container
```bash
docker restart <container_name>
docker restart <container_id>
```

### Pause and Unpause
```bash
# Pause a running container
docker pause <container_name>

# Resume a paused container
docker unpause <container_name>
```

---

## 🐳 Docker Compose Commands

### Stop and Remove Services (with Docker Compose)
```bash
# Stop services without removing containers
docker-compose stop

# Stop and remove containers
docker-compose down

# Stop, remove containers, images, and volumes
docker-compose down -v --rmi all
```

### Start Services
```bash
# Start services
docker-compose start

# Start and create containers
docker-compose up -d
```

### View Compose Logs
```bash
# View all service logs
docker-compose logs

# Follow logs in real-time
docker-compose logs -f

# View logs for specific service
docker-compose logs -f <service_name>
```

---

## 💾 Images Management

### List All Images
```bash
docker images
```

### Remove Unused Images
```bash
# Remove dangling images
docker image prune -f

# Remove all unused images
docker image prune -a -f
```

### Remove Specific Image
```bash
# Remove by image name
docker rmi <image_name>

# Force remove
docker rmi -f <image_name>
```

---

## 📦 Volumes Management

### List All Volumes
```bash
docker volume ls
```

### Remove Unused Volumes
```bash
docker volume prune -f
```

### Remove Specific Volume
```bash
docker volume rm <volume_name>
```

---

## 🔍 Inspect Container Details

### Get Container Information
```bash
# View detailed container info
docker inspect <container_name>

# Get container IP address
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' <container_name>
```

### View Container Processes
```bash
# List processes running in container
docker top <container_name>
```

---

## 🚀 Common Workflows

### Clean Start (Remove Everything and Start Fresh)
```bash
# 1. Stop all containers
docker stop $(docker ps -q)

# 2. Remove all containers
docker rm $(docker ps -aq)

# 3. Remove all images
docker rmi $(docker images -q)

# 4. Remove all volumes
docker volume prune -f

# 5. Run your application fresh
docker-compose up -d
```

### Restart All Services
```bash
# Stop all services
docker-compose down

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f
```

### Emergency Cleanup (Nuclear Option)
```bash
# WARNING: This removes EVERYTHING - use with caution!
docker system prune -a -f --volumes
```

---

## ⚠️ Warning - Data Loss

These commands **WILL DELETE** containers and data:

```bash
# DESTRUCTIVE - Removes all containers permanently
docker rm -f $(docker ps -aq)

# DESTRUCTIVE - Removes all volumes (databases, data) permanently
docker volume prune -f

# DESTRUCTIVE - Removes all images permanently
docker rmi -f $(docker images -q)

# DESTRUCTIVE - Nuclear option - removes everything
docker system prune -a -f --volumes
```

**Make sure to backup important data before running these commands!**

---

## 📋 Quick Command Cheat Sheet

| Task | Command |
|------|---------|
| List running containers | `docker ps` |
| List all containers | `docker ps -a` |
| Stop container | `docker stop <name>` |
| Remove container | `docker rm <name>` |
| Remove all containers | `docker rm -f $(docker ps -aq)` |
| View container logs | `docker logs <name>` |
| Follow container logs | `docker logs -f <name>` |
| Remove unused images | `docker image prune -a -f` |
| Remove unused volumes | `docker volume prune -f` |
| Docker Compose up | `docker-compose up -d` |
| Docker Compose down | `docker-compose down` |
| Docker Compose logs | `docker-compose logs -f` |
| Full cleanup | `docker system prune -a -f` |

---

## 🔧 Usage for Library Events Application

### Stop Application Containers
```bash
# If using Docker Compose
docker-compose down

# Or stop individual containers
docker stop kafka
docker stop zookeeper
docker stop library-events-app
```

### Remove All Application Containers
```bash
# Remove all containers
docker rm -f $(docker ps -aq)
```

### Clean Up Before Restart
```bash
# Stop services
docker-compose down

# Remove containers
docker rm -f $(docker ps -aq)

# Remove volumes (be careful - loses data)
docker volume prune -f

# Start fresh
docker-compose up -d
```

### View Application Logs
```bash
# View all service logs
docker-compose logs

# Follow logs in real-time
docker-compose logs -f

# View only application logs
docker-compose logs -f library-events-app
```

---

## ℹ️ Tips & Tricks

### Filter Containers by Status
```bash
# Show only running containers
docker ps --filter "status=running"

# Show only stopped containers
docker ps --filter "status=exited"

# Show only containers with specific label
docker ps --filter "label=app=library-events"
```

### View Disk Usage
```bash
# See Docker system disk usage
docker system df
```

### Export Container to Image
```bash
# Save running container as new image
docker commit <container_name> <new_image_name>
```

---

## 🆘 Troubleshooting

### Container Won't Stop
```bash
# Kill container forcefully
docker kill <container_name>
```

### Can't Remove Container
```bash
# First stop it
docker stop <container_name>

# Then remove it
docker rm <container_name>
```

### Check What's Using Port
```bash
# Linux/Mac
lsof -i :<port_number>

# Windows PowerShell
netstat -ano | findstr :<port_number>
```

### View Container Errors
```bash
# Check last 50 lines of logs
docker logs --tail 50 <container_name>

# Follow errors in real-time
docker logs -f <container_name> | grep -i error
```
