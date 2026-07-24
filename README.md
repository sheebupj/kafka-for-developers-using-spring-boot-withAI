# Kafka for Developers using Spring Boot

Multi-module Spring Boot project with Apache Kafka integration.
using java 25 springboot 4 kafka and github copilot

## Modules

- **lib-events-producer-boot4** - Kafka producer application
- *Add more modules here*

## Setup

### Clone with Submodules

```bash
git clone --recurse-submodules <repo-url>
```

If you already cloned without submodules:

```bash
git submodule init
git submodule update --recursive
```

### Build All Modules

```bash
./gradlew build
```

### Build Specific Module

```bash
cd lib-events-producer-boot4
./gradlew build
```

## Adding New Submodules

To add a new Spring Boot module as a submodule:

```bash
git submodule add <module-repo-url> <module-name>
git add .gitmodules <module-name>
git commit -m "Add new submodule: <module-name>"
git push
```

## Working with Submodules

### Update all submodules

```bash
git submodule update --remote --merge
```

### Pull changes from all submodules

```bash
git pull --recurse-submodules
```

### Commit changes in submodule

```bash
cd lib-events-producer-boot4
git add .
git commit -m "Your changes"
git push origin main

# Return to root
cd ..
git add lib-events-producer-boot4
git commit -m "Update submodule reference"
git push
```
