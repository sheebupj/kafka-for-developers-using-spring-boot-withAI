# Kafka for Developers using Spring Boot

Gradle multi-module Spring Boot project with Apache Kafka integration.

**Technologies:** Java 25, Spring Boot 4, Apache Kafka, GitHub Copilot

## Project Structure

This is a **Gradle multi-module project** where all modules are defined in the root `build.gradle` and included in `settings.gradle`.

### Modules

- **lib-events-producer-boot4** - Kafka producer application

## Setup

### Clone the Repository

```bash
git clone <repo-url>
cd kafka-for-developers-using-spring-boot-withAI
```

### Build All Modules

```bash
./gradlew build
```

### Build Specific Module

```bash
./gradlew :lib-events-producer-boot4:build
```

Or from the module directory:

```bash
cd lib-events-producer-boot4
./gradlew build
```

### Run Specific Module

```bash
./gradlew :lib-events-producer-boot4:bootRun
```

## Adding New Modules

To add a new Spring Boot module to the project:

1. Create a new directory for the module in the root:
   ```bash
   mkdir new-module-name
   ```

2. Add the module to `settings.gradle`:
   ```gradle
   include 'new-module-name'
   ```

3. Create module structure with `build.gradle`:
   ```gradle
   plugins {
       id 'org.springframework.boot'
       id 'io.spring.dependency-management'
   }

   dependencies {
       // Add module dependencies here
   }
   ```

4. Rebuild the project:
   ```bash
   ./gradlew build
   ```

## Common Gradle Commands

```bash
# Clean all modules
./gradlew clean

# Run tests for all modules
./gradlew test

# Run tests for a specific module
./gradlew :lib-events-producer-boot4:test

# Build and run a specific module
./gradlew :lib-events-producer-boot4:bootRun

# List all tasks
./gradlew tasks
```
