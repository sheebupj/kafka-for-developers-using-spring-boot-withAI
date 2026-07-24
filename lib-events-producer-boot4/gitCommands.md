Step-by-Step: Setting Up Git Submodules
1️⃣ Add Your First Submodule (lib-events-producer-boot4)
cd kafka-for-developers-using-spring-boot-withAI
git submodule add <repo-url> lib-events-producer-boot4
If lib-events-producer-boot4 is already a git repo locally, convert it first:
cd lib-events-producer-boot4
git init
git add .
git commit -m "Initial commit"
cd ..
2️⃣ Add More Submodules
When you have other Spring Boot modules ready (e.g., lib-events-consumer-boot4):
git submodule add <repo-url> lib-events-consumer-boot4
git submodule add <repo-url> another-module
3️⃣ Complete Git Commands Reference
# Clone repo with all submodules
git clone --recurse-submodules <main-repo-url>

# If cloned without submodules, initialize them
git submodule init
git submodule update

# Update all submodules to latest
git submodule update --remote

# Create .gitmodules file (lists all submodules)
# This will be auto-created after adding submodules

# View all submodules
git submodule

# Work on a specific submodule
cd lib-events-producer-boot4
git checkout main  # or your branch
git pull

# Commit changes in submodule
git add .
git commit -m "Updates in producer"
git push

# Return to root and commit submodule pointer
cd ..
git add lib-events-producer-boot4
git commit -m "Update producer submodule reference"
git push