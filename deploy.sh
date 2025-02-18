#!/bin/bash
# deploy.sh - Clone (or update) the Git repository and run the deployment script.

# Define the Git repository URL (replace with your repository URL)
REPO_URL="https://github.com/bharath30024/aws-glue-project.git"
PROJECT_DIR="aws-glue-project"

# Clone repository if not already present
if [ ! -d "$PROJECT_DIR" ]; then
    echo "Cloning repository from $REPO_URL..."
    git clone "$REPO_URL"
else
    echo "Repository exists. Pulling latest changes..."
    cd "$PROJECT_DIR" || exit 1
    git pull
    cd ..
fi

# Change directory to the project and run the deployment script
cd "$PROJECT_DIR" || exit 1
echo "Running deployment script..."
python3 deploy_and_run.py
