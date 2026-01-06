#!/bin/bash

# Package the Databricks Metadata Tool for distribution
# Excludes: .md files, outputs, logs, venv, __pycache__, .env, scripts, docs

PACKAGE_NAME="databricks-metadata-tool"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
ZIP_FILE="${PACKAGE_NAME}_${TIMESTAMP}.zip"

echo "Creating package: $ZIP_FILE"

# Create zip with specific inclusions and exclusions
zip -r "$ZIP_FILE" \
    main.py \
    requirements.txt \
    config.yaml.example \
    LICENSE \
    export_json_to_csv.py \
    src/ \
    -x "*.pyc" \
    -x "*__pycache__*" \
    -x "*.md"

echo ""
echo "Package created: $ZIP_FILE"
echo ""
echo "Contents:"
unzip -l "$ZIP_FILE" | head -30

echo ""
echo "Package size: $(du -h "$ZIP_FILE" | cut -f1)"
