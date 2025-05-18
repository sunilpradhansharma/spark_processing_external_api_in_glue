#!/bin/bash

# Create a temporary directory for packaging
TEMP_DIR=$(mktemp -d)
PACKAGE_DIR="$TEMP_DIR/processing_modules"
mkdir -p "$PACKAGE_DIR"

# Copy source files
cp -r ../src/* "$PACKAGE_DIR/"

# Create zip file
cd "$TEMP_DIR"
zip -r processing_modules.zip processing_modules/

# Upload to S3
aws s3 cp processing_modules.zip s3://sparkprocessingstack-sparkprocessingdatabucketfef2-l9mrdgqisohw/dependencies/

# Clean up
rm -rf "$TEMP_DIR" 