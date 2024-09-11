#!/usr/bin/env bash

set -e

BUILD_DIR=build
TARGET_DIR=../duo/ui/
# Remove legacy app dir
rm -rf ${TARGET_DIR}/_app
npm run build
cp -r ${BUILD_DIR}/* ${TARGET_DIR}
