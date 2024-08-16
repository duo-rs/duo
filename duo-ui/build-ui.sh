#!/usr/bin/env bash

set -e

BUILD_DIR=build
TARGET_DIR=../duo/ui/
npm run build
cp -r ${BUILD_DIR}/* ${TARGET_DIR}
