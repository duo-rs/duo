#!/usr/bin/env bash

set -e

# Copy modified file to jaeger-ui repository
# cp -r packages/* jage-ui/jaeger-ui/packages/

cd jage-ui/jaeger-ui

yarn && yarn build

BUILD_DIR=packages/jaeger-ui/build
find ${BUILD_DIR} -type f \( -name "*runtime*.js" -o -name "*.map" \) | xargs rm

TARGET_DIR=../../jage/ui/

if [ -d "${TARGET_DIR}" ]
then
    rm -r ${TARGET_DIR}*
else
    mkdir -p ${TARGET_DIR}
fi

# Copy index.html file
cp ${BUILD_DIR}/index.html ${TARGET_DIR}

# Copy the rest static files
cp -r ${BUILD_DIR}/static ${TARGET_DIR}