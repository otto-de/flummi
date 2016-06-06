#!/bin/sh


GRADLE_VERSION=$1

USERNAME="ft3_pipeline"
PASSWORD="]peZaq1dBk3E"

PROPERTIES_FILE="wrapper/gradle-wrapper.properties"


ARTIFACT_TARGET_URL="https://artifactory.lhotse.ov.otto.de/artifactory/maven-p13n-releases-local/thirdparty/de/otto/p13n/gradle/${GRADLE_VERSION}"
BIN_TARGET="${ARTIFACT_TARGET_URL}/gradle-${GRADLE_VERSION}-bin.zip"

WRAPPER_TARGET="/tmp/gradle-bin.zip"

WRAPPER_URL=$(cat $PROPERTIES_FILE | grep distributionUrl | cut -d'=' -f 2 | sed -e s/https\\\\:/https:/)


echo "Downloading the Gradle wrapper from ${WRAPPER_URL}"

curl --location -o "${WRAPPER_TARGET}" "${WRAPPER_URL}"


echo "Uploading artifact to ${BIN_TARGET}"

curl -v -u "${USERNAME}:${PASSWORD}" --upload-file "${WRAPPER_TARGET}" "${BIN_TARGET}"
