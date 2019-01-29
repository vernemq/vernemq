#!/bin/bash

set -e

ERLANG=$1
if [ -z ${ERLANG} ]; then
    echo "Erlang version not set"
    exit -1
fi
echo "Erlang Version: ${ERLANG}"

RELEASE=$2
if [ -z ${RELEASE} ]; then
    echo "Release not set"
    exit -1
fi
echo "Release '${RELEASE}'"

PLATFORM=$3
if [ -z ${PLATFORM} ]; then
    echo "Platform [deb|rpm] not set"
    exit -1
fi
echo "Platform '${PLATFORM}'"

OS=$4
if [ -z ${OS} ]; then
    echo "OS not set"
    exit -1
fi
echo "Os '${OS}'"

. erlang-$ERLANG/activate

cd /opt
rm -Rf vernemq
git clone git://github.com/erlio/vernemq vernemq 
cd vernemq

PKG_NAME=$RELEASE

git checkout $RELEASE

make package

cd /packages

for FILE in `find /opt/vernemq/package/packages -name "*.${PLATFORM}"` ; do
    file=$(basename $FILE)
    if [[ $file == *"src.rpm"* ]]; then
        continue
    fi
    if [[ $file == *"dbgsym"* ]]; then
        continue
    fi
    if [[ $file == *_amd64.deb ]]; then
        file=vernemq-$RELEASE.$OS.x86_64.deb
        mv $FILE $file
        sha256sum $file >> sha256sums.txt
    fi
    if [[ $file == *.rpm ]]; then
        file=vernemq-$RELEASE.$OS.x86_64.rpm
        mv $FILE $file
        sha256sum $file >> sha256sums.txt
    fi
done
