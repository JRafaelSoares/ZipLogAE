#!/bin/sh

BASEDIR=$(dirname "$0")
USER=`whoami`
TARGET_PATH=/home/$USER/zipkat/
CODEBASE=$BASEDIR/..
EXEC_PATH=/home/$USER/workspace/zipkat/
ZIPLOG_EXEC_PATH=/home/$USER/workspace/ziplog/build

cat init_servers.txt|xargs -P0 -I% rsync -az ${EXEC_PATH}/store/benchmark/* $USER@%:${TARGET_PATH}
cat init_servers.txt|xargs -P0 -I% rsync -az ${EXEC_PATH}/*.shard0.config $USER@%:${TARGET_PATH}
cat init_servers.txt|xargs -P0 -I% rsync -az ${ZIPLOG_EXEC_PATH}/* $USER@%:${TARGET_PATH}

# setup
cat init_servers.txt | awk '{print $1}' | xargs -P0 -I% ssh $USER@% sudo mkdir -p /mnt/log
cat init_servers.txt | awk '{print $1}' | xargs -P0 -I% ssh $USER@% sudo chmod 777 /mnt/log
