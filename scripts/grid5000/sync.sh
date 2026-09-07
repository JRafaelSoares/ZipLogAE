#!/bin/bash

if [ -z "$1" ]; then
	echo "Missing front end of grid5000"
	exit 1
fi

rsync -av --exclude deps --exclude .git --exclude .idea --exclude .venv --exclude cmake-build-docker --exclude .github/ . $1.g5k:~/ZipPaper/NextGenZipLog/
