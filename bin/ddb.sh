#!/bin/bash

# Stop on errors
# See https://vaneyckt.io/posts/safer_bash_scripts_with_set_euxo_pipefail/
set -Eeuo pipefail

# Sanity check command line options
usage() {
  echo "Usage: $0 (start)"
}

if [ $# -ne 1 ]; then
  usage
  exit 1
fi

# Parse argument.  $1 is the first argument
case $1 in
  "start")
    DDB_PATH=dynamodb_local_latest
    java -Djava.library.path=$DDB_PATH/DynamoDBLocal_lib -jar $DDB_PATH/DynamoDBLocal.jar -sharedDb
    ;;
  *)
    usage
    exit 1
    ;;
esac
