#!/usr/bin/env bash

set -e

# Copyright (c) Bartłomiej Płotka @bwplotka
# Licensed under the Apache License 2.0.

# Yolo script allowing nice benchmark framework for iterative work on Go performance.
# Requirements:
#  * go install golang.org/x/perf/cmd/benchstat
#  * Results produce by previous bench.sh runs.
# Example usage:
#  * Commit changes you want to test against e.g c513dab8752dc69a46e29de7a035680947778593.
#  * Name this comparable with some human name like `1mblimit`:
#  * Run bash bench.sh 1mblimit c513dab8752dc69a46e29de7a035680947778593 make sure WORK_DIR points to your repo.
#  * Expect results being printed and persisted in ./results for further use (e.g comparing via cmp.sh
#  * Run some other version with different name: bash bench.sh nolimit a693a667f9eea1fbd56c7bc2a2140fb3c598ef34
#  * Compare using bash cmp.sh nolimit 1mblimt

# Shared @ https://gist.github.com/bwplotka/3b853c31ed11e77c975b9df45d105d74

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

OLD="${1}"
NEW="${2}"

# TODO: Hardcoded, change it, or parametrize.
TESTS="BenchmarkTelemeterRealData_Series"
DATE=$(date '+%Y-%m-%d-T%H-%M-%S')

for TEST in ${TESTS}
do
  STEST=${TEST//\//-}
  TEST_DIR="${DIR}/diffs/${STEST}/"
  mkdir -p ${TEST_DIR}

  # TODO(bwplotka): Support more results than just two.
  OLD_OUT=$(ls ${DIR}/results/${STEST}/${OLD}/*.bench.out | tail -n 1)
  OLD_SPEC=$(cat ${OLD_OUT} | head -n 1)
  NEW_OUT=$(ls ${DIR}/results/${STEST}/${NEW}/*.bench.out | tail -n 1)
  NEW_SPEC=$(cat ${OLD_OUT} | head -n 1)

  echo "Comparing '${OLD_OUT}' (${OLD_SPEC}) with (new) '${NEW_OUT}' (${NEW_SPEC})"
  benchstat -delta-test=none ${OLD_OUT} ${NEW_OUT} | tee "${TEST_DIR}/${DATE}.${OLD}-vs-${NEW}.diff.out"
done