#!/usr/bin/env bash

set -e

# Copyright (c) Bartłomiej Płotka @bwplotka
# Licensed under the Apache License 2.0.

# Yolo script allowing nice benchmark framework for iterative work on Go performance.
# Requirements:
#  * Install github.com/aclements/perflock (+daemon).
#  * Prepare worktree if you want to benchmark in background: git worktree add ../thanos_b yolo
# Example usage:
#  * Commit changes you want to test against e.g c513dab8752dc69a46e29de7a035680947778593.
#  * Name this comparable with some human name like `1mblimit`:
#  * Run bash bench.sh 1mblimit c513dab8752dc69a46e29de7a035680947778593 make sure WORK_DIR points to your repo.
#  * Expect results being printed and persisted in ./results for further use (e.g comparing via cmp.sh

# Shared @ https://gist.github.com/bwplotka/3b853c31ed11e77c975b9df45d105d74

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG="./pkg/store"
TEST="BenchmarkTelemeterRealData_Series"
STEST=${TEST//\//-}

# TODO: Parametrize all via flags.
RUN="${1}"
COMMIT=$(git rev-parse --short HEAD)
BENCH_TIME="2m"
DATE=$(date '+%Y-%m-%d-T%H-%M-%S')

echo "Running ${TEST} (sanitized: ${STEST}) in ${WORK_DIR} for commit ${COMMIT} as ${RUN}"
TEST_DIR="${DIR}/results/${STEST}/${RUN}"
OUT_FILE="${TEST_DIR}/${DATE}.bench.out"
CPU_PROF_FILE="${TEST_DIR}/${DATE}.cpu.pprof"
MEM_PROF_FILE="${TEST_DIR}/${DATE}.mem.pprof"

mkdir -p ${TEST_DIR}

# Give context.
echo "${COMMIT} ${TEST} ${PKG} 2h ${BENCH_TIME} ${WORK_DIR} on core #3" | tee ${OUT_FILE}
git --no-pager log --oneline -n 10 | tee -a ${OUT_FILE}

echo "-------- BEGIN BENCHMARK --------" | tee -a ${OUT_FILE}

# Alternatively add -memprofile ${TEST_DIR}/${DATE}.memprofile.out but only one package at the time allowed.
# Pinned to Core #3
taskset -c 3 go test -bench=${TEST} -run=^$ -cpuprofile ${CPU_PROF_FILE} -memprofile ${MEM_PROF_FILE} -benchmem -timeout 2h -benchtime ${BENCH_TIME} ${PKG} | tee -a ${OUT_FILE}
