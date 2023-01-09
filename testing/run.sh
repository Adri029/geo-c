#!/bin/bash

# Before running any tests, don't forget to:
# - start the database instance on the target(s) machine(s)
# - stop any other database systems

# Variables to change!

BENCHMARK=tpcc
DATABASE=postgres
RUNS=5

INVENTORY="inventories/postgres_tester.ini"

GENERATE=True
EXECUTE=True

# Run the playbook.

for i in $(seq $RUNS); do
    # If executing more than one run, sleep in-between tests.
    if [ $i -gt 1 ]; then
        echo "------------------------"
        echo "Sleeping before test $i!"
        echo "------------------------"
        sleep 1m
    fi

    ansible-playbook -i $INVENTORY \
        --extra-vars="database=$DATABASE benchmark=$BENCHMARK generate=$GENERATE execute=$EXECUTE" \
        playbook.yml
done
