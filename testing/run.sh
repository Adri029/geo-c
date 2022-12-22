#!/bin/bash

# Before running any tests, don't forget to:
# - start the database instance on the target(s) machine(s)
# - stop any other database systems

# Variables to change!

BENCHMARK=tpcc
DATABASE=postgres

INVENTORY="inventories/postgres_tester.ini"

GENERATE=True
EXECUTE=True

# Run the playbook.

ansible-playbook -i $INVENTORY \
    --extra-vars="database=$DATABASE benchmark=$BENCHMARK generate=$GENERATE execute=$EXECUTE" \
    playbook.yml
