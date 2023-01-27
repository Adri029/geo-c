[benchbase]: https://github.com/cmu-db/benchbase

# GeoC (with BenchBase)

This repo holds a fork of [BenchBase][benchbase] with the GeoC benchmark, a workload designated for geo-replicated database systems.

## Requirements

- Java 17 or newer

## Usage

Create a database named `benchbase` on your DBMS, e.g. for PostgreSQL:

    createdb -U postgres benchbase

If you want to run everything in a single command (replace parameters inside `<>`):

    ./mvnw clean compile exec:java -P <database> -Dexec.args="-b <benchmark> -c <config_path> --create=true --clear=true --load=true --execute=true"

## Repository Structure

The relevant folders/files for GeoC are as follows:

    config/         # sample config files
    deployment/     # Ansible scripts to provision PSQL and CRDB
    scripts/        # generate graphs for the results
    src/            # source code for BenchBase
    testing/        # Ansible scripts to run automated tests
