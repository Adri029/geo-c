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

    ├── config/         # sample config files
    ├── deployment/     # Ansible scripts to provision PostgreSQL and CockroachDB
    ├── scripts/        # generate graphs for the results
    ├── src/            # source code for BenchBase
    │   └─── main/java/com/oltpbenchmark/benchmarks/geoc    # GeoC implementation
    └── testing/        # Ansible scripts to run automated tests

## Notes

We've deleted some of the top-level directories that were related to CI/CD, containerisation, other benchmark data, etc., in order to tidy up the repo. The original code has been mostly left as it was, as to avoid any potential bugs related to the deletion of the other unrelated benchmarks.

For more info refer to the original BenchBase README.
