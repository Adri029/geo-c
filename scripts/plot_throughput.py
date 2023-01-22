import argparse
import csv
import sys

import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray

# -------------------------------------
# Arguments
# -------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument("result_csv_paths", nargs="+")
parser.add_argument(
    "-o", "--output", help="Name of output file", required=False, default=None
)

args = parser.parse_args()

# -------------------------------------
# Parsing
# -------------------------------------

aggregator: dict[str, tuple[int, NDArray, NDArray]] = {}
"""Aggregates results by type of test, the result being an average of them. The
key is the type of test and the value is a tuple composed of (#tests, times,
throughputs)."""

for i, path in enumerate(args.result_csv_paths):
    with open(path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the first line, the headers.
        data = [(float(row[0]), float(row[1])) for row in reader]

    times, throughputs = zip(*data)

    # The name is composed of the database, benchmark and type of test ran.
    name = ", ".join(path.split("/")[-4:-1])

    if val := aggregator.get(name):
        # Some tests count the final second, that's why sometimes the length of
        # the arrays does not match perfectly. To avoid mismatching array
        # shapes, we reduce the length of one of them to be as short as the
        # shorter.
        length = min(len(val[1]), len(throughputs))

        aggregator[name] = (
            val[0] + 1,
            val[1][:length],
            val[2][:length] + np.array(throughputs)[:length],
        )
    else:
        aggregator[name] = (1, np.array(times), np.array(throughputs))

# -------------------------------------
# Plotting
# -------------------------------------

for key, [count, times, throughputs] in aggregator.items():
    plt.plot(times, throughputs / count, label=key)

plt.title("Throughput chart")
plt.legend()
plt.xlabel("Time (s)")
plt.ylabel("Throughput (txn/s)")

if args.output is not None:
    plt.savefig(args.output, bbox_inches="tight")
else:
    plt.show()
