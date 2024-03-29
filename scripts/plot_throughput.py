import argparse
import csv

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray

# -------------------------------------
# Arguments
# -------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument(
    "result_csv_paths",
    nargs="+",
    help="the `result.csv` files that are result of a Benchbase execution",
)
parser.add_argument(
    "-o", "--output", help="name of output file", required=False, default=None
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

mpl.rcParams["font.family"] = "Inter"

NUM_COLORS = len(aggregator)
LINE_STYLES = ["solid", "dashed", "dashdot", "dotted"]
NUM_STYLES = len(LINE_STYLES)

cm = plt.get_cmap("gist_rainbow")
ax = plt.gca()

for i, (key, [count, times, throughputs]) in enumerate(aggregator.items()):
    lines = ax.plot(times, throughputs / count, label=key)
    # lines[0].set_color(cm(i // NUM_STYLES * float(NUM_STYLES) / NUM_COLORS))
    # lines[0].set_linestyle(LINE_STYLES[i % NUM_STYLES])

# plt.title("Throughput chart")
plt.legend()
plt.xlabel("Time (s)")
plt.ylabel("Throughput (txn/s)")

ax.set_ylim([0, 2000])  # Adjust the maximum as necessary.
ax.set_xlim([0, 295])  # Adjust the maximum as necessary.

fig = plt.gcf()
fig.set_size_inches(8, 4)

if args.output is not None:
    plt.savefig(args.output, bbox_inches="tight")
else:
    plt.show()
