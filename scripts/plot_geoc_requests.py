import argparse
import json
from typing import Any, Final

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray

# -------------------------------------
# Arguments
# -------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument(
    "json_paths",
    nargs="+",
    help="""the histogram JSON files that are result of a Benchbase execution
         (if it has the `-jh` flag active)""",
)
parser.add_argument(
    "-o", "--output", help="Name of output file", required=False, default=None
)

args = parser.parse_args()

# -------------------------------------
# Parsing helpers
# -------------------------------------

TRANSACTIONS: Final[list[str]] = [
    "ApproveCart",
    "Payment",
    "OrderStatus",
    "Delivery",
    "StockLevel",
    "IncreaseCartLine",
    "DecreaseCartLine",
    "CheckCart",
    "Restock",
]


def parse_requests(requests: dict[str, Any], type: str) -> NDArray:
    result = np.repeat(0, len(TRANSACTIONS))
    items = requests[type]["HISTOGRAM"].items()

    for (name, count) in items:
        index = get_transaction_index(name)
        result[index] += count

    return result


def get_transaction_index(name: str) -> int:
    return int(name.split("/")[1]) - 1


# -------------------------------------
# Parsing
# -------------------------------------

completed = np.repeat(0, len(TRANSACTIONS))
aborted = np.repeat(0, len(TRANSACTIONS))
rejected = np.repeat(0, len(TRANSACTIONS))
unexpected = np.repeat(0, len(TRANSACTIONS))

for json_path in args.json_paths:
    with open(json_path, "r") as file:
        requests = json.load(file)

    completed += parse_requests(requests, "completed")
    aborted += parse_requests(requests, "aborted")
    rejected += parse_requests(requests, "rejected")
    unexpected += parse_requests(requests, "unexpected")

# Normalise the inputs (0% to 100%).
total = completed + aborted + rejected + unexpected
completed = (completed / total) * 100
aborted = (aborted / total) * 100
rejected = (rejected / total) * 100
unexpected = (unexpected / total) * 100


# -------------------------------------
# Plotting
# -------------------------------------

mpl.rcParams["font.family"] = "Inter"

fig, ax = plt.subplots()
bar_width = 0.8
bar1 = ax.bar(
    TRANSACTIONS,
    completed,
    width=bar_width,
    label="Completed",
    color="#9dd866",
)
bar2 = ax.bar(
    TRANSACTIONS,
    aborted,
    bottom=completed,
    width=bar_width,
    label="Aborted",
    color="#ca472f",
)
bar3 = ax.bar(
    TRANSACTIONS,
    rejected,
    bottom=[i + j for i, j in zip(completed, aborted)],
    width=bar_width,
    label="Rejected",
    color="#f6c85f",
)
bar4 = ax.bar(
    TRANSACTIONS,
    unexpected,
    bottom=[i + j + k for i, j, k in zip(completed, aborted, rejected)],
    width=bar_width,
    label="Unexpected",
    color="#0b84a5",
)

ax.set_ylabel("Percentage of requests (%)")

box = ax.get_position()
ax.set_position([box.x0, box.y0 + box.height * 0.1, box.width, box.height * 0.9])
ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=4)

threshold = 2
for bar in [bar1, bar2, bar3, bar4]:
    labels = [f"{v:.1f}" if v > threshold else "" for v in bar.datavalues]
    ax.bar_label(bar, labels=labels, label_type="center")

splitted_path = args.json_paths[0].split("/")
plt.title(f"GeoC request chart: {splitted_path[-4]}, {splitted_path[-2]}")
plt.xticks(rotation=20, ha="right")

if args.output is not None:
    plt.savefig(args.output, bbox_inches="tight")
else:
    plt.show()
