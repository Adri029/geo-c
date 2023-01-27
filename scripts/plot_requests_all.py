import argparse
import glob
import json
import os
from typing import Any, Final

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray

mpl.rcParams["font.family"] = "Inter"

# -------------------------------------
# Arguments
# -------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument(
    "root_folder",
    help="the folder where the results are stored (i.e. `results`)",
)
parser.add_argument(
    "results_type",
    help="the type of benchmark results (i.e. `1wh-10term`)",
)
parser.add_argument(
    "-o", "--output", help="Name of output file", required=False, default=None
)

args = parser.parse_args()

# -------------------------------------
# Parsing helpers
# -------------------------------------

GEOC_TRANSACTIONS: Final[list[str]] = [
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

TPCC_TRANSACTIONS: Final[list[str]] = [
    "NewOrder",
    "Payment",
    "OrderStatus",
    "Delivery",
    "StockLevel",
]

transactions = []  # Will change as the plots are plotted.


def parse_requests(requests: dict[str, Any], type: str) -> NDArray:
    result = np.repeat(0, len(transactions))
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

nrows, ncols = (3, 2)
fig, axs = plt.subplots(nrows, ncols)
charts = [
    ("postgres", "tpcc", (0, 0)),
    ("cockroachdb", "tpcc", (1, 0)),
    ("cockroachdb-triple", "tpcc", (2, 0)),
    ("postgres", "geoc", (0, 1)),
    ("cockroachdb", "geoc", (1, 1)),
    ("cockroachdb-triple", "geoc", (2, 1)),
]

for (database, benchmark, index) in charts:
    transactions = GEOC_TRANSACTIONS if benchmark == "geoc" else TPCC_TRANSACTIONS

    completed = np.repeat(0, len(transactions))
    aborted = np.repeat(0, len(transactions))
    rejected = np.repeat(0, len(transactions))
    unexpected = np.repeat(0, len(transactions))
    json_paths = glob.glob(
        os.path.join(
            args.root_folder,
            database,
            benchmark,
            args.results_type,
            "*-histograms.json",
        ),
        recursive=True,
    )

    for json_path in json_paths:
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

    bar_width = 0.8
    bar1 = axs[index].bar(
        transactions,
        completed,
        width=bar_width,
        label="Completed",
        color="#b1d760",
    )
    bar2 = axs[index].bar(
        transactions,
        aborted,
        bottom=completed,
        width=bar_width,
        label="Aborted",
        color="#f67e6d",
    )
    bar3 = axs[index].bar(
        transactions,
        rejected,
        bottom=[i + j for i, j in zip(completed, aborted)],
        width=bar_width,
        label="Rejected",
        color="#f9b65d",
    )
    bar4 = axs[index].bar(
        transactions,
        unexpected,
        bottom=[i + j + k for i, j, k in zip(completed, aborted, rejected)],
        width=bar_width,
        label="Unexpected",
        color="#7caed5",
    )

    axs[index].set_ylabel("Requests (%)")

    # box = axs[index].get_position()
    # axs[index].set_position(
    #     [box.x0, box.y0 + box.height * 0.1, box.width, box.height * 0.9]
    # )

    threshold = 4
    for bar in [bar1, bar2, bar3, bar4]:
        labels = [f"{v:.1f}" if v > threshold else "" for v in bar.datavalues]
        axs[index].bar_label(bar, labels=labels, label_type="center")

    axs[index].set_ylim([0, 100])
    axs[index].set_title(f"{database} / {benchmark} / {args.results_type}")
    # axs[index].tick_params(labelrotation=25, ha="right")
    plt.setp(axs[index].get_xticklabels(), rotation=25, ha="right", rotation_mode="anchor")

plt.subplots_adjust(wspace=0.05)
plt.legend(loc="center right", bbox_to_anchor=(1.35, 1.75))
  
for ax in axs.flat:
    ax.label_outer()

fig.set_size_inches(12, 6)

if args.output is not None:
    plt.savefig(args.output, bbox_inches="tight")
else:
    plt.show()
