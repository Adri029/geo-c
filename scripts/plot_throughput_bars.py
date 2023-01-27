import glob
import json
import os
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

mpl.rcParams["font.family"] = "Inter"


def get_means(paths: list[str]) -> list[int]:
    results = []

    for path in paths:
        file_names = glob.glob(os.path.join(path, "*.summary.json"))

        throughput = 0
        count = 0
        for file_name in file_names:
            with open(file_name, "r") as file:
                data = json.load(file)
                throughput += data["Throughput (requests/second)"]
                count += 1

        results.append(throughput / count)

    return results


labels = [1, 2, 4]
root_folder = os.path.join("testing", "remote_results", "results")

crdb1_tpcc_data = get_means(
    [
        os.path.join(root_folder, "cockroachdb", "tpcc", "1wh-10term"),
        os.path.join(root_folder, "cockroachdb", "tpcc", "2wh-20term"),
        os.path.join(root_folder, "cockroachdb", "tpcc", "4wh-40term"),
    ]
)
crdb3_tpcc_data = get_means(
    [
        os.path.join(root_folder, "cockroachdb-triple", "tpcc", "1wh-10term"),
        os.path.join(root_folder, "cockroachdb-triple", "tpcc", "2wh-20term"),
        os.path.join(root_folder, "cockroachdb-triple", "tpcc", "4wh-40term"),
    ]
)
psql_tpcc_data = get_means(
    [
        os.path.join(root_folder, "postgres", "tpcc", "1wh-10term"),
        os.path.join(root_folder, "postgres", "tpcc", "2wh-20term"),
        os.path.join(root_folder, "postgres", "tpcc", "4wh-40term"),
    ]
)
crdb1_geoc_data = get_means(
    [
        os.path.join(root_folder, "cockroachdb", "geoc", "1wh-10term"),
        os.path.join(root_folder, "cockroachdb", "geoc", "2wh-20term"),
        os.path.join(root_folder, "cockroachdb", "geoc", "4wh-40term"),
    ]
)
crdb3_geoc_data = get_means(
    [
        os.path.join(root_folder, "cockroachdb-triple", "geoc", "1wh-10term"),
        os.path.join(root_folder, "cockroachdb-triple", "geoc", "2wh-20term"),
        os.path.join(root_folder, "cockroachdb-triple", "geoc", "4wh-40term"),
    ]
)
psql_geoc_data = get_means(
    [
        os.path.join(root_folder, "postgres", "geoc", "1wh-10term"),
        os.path.join(root_folder, "postgres", "geoc", "2wh-20term"),
        os.path.join(root_folder, "postgres", "geoc", "4wh-40term"),
    ]
)

x = np.arange(len(labels))  # the label locations
width = 0.10  # the width of the bars

fig, ax = plt.subplots()
crdb1_tpcc = ax.bar(
    x - (5 * width / 2), crdb1_tpcc_data, width, label="cockroachdb, tpcc"
)
crdb3_tpcc = ax.bar(
    x - (3 * width / 2), crdb3_tpcc_data, width, label="cockroachdb-triple, tpcc"
)
psql_tpcc = ax.bar(x - (width / 2), psql_tpcc_data, width, label="postgres, tpcc")
crdb1_geoc = ax.bar(x + (width / 2), crdb1_geoc_data, width, label="cockroachdb, geoc")
crdb3_geoc = ax.bar(
    x + (3 * width / 2), crdb3_geoc_data, width, label="cockroachdb-triple, geoc"
)
psql_geoc = ax.bar(x + (5 * width / 2), psql_geoc_data, width, label="postgres, geoc")

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel("Average throughput (txn/s)")
ax.set_xlabel("Number of warehouses")
ax.set_xticks(x, labels)
ax.legend(loc="center right", bbox_to_anchor=(1.77, 0.75))

fig.set_size_inches(4, 3)

# fig.tight_layout()

plt.savefig("txn-bars.pdf", bbox_inches="tight")
# plt.show()
