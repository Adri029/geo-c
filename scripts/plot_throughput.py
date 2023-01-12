import csv
import matplotlib.pyplot as plt
import sys

# Get the paths to the CSV files from the command line arguments
if len(sys.argv) < 2:
    print("Error: Please specify the paths to the CSV files as command line arguments.")
    sys.exit(1)

csv_paths = sys.argv[1:]

# Read in the data from each CSV file and plot it as a separate line
for i, path in enumerate(csv_paths):
    with open(path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the first line
        data = [(float(row[0]), float(row[2])) for row in reader]

    # Unpack the data into separate lists
    times, throughputs = zip(*data)

    # Plot the data
    plt.plot(times, throughputs, label=f"File {i+1}")

plt.title(", ".join(csv_paths[0].split("/")[-4:-1]))

# Add a legend and show the plot
plt.legend()
plt.xlabel("Time (s)")
plt.ylabel("Throughput (txn/s)")
plt.show()
