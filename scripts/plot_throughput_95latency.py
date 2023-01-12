import csv
import matplotlib.pyplot as plt
import sys

# Get the paths to the CSV files from the command line arguments
if len(sys.argv) < 2:
    print("Error: Please specify the paths to the CSV files as command line arguments.")
    sys.exit(1)

csv_path = sys.argv[1]

# Read in the data from each CSV file and plot it as a separate line

with open(csv_path, 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Skip the first line
    data = [(float(row[0]), float(row[2]), float(row[9])) for row in reader]

# Unpack the data into separate lists
times, throughputs, latencies = zip(*data)

# Plot the latency data on the primary y-axis
plt.plot(times, latencies, color='C1', label=f'95th percentile latency')

# Plot the throughput data on the secondary y-axis
ax2 = plt.gca().twinx()
ax2.plot(times, throughputs, label=f'Throughput')

# Add a legend and show the plot
plt.legend()
plt.xlabel('Time (s)')
plt.show()