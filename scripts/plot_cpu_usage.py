import sys
import matplotlib.pyplot as plt
import csv

# Get the paths to the CSV files from the command line arguments
if len(sys.argv) < 2:
    print("Error: Please specify the paths to the CSV files as command line arguments.")
    sys.exit(1)

csv_path = sys.argv[1]

# Open the CSV file and read in the data
with open(csv_path, "r") as f:
    reader = csv.reader(f)
    next(reader)
    next(reader)
    next(reader)
    next(reader)
    next(reader)
    header = next(reader)  # get the header row
    data = [row for row in reader]

# Extract the CPU usage column from the data
cpu_usage = [100 - float(row[header.index("idl")]) for row in data]
disk_usage = [float(row[header.index("writ")]) / 1024 for row in data]

# Plot the CPU usage
plt.plot(cpu_usage, label="CPU usage")
ax = plt.gca()
ax.set_ylabel("CPU usage (%)")

ax2 = plt.gca().twinx()
ax2.plot(disk_usage, color="C1", label=f"Disk usage")
ax2.set_ylabel("Disk usage (KB written)")

plt.legend()
plt.xlabel("Time")
plt.show()
