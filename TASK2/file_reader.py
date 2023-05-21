import csv


def file_reader(file):
    passenger_data = []
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for line in reader:
            passenger_data.append(line)

    return passenger_data
