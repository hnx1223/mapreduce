import concurrent.futures
from collections import defaultdict


class MapReduce:
    def __init__(self):
        self.passenger_count = defaultdict(int)

    # map passenger data to key-value
    def map(self, passenger_data):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            mapped_data = list(executor.map(self.map_passenger, passenger_data))
        print('mapped data:', mapped_data)
        return mapped_data

    def map_passenger(self, row):
        passenger_id = row[0]
        return (passenger_id, 1)

    # Receives mapping data and divides it into different partitions based on the hash value of the passenger ID
    def partition(self, mapped_data, num_partitions):
        partitions = [[] for _ in range(num_partitions)]
        for passenger_id, count in mapped_data:
            partition_id = hash(passenger_id) % num_partitions
            partitions[partition_id].append((passenger_id, count))
        print('partitions:', partitions)
        return partitions

    # Receive the partition data and create an empty dictionary to store the shuffled data
    def shuffle(self, partitions):
        shuffled_data = defaultdict(list)
        # Create a thread pool for concurrent execution on each partition
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Add passenger IDs and counts to the appropriate lists in the shuffle data
            for partition in partitions:
                executor.submit(self.shuffle_partition, partition, shuffled_data)
            print('shuffled data:', shuffled_data)
        return shuffled_data

    def shuffle_partition(self, partition, shuffled_data):
        for passenger_id, count in partition:
            shuffled_data[passenger_id].append(count)

    # The number of flights per passenger is added up to get the result after reduction
    def reduce(self, combined_data):
        reduced_data = defaultdict(int)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for passenger_id, count in combined_data.items():
                executor.submit(self.reduce_passenger, passenger_id, count, reduced_data)
        print('reduced data:', reduced_data)
        return reduced_data

    def reduce_passenger(self, passenger_id, count, reduced_data):
        reduced_data[passenger_id] = sum(count)

    # find the passenger having had the highest number of flights
    def find_max_flight_count(self, passenger_count):
        max_passenger_id = max(passenger_count, key=passenger_count.get)
        max_flight_count = passenger_count[max_passenger_id]
        return {
            'passenger_id': max_passenger_id,
            'flight_count': max_flight_count
        }

    def run(self, passenger_data):
        # Map phase
        mapped_data = self.map(passenger_data)

        # Partition phase
        num_partitions = 4  # Number of partitions
        partitions = self.partition(mapped_data, num_partitions)

        # Shuffle phase
        shuffled_data = self.shuffle(partitions)

        # Reduce phase
        reduced_data = self.reduce(shuffled_data)

        # Find the passenger with the highest number of flights
        max_passenger = self.find_max_flight_count(reduced_data)

        return max_passenger
