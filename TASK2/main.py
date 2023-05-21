from file_reader import file_reader

from map_reduce import MapReduce


def main():
    # read file data
    passenger_data = file_reader('data/AComp_Passenger_data_no_error.csv')

    # create a MapReduce instance
    mapreduce = MapReduce()

    # get result
    result = mapreduce.run(passenger_data)

    # output
    print("passenger ID:", result['passenger_id'])
    print("number of flights:", result['flight_count'])


if __name__ == "__main__":
    main()
