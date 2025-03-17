import click
import sys
import os 
import csv

# Class to generate benchmarking data
class GenerateData:
    def __init__(self, ltc, rtc, ltm, rtm, rbs):
        self.ltc = ltc
        self.rtc = rtc
        self.ltm = ltm
        self.rtm = rtm
        self.rbs = rbs
        self.curr_bytes = 0

    # Generate a row with an id, join id, and a random string to get the record bytes to the desired amount
    def add_row(self, table, id, join_id):
        table.append({'id': id, 'join_id': join_id, 'random_string': bytes(self.rbs - self.curr_bytes)})

    # Creates the directory and writes the data to the file
    def create_directory_and_write_csv(self, dir_name, file_name, data):
        if len(data) > 0:
            # Create the directory if it doesn't exist
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)

            # Construct the full file path
            file_path = os.path.join(dir_name, file_name)

            # Open the file in write mode and write content
            with open(file_path, 'w') as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writerows(data)


    def export_to_file(self, table, l_or_r, num):
        self.create_directory_and_write_csv(f"benchmark-datasets/L{self.ltc}_R{self.rtc}_M{self.ltm}-{self.rtm}_RS{self.rbs}/{l_or_r}", f"{num}.csv", table)
        table.clear()

    # fills a table
    def generate_table(self, cardinality, mapping, l_or_r):
        table = []
        id = 1
        num_in_mapping = 0
        file_num = 1
        self.curr_bytes = sys.getsizeof(id) + sys.getsizeof(id - num_in_mapping)
        while id < cardinality:
            # TODO: Change implementation to ensure that the join index doesn't go out of bounds
            self.add_row(table, id, id - num_in_mapping)
            id = id + 1
            num_in_mapping = ((num_in_mapping + 1) % mapping)
            # if we have 100 MB, write table to file and clear it
            if len(table) * self.rbs >= 100000:
                self.export_to_file(table, l_or_r, file_num)
                file_num = file_num + 1
        self.export_to_file(table, l_or_r, file_num)


    def generate_data(self):
        # Add Rows
        self.generate_table(self.ltc, self.ltm, "L")
        self.generate_table(self.rtc, self.rtm, "R")


@click.command()
@click.option('--left-table-cardinality', '-ltc', type=click.INT, default=100, help='Cardinality of Left Table')
@click.option('--right-table-cardinality', '-rtc', type=click.INT, default=100, help='Cardinality of Right Table')
@click.option('--left-table-mapping', '-ltm', type=click.INT, default=1, help='Number of records in left table that joins to right.')
@click.option('--right-table-mapping', '-rtm', type=click.INT, default=1, help='Number of records in right table that joins to left.')
@click.option('--record-byte-size', '-rbs', type=click.INT, default=1000, help='Size of an individual record in bytes.')
def main(left_table_cardinality, right_table_cardinality, left_table_mapping, right_table_mapping, record_byte_size):
    """
    A program to generate a dataset
    """
    click.echo(f"Generating a dataset that with tables of cardinality {left_table_cardinality} and {right_table_cardinality}.")
    click.echo(f"It will create a {left_table_mapping} - {right_table_mapping} mapping.")
    generate_data = GenerateData(left_table_cardinality, right_table_cardinality, left_table_mapping, right_table_mapping, record_byte_size)
    generate_data.generate_data()


if __name__ == '__main__':
    main()