import click
import sys
import os 
import csv
import random
import string

# Class to generate benchmarking data
class GenerateData:
    def __init__(self, ltc, rtc, ltm, rtm, rbs, sorted):
        self.ltc = ltc
        self.rtc = rtc
        self.ltm = ltm
        self.rtm = rtm
        self.rbs = rbs
        self.sorted = sorted

    # Generate a row with an id, join id, and a random string to get the record bytes to the desired amount
    def add_row(self, table, id, join_id):
        # Create a random string with the required padding size
        padding_size = self.rbs - (len(str(id)) + len(',')  + len(str(join_id)) + len(','))
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=padding_size))
        table.append({'id': id, 'join_id': join_id, 'random_string': random_string})

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
        s_bool = "F"
        if self.sorted:
            s_bool = "T"
        self.create_directory_and_write_csv(f"benchmark-datasets/L{self.ltc}_R{self.rtc}_M{self.ltm}-{self.rtm}_RS{self.rbs}_S{s_bool}/{l_or_r}", f"{num}.csv", table)
        table.clear()

    # fills a table
    def generate_table(self, cardinality, mapping, l_or_r):
        table = []
        id = 1
        join_ids = [(i // mapping) + 1 for i in range(cardinality)]

        if not self.sorted:
            random.shuffle(join_ids)

        file_num = 1
        while id < (cardinality + 1):
            self.add_row(table, id, join_ids[id - 1])
            id = id + 1
            # if we have 100 MB, write table to file and clear it
            if len(table) * self.rbs >= 100000000:
                self.export_to_file(table, l_or_r, file_num)
                file_num = file_num + 1
        self.export_to_file(table, l_or_r, file_num)


    def generate_data(self):
        # Add Rows
        self.generate_table(self.ltc, self.ltm, "L")
        self.generate_table(self.rtc, self.rtm, "R")


@click.command()
@click.option('--left-table-cardinality', '-ltc', type=click.INT, default=100000, help='Cardinality of Left Table')
@click.option('--right-table-cardinality', '-rtc', type=click.INT, default=100000, help='Cardinality of Right Table')
@click.option('--left-table-mapping', '-ltm', type=click.INT, default=1, help='Number of records in left table that joins to right.')
@click.option('--right-table-mapping', '-rtm', type=click.INT, default=1, help='Number of records in right table that joins to left.')
@click.option('--record-byte-size', '-rbs', type=click.INT, default=1000, help='Size of an individual record in bytes.')
@click.option('--sorted', '-s', type=click.BOOL, default=False, help='If true, dataset will be sorted by join index. If false, datset will be random.')
def main(left_table_cardinality, right_table_cardinality, left_table_mapping, right_table_mapping, record_byte_size, sorted):
    """
    A program to generate a dataset
    """
    click.echo(f"Generating a dataset that with tables of cardinality {left_table_cardinality} and {right_table_cardinality}.")
    click.echo(f"It will create a {left_table_mapping} - {right_table_mapping} mapping.")
    generate_data = GenerateData(left_table_cardinality, right_table_cardinality, left_table_mapping, right_table_mapping, record_byte_size, sorted)
    generate_data.generate_data()


if __name__ == '__main__':
    main()