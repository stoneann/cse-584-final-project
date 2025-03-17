import click
import sys

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

    # fills a table
    def generate_table(self, table, cardinality, mapping):
        id = 1
        num_in_mapping = 0
        self.curr_bytes = sys.getsizeof(id) + sys.getsizeof(id - num_in_mapping)
        while len(table) < cardinality:
            # TODO: Change implementation to ensure that the join index doesn't go out of bounds
            self.add_row(table, id, id - num_in_mapping)
            id = id + 1
            num_in_mapping = ((num_in_mapping + 1) % mapping)

    def generate_data(self):
        # Initialize Variables
        left = []
        right = []

        # Add Rows
        self.generate_table(left, self.ltc, self.ltm)
        self.generate_table(right, self.rtc, self.rtm)

        # Save to CSV
        for row in left:
            # print(row)
            print(sys.getsizeof(row))
            print(sys.getsizeof(row['id']), sys.getsizeof(row['join_id']), sys.getsizeof(row['random_string']))

        # for row in right:
    #     print(row)


@click.command()
@click.option('--left-table-cardinality', '-ltc', type=click.INT, default=100, help='Cardinality of Left Table')
@click.option('--right-table-cardinality', '-ltc', type=click.INT, default=100, help='Cardinality of Right Table')
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