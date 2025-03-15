import click


def add_row(table, id, join_id):
    table.append({'id': id, 'join_id': join_id})


def generate_table(table, cardinality, mapping):
    id = 1
    num_in_mapping = 0
    while len(table) < cardinality:
        # TODO: Change implementation to ensure that the join index doesn't go out of bounds
        add_row(table, id, id - num_in_mapping)
        id = id + 1
        num_in_mapping = ((num_in_mapping + 1) % mapping)
        

def generate_data(ltc, rtc, ltm, rtm):
    # Initialize Variables
    left = []
    right = []

    # Add Rows
    generate_table(left, ltc, ltm)
    generate_table(right, rtc, rtm)
    # Apply Sort

    # Save to CSV
    for row in left:
        print(row)

    for row in right:
        print(row)


@click.command()
@click.option('--left-table-cardinality', '-ltc', type=click.INT, default=100, help='Cardinality of Left Table')
@click.option('--right-table-cardinality', '-ltc', type=click.INT, default=100, help='Cardinality of Right Table')
# @click.option('--avg-distance', '-ad', type=click.INT, default=1, help="Average Space between sorted records. 1 is ")
@click.option('--left-table-mapping', '-ltm', type=click.INT, default=1, help='Number of records in left table that joins to right.')
@click.option('--right-table-mapping', '-rtm', type=click.INT, default=10, help='Number of records in right table that joins to left.')
def main(left_table_cardinality, right_table_cardinality, left_table_mapping, right_table_mapping):
    """
    A program to generate a dataset
    """
    click.echo(f"Generating a dataset that with tables of cardinality {left_table_cardinality} and {right_table_cardinality}.")
    click.echo(f"It will create a {left_table_mapping} - {right_table_mapping} mapping.")
    generate_data(left_table_cardinality, right_table_cardinality, left_table_mapping, right_table_mapping)

if __name__ == '__main__':
    main()