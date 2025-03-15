import click

@click.command()
@click.option('--left-table-cardinality', '-ltc', type=click.INT, default=100, help='Cardinality of Left Table')
@click.option('--right-table-cardinality', '-ltc', type=click.INT, default=100, help='Cardinality of Right Table')
# @click.option('--avg-distance', '-ad', type=click.INT, default=1, help="Average Space between sorted records. 1 is ")
@click.option('--left-table-mapping', '-ltm', type=click.INT, default=1, help='Number of records in left table that joins to right.')
@click.option('--right-table-mapping', '-rtm', type=click.INT, default=10, help='Number of records in right table that joins to left.')
def main(left_table_cardinality, right_table_cardinality, avg_distance, left_table_mapping, right_table_mapping):
    """
    A program to generate a dataset
    """
    click.echo(f"Generating a dataset that with tables of cardinality {left_table_cardinality} and {right_table_cardinality}.")
    click.echo(f"It will create a {left_table_mapping} - {right_table_mapping} mapping.")

if __name__ == '__main__':
    main()