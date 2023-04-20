## Usage

This script is a data generator that generates rows of data for a given database schema and table.

To use the script, run:

python script_name.py --schema <schema_name> --table <table_name> --count <number_of_rows_to_generate> [--batch_size <batch_size>] [--sample <column_name>]

### Arguments

* `--schema`: The name of the schema in the database.
* `--table`: The name of the table in the schema. If you want to generate data for all tables in the schema, enter 'all_tables'.
* `--count`: The number of rows to generate.
* `--batch_size` (optional): The number of rows to generate in each batch. The default value is 100,000 rows per batch.
* `--sample` (optional): The name of the column(s) to sample. This argument works with multiple columns. To add multiple columns, use `--sample` followed by the name of the column.

### Example

python data_generator.py --schema test_schema --table test_table --count 1000000 --batch_size 50000 --sample column_1 --sample column_2

This command will generate 1,000,000 rows of data for 'test_table' in 'test_schema' in batches of 50,000 rows per batch. The script will also sample data from 'column_1' and 'column_2'.
