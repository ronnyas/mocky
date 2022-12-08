import psycopg2
import psycopg2.extras
psycopg2.extras.register_uuid()
from psycopg2 import pool
import argparse
import logging
import asyncio
import sys
import uuid
import random
import string
import datetime
import json
import csv
import os
import subprocess
import time

ok = '\033[92m\u2713\033[0m'
err = '\033[91m\u2717\033[0m'
warn = '\033[93m\u26A0\033[0m'
waiting = '\033[94m\u231B\033[0m'
party = '\033[95m\u1F389\033[0m'
new = '\033[92m\u2719\033[0m'

logging.basicConfig(
    format='%(asctime)s | %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO
)

class Mocky:
    def __init__(self):
        self._database = {}
        self.columns = {}
        self.columns_fk = {}

        self.session_id = f"{datetime.datetime.now().strftime('%Y-%m-%d-%H.%M.%S')}#{uuid.uuid4().hex[:6]}"
        self.store_path = f"data/{self.session_id}"
        self.start_time = time.time()
        os.makedirs(self.store_path, exist_ok=True)

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, db):
        self._database = db
        self.mockdata = {}

        self.connect_db()

    def connect_db(self):
        if not len(self._database):
            logging.error(f"{err} No database configured")
            exit()

        try:
            self.db = pool.SimpleConnectionPool(
                1,
                5,
                host=self._database['host'],
                port=self._database['port'],
                user=self._database['user'],
                password=self._database['password'],
                database=self._database['database'],
                application_name=f"mocky.py",
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=5,
                keepalives_count=5,
                options=f"-c statement_timeout={889120*1000}"
            )

        except Exception as e:
            logging.error(f"{err} Unable to connect to database: {e}")
            exit()
        logging.info(f"{ok} Connected to {self._database.get('host')}")
   
    async def table_exists(self, schema, table):
        query = "select exists (select from pg_tables where schemaname = %s and tablename  = %s)"
        try:
            conn = self.db.getconn()
            cursor = conn.cursor()
            cursor.execute(query, (schema, table))

            if not cursor.fetchone()[0]:
                logging.error(f"{err} Table {table} does not exist")
                return False

            return True
        except Exception as e:
            logging.error(f"{err} Unable to execute query {query}: {e}")
            return False
        finally:
            self.db.putconn(conn)
        
    
    async def get_foreign_keys(self, schema, table):
        query = """
                select
                    a.table_schema as foreign_table_schema,
                    a.table_name as foreign_table_name,
                    a.column_name as foreign_column_name,
                    b.column_name as fk_name
                from
                    information_schema.table_constraints as c 
                join information_schema.key_column_usage as b
                    on c.constraint_name = b.constraint_name
                    and c.table_schema = b.table_schema
                join information_schema.constraint_column_usage as a
                    on a.constraint_name = c.constraint_name
                    and a.table_schema = c.table_schema
                where c.constraint_type = 'FOREIGN KEY' and c.table_name=%s
                and c.table_schema=%s
            """

        try:
            conn = self.db.getconn()
            cursor = conn.cursor()
            cursor.execute(query, (table, schema))
        except Exception as e:
            logging.error(f"{err} Unable to execute query {query}: {e}")
            return False
        finally:
            self.db.putconn(conn)
        
        return cursor.fetchall()

    async def table_has_rows(self, schema, table):
        query = f"select * from {schema}.{table} limit 1"
        try:
            conn = self.db.getconn()
            cursor = conn.cursor()
            cursor.execute(query)
        except Exception as e:
            logging.error(f"{err} Unable to execute query {query}: {e}")
        finally:
            self.db.putconn(conn)
        
        return True if cursor.rowcount else False


    async def get_table_columns(self, schema, table):
        query = f"select column_name, data_type from information_schema.columns where table_schema = '{schema}' and table_name = '{table}'"
        try:
            conn = self.db.getconn()
            cursor = conn.cursor()
            cursor.execute(query)
        except Exception as e:
            logging.error(f"{err} Unable to execute query {query}: {e}")
            return False
        finally:
            self.db.putconn(conn)
        
        return cursor.fetchall()

    async def get_tables_in_schema(self, schema):
        query = f"select table_name from information_schema.tables where table_schema = '{schema}'"
        try:
            conn = self.db.getconn()
            cursor = conn.cursor()
            cursor.execute(query)
        except Exception as e:
            logging.error(f"{err} Unable to execute query {query}: {e}")
            return False
        finally:
            self.db.putconn(conn)
        
        return [table[0] for table in cursor.fetchall()]

    async def verity_status(self, schema, table):
        missing_data = []
        if not await self.table_exists(schema, table):
            logging.error(f"{err} Table {table} does not exist")
            sys.exit()
        
        fkeys = await self.get_foreign_keys(schema, table)
        for fk in fkeys:
            if not await self.table_has_rows(fk[0], fk[1]):
                missing_data.append(fk[1])
            else:
                self.columns_fk[fk[3]] = {
                    'table': fk[1],
                    'column': fk[2]
                }

        if len(missing_data):
            logging.error(f"{err} {table} has foreign keys in {', '.join(missing_data)}, which is missing data. Run dummydata.py first")
            return False
        else:
            columns = await self.get_table_columns(schema, table)
            for column in columns:
                if column not in self.columns_fk:
                    self.columns[column[0]] = {
                        'type': column[1]
                    }
            logging.info(f"{ok} {table} is ready to be filled")
            return True
    
    async def collect_column_samples(self, columns, schema, table, sample):
        for column in columns:
            query = f"select distinct({column[0]}) from {schema}.{table} limit 5"
            try:
                conn = self.db.getconn()
                cursor = conn.cursor()
                cursor.execute(query)
            except Exception as e:
                logging.error(f"{err} Unable to execute query {query}: {e}")
                return False
            finally:
                self.db.putconn(conn)
            
            if cursor.rowcount:
                if column[0] in self.columns_fk:
                    self.columns_fk[column[0]]['sample_data'] = cursor.fetchall()
                else:
                    if sample and column[0] in sample:
                        self.columns[column[0]]['sample_data'] = cursor.fetchall()
            else:
                logging.error(f"{err} Unable to collect sample data for {column[0]}. Will generate random data based on data type")
                return False

    def use_random_sample(self, sample_data):
        return random.choice(sample_data)[0]

    def generate_random_data(self, type):
        supported_types = {
            'tinyint': lambda: random.randint(0, 255),
            'integer': lambda: random.randint(1, 100000),
            'bigint': lambda: random.randint(1, 100000),
            'smallint': lambda: random.randint(1, 100000),
            'numeric': lambda: random.randint(1, 100000),
            'real': lambda: random.randint(1, 100000),
            'double precision': lambda: random.randint(1, 100000),
            'character varying': lambda: ''.join(random.choice(string.ascii_letters) for i in range(10)),
            'text': lambda: ''.join(random.choice(string.ascii_letters) for i in range(10)),
            'timestamp without time zone': lambda: datetime.datetime.now(),
            'timestamp with time zone': lambda: datetime.datetime.now(),
            'time without time zone': lambda: datetime.datetime.now(),
            'time with time zone': lambda: datetime.datetime.now(),
            'date': lambda: datetime.datetime.now(),
            'boolean': lambda: random.choice([True, False]),
            'uuid': lambda: uuid.uuid4(),
            'json': lambda: json.dumps({'foo': 'bar'}),
            'jsonb': lambda: json.dumps({'foo': 'bar'}),
            # 'array': lambda: json.dumps(['foo', 'bar']),
            'bytea': lambda: ''.join(random.choice(string.ascii_letters) for i in range(10)),
        }

        return supported_types[type.lower()]() if type.lower() in supported_types else None

    async def fill_table(self, schema, table, rows, batch_size, samples):
        table_start_time = time.time()
        rows_to_insert = []
        counter = 0

        if table == 'all_tables':
            tables = await self.get_tables_in_schema(schema)
            print(tables)
            logging.info(f"{ok} Filling {len(tables)} tables in {schema} schema")

            for _table in tables:
                logging.info(f"{ok} Filling table {_table}")
                await self.fill_table(schema, _table, rows, batch_size)
                print()
        else:
            if not await self.verity_status(schema, table):
                return False

            columns = await self.get_table_columns(schema, table)

            if columns:
                await self.collect_column_samples(columns, schema, table, samples)
            
            print(self.columns_fk)
            print(self.columns)
            
            for i in range(rows):
                counter += 1
                mockdata = {}

                for column in columns:
                    if column[0] in self.columns_fk:
                        if not self.columns_fk[column[0]].get('sample_data'):
                            sample = self.generate_random_data(column[1])
                        else: 
                            sample = self.use_random_sample(self.columns_fk[column[0]]['sample_data'])
                        mockdata[column[0]] = sample
                    else:
                        if not self.columns[column[0]].get('sample_data'):
                            mockdata[column[0]] = self.generate_random_data(type=column[1])
                        else:
                            mockdata[column[0]] = self.use_random_sample(self.columns[column[0]]['sample_data'])

                            # print data type
                            if column[1] == "jsonb": # jsonb fucks up the csv parser
                                mockdata[column[0]] = json.dumps(mockdata[column[0]])
                            else:
                                mockdata[column[0]] = str(mockdata[column[0]])
            
                rows_to_insert.append(mockdata)

                if counter % batch_size == 0:
                    asyncio.ensure_future(self.insert_rows(schema, table, rows_to_insert, i))
                    rows_to_insert = []
                    counter = 0

            length = len(rows_to_insert)
            if length > 0:
                asyncio.ensure_future(self.insert_rows(schema, table, rows_to_insert, i))
            
            logging.info(f"{ok} Table filled with {rows} rows into {table} after {round(time.time() - table_start_time)} seconds")
            logging.info(f"{ok} Throughput: {round(rows / (time.time() - self.start_time))} rows/second")
    
    async def insert_rows(self, schema, table, rows, i):
        logging.info(f"{ok} Inserting {len(rows)} rows into {table} ({i})")
        
        with open(f'{self.store_path}/{i}.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter='|')
            writer.writeheader()
            writer.writerows(rows)

        path = os.path.dirname(os.path.realpath(__file__))

        try:
            proc = subprocess.Popen(
                    [
                        'psql', 
                        '-h', self._database.get('host'),
                        '-p', str(self._database.get('port')),
                        '-U', self._database.get('user'),
                        '-d', self._database.get('database'),
                        '-c', f"\COPY {schema}.{table} FROM '{path}/{self.store_path}/{i}.csv' DELIMITER '|' CSV HEADER;"
                    ],
                    env={'PGPASSWORD': self._database.get('password')},
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
        except Exception as e:
            logging.error(f"{err} Unable to insert rows into {table}: {e}")
            return False
        else:
            return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema', type=str, help='Schema name', required=True)
    parser.add_argument('--table', type=str, help='Table name. Write all_tables for all tables within specified schema.', required=True)
    parser.add_argument('--count', type=int, help='Number of rows to generate', required=True)
    parser.add_argument('--batch_size', type=int, default=100_000, help='Number of rows for every batch', required=False)
    parser.add_argument('--sample', action='append', help='Sample the data from this column. Works with multiple --samlpe=<column>', required=False)

    args = parser.parse_args()

    mocky = Mocky()

    mocky.database = {
        'host': 'localhost',
        'port': 5432,
        'user': '',
        'password': '',
        'database': '',
    }

    asyncio.run(
        mocky.fill_table(
            args.schema,
            args.table,
            args.count,
            args.batch_size,
            args.sample
        )
    )

if __name__ == '__main__':
    main()
