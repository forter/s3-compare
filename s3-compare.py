#!/usr/bin/env python

import argparse
import concurrent.futures
import pathlib

import boto3
import pyathena


class Bucket:
    def __init__(self, bucket, path):
        self.bucket = bucket
        self.path = path


class Work(Bucket):
    def __init__(
            self,
            athena_region,
            athena_schema,
            athena_query_result_location,
            bucket,
            path,
            local_workdir):
        super().__init__(bucket, path)
        self.athena_query_result_location = athena_query_result_location
        self.athena_schema = athena_schema
        self.athena_region = athena_region
        self.dir = pathlib.Path(local_workdir).expanduser()
        if not self.dir.is_dir():
            self.dir.mkdir(parents=True)

    def run_athena_query(self, sql):
        return list(self.run_athena_query_iter(sql))

    def run_athena_query_iter(self, sql):
        with pyathena.connect(
                s3_staging_dir=self.athena_query_result_location,
                region_name=self.athena_region,
                schema_name=self.athena_schema) as connection:
            cursor = connection.cursor()
            cursor.execute(sql)
            for row in cursor:
                yield row


class Inventory(Bucket):
    def __init__(self, bucket, path, work, compared_bucket):
        super().__init__(bucket, path)
        self.compared_bucket = compared_bucket
        self.work = work
        self.s3 = boto3.client('s3')
        table_name_suffix = self.compared_bucket.replace('-', '_').replace('.', '_')
        self.table_name = f's3_inventory_{table_name_suffix}'
        self.work_path = f'{work.path}/{self.compared_bucket}'
        normalized_path = self.path.replace('/', '-')
        symlink_prefix = f'{work.dir}/{normalized_path}'
        self.local_src_symlink_path = f'{symlink_prefix}-src-symlink.txt'
        self.local_dst_symlink_path = f'{symlink_prefix}-dst-symlink.txt'

    def get_latest_partition(self):
        response = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix=f'{self.path}/hive/'
        )
        assert not response['IsTruncated']
        return sorted([obj['Key'] for obj in response['Contents']])[-1]

    def download_partition_symlink(self, partition_symlink):
        self.s3.download_file(
            Bucket=self.bucket,
            Key=partition_symlink,
            Filename=self.local_src_symlink_path
        )

    def copy_latest_partition_to_work_area(self, partition_symlink, num_workers=100):
        with concurrent.futures.ThreadPoolExecutor(num_workers) as executor, \
                open(self.local_src_symlink_path) as f_src, \
                open(self.local_dst_symlink_path, 'w') as f_dst:
            futures = []
            for s3_path in f_src:
                s3_path = s3_path.strip()
                if not s3_path:
                    continue
                new_s3_path = (
                    s3_path.replace(self.bucket, self.work.bucket)
                           .replace(self.path, self.work_path)
                )
                f_dst.write(f'{new_s3_path}\n')
                futures.append(executor.submit(self.copy_s3, s3_path, new_s3_path))
            for future in futures:
                future.result()
        dst_symlink = partition_symlink.replace(self.path, self.work_path)
        print(f'Upload modified hive symlink {dst_symlink}')
        self.s3.upload_file(
            Filename=self.local_dst_symlink_path,
            Bucket=self.work.bucket,
            Key=dst_symlink,
        )

    def copy_s3(self, src_s3_path, dst_s3_path):
        print(f'Copy {src_s3_path} => {dst_s3_path}')
        src_s3_path = src_s3_path.replace('s3://', '')
        dst_s3_path = dst_s3_path.replace('s3://', '')
        src_bucket, src_key = src_s3_path.split('/', 1)
        dst_bucket, dst_key = dst_s3_path.split('/', 1)
        self.s3.copy_object(
            CopySource={
                'Bucket': src_bucket,
                'Key': src_key,
            },
            Bucket=dst_bucket,
            Key=dst_key
        )

    def create_athena_table(self):
        table_name = self.table_name
        print(f'Creating {table_name}')
        self.work.run_athena_query(f'DROP TABLE IF EXISTS {table_name}')
        self.work.run_athena_query(f'''
            CREATE EXTERNAL TABLE {table_name} (
                `bucket` string,
                key string
            )
            PARTITIONED BY (dt string)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
            LOCATION 's3://{self.work.bucket}/{self.work_path}/hive/'
        ''')
        self.work.run_athena_query(f'MSCK REPAIR TABLE {table_name}')


class Compare(object):
    def __init__(self, work, left_inventory, right_inventory):
        self.work = work
        self.left_inventory = left_inventory
        self.right_inventory = right_inventory
        self.inventories = [left_inventory, right_inventory]

    def setup(self):
        self.copy_inventories_to_work_area()
        self.create_inventories_athena_tables()

    def copy_inventories_to_work_area(self):
        for inventory in self.inventories:
            latest_partition = inventory.get_latest_partition()
            inventory.download_partition_symlink(latest_partition)
            inventory.copy_latest_partition_to_work_area(latest_partition)

    def create_inventories_athena_tables(self):
        for inventory in self.inventories:
            inventory.create_athena_table()

    def create_inventories_join_table(self, join_type):
        table_name = self.join_table_name(join_type)
        print(f'Creating join table {table_name}')
        self.work.run_athena_query(f'DROP TABLE IF EXISTS {table_name}')
        self.work.run_athena_query(f'''
            CREATE TABLE {table_name}
            WITH (format='PARQUET') AS
            SELECT
              table1.key AS table1_key, table2.key AS table2_key
            FROM
              {self.left_inventory.table_name} table1
            {join_type} JOIN
              {self.right_inventory.table_name} table2
            USING (key)
        ''')

    def find_missing_keys(self, join_type):
        table_name = self.join_table_name(join_type)
        if join_type.lower() == 'left':
            select_key, null_key = 'table1_key', 'table2_key'
        else:
            select_key, null_key = 'table2_key', 'table1_key'
        query = f'''
            SELECT {select_key} AS key FROM {table_name}
            WHERE {null_key} IS NULL
        '''
        for entry in self.work.run_athena_query_iter(query):
            yield entry

    def join_table_name(self, join_type):
        join_type = join_type.lower()
        return 's3_inventory_join_{}_{}_{}'.format(
            join_type,
            self.left_inventory.compared_bucket.replace('-', '_').replace('.', '_'),
            self.right_inventory.compared_bucket.replace('-', '_').replace('.', '_'),
        )


class Runner(object):
    def __init__(
            self,
            work_bucket,
            left_inventory_bucket,
            left_inventory_path,
            left_compared_bucket,
            right_inventory_bucket,
            right_inventory_path,
            right_compared_bucket,
            work_path,
            local_workdir,
            athena_query_result_location,
            athena_schema,
            athena_region):
        work = Work(
            athena_query_result_location=athena_query_result_location,
            athena_schema=athena_schema,
            athena_region=athena_region,
            bucket=work_bucket,
            path=work_path,
            local_workdir=local_workdir,
        )
        self.compare = Compare(
            work=work,
            left_inventory=Inventory(
                work=work,
                bucket=left_inventory_bucket,
                path=left_inventory_path,
                compared_bucket=left_compared_bucket,
            ),
            right_inventory=Inventory(
                work=work,
                bucket=right_inventory_bucket,
                path=right_inventory_path,
                compared_bucket=right_compared_bucket,
            ),
        )
        self.filters = [
            self.find_table_missing_keys
        ]

    def run(self, missing_in, skip_setup, skip_create_join_table):
        join_type = 'left' if missing_in == 'right' else 'right'
        if not skip_setup:
            self.compare.setup()
        if not skip_create_join_table:
            self.compare.create_inventories_join_table(join_type)
        previous_output_path = None
        for index, fn in enumerate(self.filters):
            name = f'{index:02d}-{fn.__name__}'
            output_path = self.compare.work.dir / name
            print(f'Running {name}')
            fn(
                join_type=join_type,
                input_path=previous_output_path,
                output_path=output_path
            )
            previous_output_path = output_path

    def find_table_missing_keys(self, join_type, output_path, **_):
        with open(output_path, 'w') as f:
            for entry in self.compare.find_missing_keys(join_type):
                f.write(f'{entry[0]}\n')


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--missing-in',
        required=True,
        choices=['left', 'right'],
        help='Which of the two buckets should be checked for missing keys (compared '
             'to the other bucket)',
    )
    parser.add_argument(
        '--skip-setup',
        action='store_true',
        help='Skip the S3 copy phase and athena preparation steps. Useful in case the '
             'script was executed already with `--missing-in` of one value (e.g. left), '
             'and you wish to check with `--missing-in` of the other (e.g. right)'
    )
    parser.add_argument(
        '--skip-create-join-table',
        action='store_true',
        help='Skip the join table creation phase'
    )
    parser.add_argument(
        '--left-compared-bucket',
        required=True,
        help='The name of the left bucket to be compared',
    )
    parser.add_argument(
        '--left-inventory-bucket',
        required=True,
        help='The name of the left inventory bucket (the bucket containing the actual '
             'inventory files for `left-compared-bucket`)',
    )
    parser.add_argument(
        '--left-inventory-path',
        required=True,
        help='The path within `left-inventory-bucket` where the inventory files are '
             'located. Should be a directory containing the `hive` directory.',
    )
    parser.add_argument(
        '--right-compared-bucket',
        required=True,
        help='The name of the right bucket to be compared',
    )
    parser.add_argument(
        '--right-inventory-bucket',
        required=True,
        help='The name of the right inventory bucket (the bucket containing the actual '
             'inventory files for `right-compared-bucket`)',
    )
    parser.add_argument(
        '--right-inventory-path',
        required=True,
        help='The path within `right-inventory-bucket` where the inventory files are '
             'located. Should be a directory containing the `hive` directory.',
    )
    parser.add_argument(
        '--work-bucket',
        required=True,
        help='A bucket name that should be used for internal work purposes',
    )
    parser.add_argument(
        '--work-path',
        required=True,
        help='A path within `work-bucket` to place all work files under',
    )
    parser.add_argument(
        '--local-workdir',
        required=True,
        help='A path to a local directory that can be used as a working directory',
    )
    parser.add_argument(
        '--athena-query-result-location',
        required=True,
        help='e.g. s3://query-results-bucket/folder/'
    )
    parser.add_argument('--athena-schema', default='default')
    parser.add_argument('--athena-region')
    return vars(parser.parse_args())


def main():
    args = parse_arguments()
    missing_in = args.pop('missing_in')
    skip_setup = args.pop('skip_setup')
    skip_create_join_table = args.pop('skip_create_join_table')
    runner = Runner(**args)
    runner.run(
        missing_in=missing_in,
        skip_setup=skip_setup,
        skip_create_join_table=skip_create_join_table,
    )


if __name__ == '__main__':
    main()
