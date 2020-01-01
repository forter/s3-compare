# s3-compare.py

`s3-compare.py` is a script for comparing very large S3 buckets using S3 inventories and Athena.

## Note
This script *does not* compare the bucket keys' content, only their existence.

## Description (or: why not simply use the list_bucket API?)

We needed to compare *very* large buckets. Using API calls to fetch all key names would
have taken forever and would not have been practical.

Instead, this script creates an athena table for each inventory and then creates a join table
(joining on the keys) between these tables.

Finding missing keys in either bucket is then simply searching for lines where one of the buckets has a
key and the other bucket's column is `null`. (This is done by the script)

The script was developed for our use case which required comparing S3 buckets that hold the tasks
of an event sourcing mechanism between two regions.
These buckets are not replicated by AWS but should be the same due to other replication
mechanisms at work. We needed a way to periodically verify there are no bugs/holes in the flow.

As such, although the script tries being generic enough, there are likely many configuration
options that could be added and exposed.

If you need to solve a similar problem, then this script is very likely a good starting
point for you, but it may need some tweaking.

## Setup

### S3 Inventories

To be able to use this script, you need to have S3 inventories already set up for both
buckets you want to compare.

See [Amazon S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html)
for details on how to do that.

### Python

Requires python `>= 3.6`

To install, run

```shell script
pip install -r requirements.txt   # You may need to use pip3 instead
```

OR

```shell script
pip install boto3 pyathena     # You may need to use pip3 instead
```

## Usage

See `./s3-compare.py --help` for the different options this script requires.

After the script finishes executing, you should find a file named `00-find_table_missing_keys`
in the directory provided as the `--local-workdir` option.
This file will contain all keys that are missing in the `--missing-in` bucket
but exist in the other bucket.
