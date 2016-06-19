## Introduction

[![Build Status](https://travis-ci.org/obsrvbl/flowlogs-reader.svg?branch=master)](https://travis-ci.org/obsrvbl/flowlogs-reader)
[![Coverage Status](https://coveralls.io/repos/obsrvbl/flowlogs-reader/badge.svg?branch=master&service=github)](https://coveralls.io/github/obsrvbl/flowlogs-reader?branch=master)
[![PyPI Version](https://img.shields.io/pypi/v/flowlogs_reader.svg)](https://pypi.python.org/pypi/flowlogs_reader)

Amazon's VPC Flow Logs are analagous to NetFlow and IPFIX logs, and can be used for security and performance analysis.
[Observable Networks](https://observable.net) uses VPC Flow logs as an input to endpoint modeling for security monitoring.

This project contains a Python library that makes retrieving VPC Flow Logs from Amazon CloudWatch Logs a bit easier. The library provides:

* A data structure that parses the Flow Log records into easily-used Python objects.
* A utility that makes iterating over all the Flow Log records in a log group very simple.

The library builds on [boto3](https://github.com/boto/boto3) and should work on both Python 2.7 and 3.4+.

For information on VPC Flow Logs and how to enable them see [this post](https://aws.amazon.com/blogs/aws/vpc-flow-logs-log-and-view-network-traffic-flows/) at the AWS blog.
You may use this library with the [kinesis-logs-reader](https://github.com/obsrvbl/kinesis-logs-reader) library when retrieving VPC flow logs from Amazon Kinesis.


## Installation

You can get `flowlogs_reader` by using `pip`:

```
pip install flowlogs_reader
```

Or if you want to install from source and/or contribute you can clone from GitHub:

```
git clone https://github.com/obsrvbl/flowlogs-reader.git
cd flowlogs-reader
python setup.py develop
```

## CLI Usage

`flowlogs-reader` provides a command line interface called `flowlogs_reader` that allows you to print VPC Flow Log records to your screen.
It assumes your AWS credentials are available through environment variables, a boto configuration file, or through IAM metadata.
Some example uses are below.

__Printing flows__

The default action is to `print` flows. You may also specify the `ipset`, `findip`, and `aggregate` actions:

* `flowlogs_reader flowlog_group` - print all flows in the past hour
* `flowlogs_reader flowlog_group print 10` - print the first 10 flows from the past hour
* `flowlogs_reader flowlog_group ipset` - print the unique IPs seen in the past hour
* `flowlogs_reader flowlog_group findip 198.51.100.2` - print all flows involving 198.51.100.2
* `flowlogs_reader flowlog_group findip aggregate` - aggregate the flows by 5-tuple, then print them as a tab-separated stream (with a header)

You may combine the output of `flowlogs_reader` with other command line utilities:

* `flowlogs_reader flowlog_group | grep REJECT` - print all `REJECT`ed Flow Log records
* `flowlogs_reader flowlog_group | awk '$6 = 443'` - print all traffic from port 443

__Time windows__

The default time window is the last hour. You may also specify a `--start-time` and/or an `--end-time`. The `-s` and `-e` switches may be used also:

* `flowlogs_reader --start-time='2015-08-13 00:00:00' flowlog_group`
* `flowlogs_reader --end-time='2015-08-14 00:00:00' flowlog_group`
* `flowlogs_reader --start-time='2015-08-13 01:00:00' --end-time='2015-08-14 02:00:00' flowlog_group`

Use the `--time-format` switch to control how start and end times are interpreted. The default is `'%Y-%m-%d %H:%M:%S'`. See the Python documentation for `strptime` for information on format strings.

__AWS options__

Other command line switches:

* `flowlogs_reader --region='us-west-2' flowlog_group` - connect to the given AWS region
* `flowlogs_reader --profile='dev_profile' flowlog_group` - use the profile from your [local AWS configuration file](http://docs.aws.amazon.com/cli/latest/topic/config-vars.html) to specify credentials and regions
* `flowlogs_reader --filter-pattern='REJECT' flowlog_group` - use the given [filter pattern](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/FilterAndPatternSyntax.html) to have the server limit the output
* `flowlogs_reader --role-arn='arn:aws:iam::12345678901:role/myrole' --external-id='0a1b2c3d' flowlog_group` - use the given role and external ID to connect to a 3rd party's account using [`sts assume-role`](http://docs.aws.amazon.com/cli/latest/reference/sts/assume-role.html)

## Module Usage

`FlowRecord` takes an `event` dictionary retrieved from a log stream. It parses the `message` in the event, which takes a record like this:

```
2 123456789010 eni-102010ab 198.51.100.1 192.0.2.1 443 49152 6 10 840 1439387263 1439387264 ACCEPT OK
```

And turns it into a Python object like this:

```python
>>> flow_record.srcaddr
'198.51.100.1'
>>> flow_record.dstaddr
'192.0.2.1'
>>> flow_record.srcport
443
>>> flow_record.to_dict()
{'account_id': '123456789010',
 'action': 'ACCEPT',
 'bytes': 840,
 'dstaddr': '192.0.2.1',
 'dstport': 49152,
 'end': datetime.datetime(2015, 8, 12, 13, 47, 44),
 'interface_id': 'eni-102010ab',
 'log_status': 'OK',
 'packets': 10,
 'protocol': 6,
 'srcaddr': '198.51.100.1',
 'srcport': 443,
 'start': datetime.datetime(2015, 8, 12, 13, 47, 43),
 'version': 2}
```

You may use the `FlowRecord.from_message(...)` constructor if you have a line of log text instead of an event dictionary.

`FlowLogsReader` takes the name of a log group and can then yield all the Flow Log records from that group.

```python
>>> from flowlogs_reader import FlowLogsReader
... flow_log_reader = FlowLogsReader('flowlog_group')
... records = list(flow_log_reader)
... print(len(records))
176
```

By default it will retrieve records from log streams that were ingested in the last hour, and yield records from those log streams in that same time window.

You can control what's retrieved with these parameters:
* `start_time` and `end_time` are Python `datetime.datetime` objects
* `filter_pattern` is a string like `REJECT` or `443` used to filter the logs. See the examples below.
* `region_name` is a string like `'us-east-1'`. This will be used to create a [boto3 Session object](http://boto3.readthedocs.io/en/latest/reference/core/session.html#boto3.session.Session).
* `profile_name` is a string like `'my-profile'`
* `boto_client_kwargs` is a dictionary of parameters to pass when creating the [boto3 client](http://boto3.readthedocs.io/en/latest/reference/core/session.html#boto3.session.Session.client).
* `boto_client` is a boto3 client object. This takes overrides `region_name`, `profile_name`, and `boto_client_kwargs`.

## Examples

Start by importing `FlowLogsReader`:

```python
from flowlogs_reader import FlowLogsReader
```

Find all of the IP addresses communicating inside the VPC:

```python
ip_set = set()
for record in FlowLogsReader('flowlog_group'):
    ip_set.add(record.srcaddr)
    ip_set.add(record.dstaddr)
```

See all of the traffic for one IP address:

```python
target_ip = '192.0.2.1'
records = []
for record in FlowLogsReader('flowlog_group'):
    if (record.srcaddr == target_ip) or (record.dstaddr == target_ip):
        records.append(record)
```

Loop through a few preconfigured profiles and collect all of the IP addresses:

```python
ip_set = set()
profile_names = ['profile1', 'profile2']
for profile_name in profile_names:
    for record in FlowLogsReader('flowlog_group', profile_name=profile_name):
        ip_set.add(record.srcaddr)
        ip_set.add(record.dstaddr)
```

Apply a filter for UDP traffic that was logged normally.

```python
FILTER_PATTERN = (
    '[version="2", account_id, interface_id, srcaddr, dstaddr, '
    'srcport, dstport, protocol="17", packets, bytes, '
    'start, end, action, log_status="OK"]'
)

flow_log_reader = FlowLogsReader('flowlog_group', filter_pattern=FILTER_PATTERN)
records = list(flow_log_reader)
print(len(records))
```

You may aggregate records with the `aggregate_records` function.
Pass in a `FlowLogsReader` object and optionally a `key_fields` tuple.
Python `dict` objects will be yielded representing the aggregated flow records.
By default the typical `('srcaddr', 'dstaddr', 'srcport', 'dstport', 'protocol')` will be used.
The `start`, `end`, `packets`, and `bytes` items will be aggregated.

```python
flow_log_reader = FlowLogsReader('flowlog_group')
key_fields = ('srcaddr', 'dstaddr')
records = list(aggregated_records(flow_log_reader, key_fields=key_fields))
```
