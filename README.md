## Introduction

[![Build Status](https://travis-ci.org/obsrvbl/flowlogs-reader.svg?branch=master)](https://travis-ci.org/obsrvbl/flowlogs-reader)
[![Coverage Status](https://coveralls.io/repos/obsrvbl/flowlogs-reader/badge.svg?branch=master&service=github)](https://coveralls.io/github/obsrvbl/flowlogs-reader?branch=master)
[![PyPI Version](https://img.shields.io/pypi/v/flowlogs_reader.svg)](https://pypi.python.org/pypi/flowlogs_reader)

Amazon's VPC Flow Logs are analagous to NetFlow and IPFIX logs, and can be used for security and performance analysis. [Observable Networks](https://observable.net) uses VPC Flow logs as an input to endpoint modeling for security monitoring.

This project contains a Python library that makes retrieving VPC Flow Logs from Amazon CloudWatch Logs a bit easier. The library provides:

* A data structure that parses the Flow Log records into easily-used Python objects.
* A utility that makes iterating over all the Flow Log records in a log group very simple.

The library builds on [boto3](https://github.com/boto/boto3) and should work on both Python 2.7 and 3.4+.

For information on VPC Flow Logs and how to enable them see [this post](https://aws.amazon.com/blogs/aws/vpc-flow-logs-log-and-view-network-traffic-flows/) at the AWS blog.

__Note__: The library is still experimental. Give it a try and file an issue or pull request if you have suggestions.

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

`flowlogs-reader` provides a command line interface called `flowlogs_reader` that allows you to print VPC Flow Log records to your screen. It assumes your AWS credentials are available through environment variables, a boto configuration file, or through IAM metadata. Some example uses are:

* `flowlogs_reader flowlog_group` - print all flows in the past hour
* `flowlogs_reader -s '2015-08-13 00:00:00' -e '2015-08-14 00:00:00' flowlog_group` - print all the flows from August 13, 2015
* `flowlogs_reader flowlog_group ipset` - print the unique IPs seen in the past hour
* `flowlogs_reader flowlog_group findip 198.51.100.2` - print all flows involving 198.51.100.2

Or combine with other command line utilities:

* `flowlogs_reader flowlog_group | grep REJECT` - print all `REJECT`ed Flow Log records
* `flowlogs_reader flowlog_group | awk '$6 = 443'` - print all traffic from port 443

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
* `region_name` is a string like `'us-east-1'`
* `profile_name` is a string like `'my-profile'`
* `start_time` and `end_time` are Python `datetime.datetime` objects
* `boto_client_kwargs` is a dictionary of parameters to pass to `boto3.client`

## Examples

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
