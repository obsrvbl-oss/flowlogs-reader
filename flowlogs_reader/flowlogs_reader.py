#  Copyright 2015 Observable Networks
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from calendar import timegm
from concurrent.futures import ThreadPoolExecutor
from csv import DictReader as csv_dict_reader
from datetime import datetime, timedelta
from gzip import open as gz_open
from os.path import basename
from parquet import DictReader as parquet_dict_reader
from threading import Lock

import boto3
import io

from botocore.exceptions import PaginationError
from dateutil.rrule import rrule, DAILY

DEFAULT_FIELDS = (
    'version',
    'account_id',
    'interface_id',
    'srcaddr',
    'dstaddr',
    'srcport',
    'dstport',
    'protocol',
    'packets',
    'bytes',
    'start',
    'end',
    'action',
    'log_status',
)
DUPLICATE_NEXT_TOKEN_MESSAGE = 'The same next token was received twice'

# The lastEventTimestamp may be delayed by up to an hour:
# https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_LogStream.html  # noqa
LAST_EVENT_DELAY_MSEC = 3600000

ACCEPT = 'ACCEPT'
REJECT = 'REJECT'
SKIPDATA = 'SKIPDATA'
NODATA = 'NODATA'

THREAD_LOCK = Lock()


class FlowRecord:
    """
    Given a VPC Flow Logs event dictionary, returns a Python object whose
    attributes match the field names in the event record. Integers are stored
    as Python int objects; timestamps are stored as Python datetime objects.
    """

    __slots__ = [
        'version',
        'account_id',
        'interface_id',
        'srcaddr',
        'dstaddr',
        'srcport',
        'dstport',
        'protocol',
        'packets',
        'bytes',
        'start',
        'end',
        'action',
        'log_status',
        'vpc_id',
        'subnet_id',
        'instance_id',
        'tcp_flags',
        'type',
        'pkt_srcaddr',
        'pkt_dstaddr',
        'region',
        'az_id',
        'sublocation_type',
        'sublocation_id',
        'pkt_src_aws_service',
        'pkt_dst_aws_service',
        'flow_direction',
        'traffic_path',
        'resource_type',
        'tgw_id',
        'tgw_attachment_id',
        'tgw_src_vpc_account_id',
        'tgw_dst_vpc_account_id',
        'tgw_src_vpc_id',
        'tgw_dst_vpc_id',
        'tgw_src_subnet_id',
        'tgw_dst_subnet_id',
        'tgw_src_eni',
        'tgw_dst_eni',
        'tgw_src_az_id',
        'tgw_dst_az_id',
        'tgw_pair_attachment_id',
        'packets_lost_no_route',
        'packets_lost_blackhole',
        'packets_lost_mtu_exceeded',
        'packets_lost_ttl_expired',
    ]

    def __init__(self, event_data, EPOCH_32_MAX=2147483647):
        # Contra the docs, the start and end fields can contain
        # millisecond-based timestamps.
        # http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/flow-logs.html
        if 'start' in event_data:
            start = int(event_data['start'])
            if start > EPOCH_32_MAX:
                start /= 1000
            self.start = datetime.utcfromtimestamp(start)
        else:
            self.start = None

        if 'end' in event_data:
            end = int(event_data['end'])
            if end > EPOCH_32_MAX:
                end /= 1000
            self.end = datetime.utcfromtimestamp(end)
        else:
            self.end = None

        for key, func in (
            ('version', int),
            ('account_id', str),
            ('interface_id', str),
            ('srcaddr', str),
            ('dstaddr', str),
            ('srcport', int),
            ('dstport', int),
            ('protocol', int),
            ('packets', int),
            ('bytes', int),
            ('action', str),
            ('log_status', str),
            ('vpc_id', str),
            ('subnet_id', str),
            ('instance_id', str),
            ('tcp_flags', int),
            ('type', str),
            ('pkt_srcaddr', str),
            ('pkt_dstaddr', str),
            ('region', str),
            ('az_id', str),
            ('sublocation_type', str),
            ('sublocation_id', str),
            ('pkt_src_aws_service', str),
            ('pkt_dst_aws_service', str),
            ('flow_direction', str),
            ('traffic_path', int),
            ('resource_type', str),
            ('tgw_id', str),
            ('tgw_attachment_id', str),
            ('tgw_src_vpc_account_id', str),
            ('tgw_dst_vpc_account_id', str),
            ('tgw_src_vpc_id', str),
            ('tgw_dst_vpc_id', str),
            ('tgw_src_subnet_id', str),
            ('tgw_dst_subnet_id', str),
            ('tgw_src_eni', str),
            ('tgw_dst_eni', str),
            ('tgw_src_az_id', str),
            ('tgw_dst_az_id', str),
            ('tgw_pair_attachment_id', str),
            ('packets_lost_no_route', int),
            ('packets_lost_blackhole', int),
            ('packets_lost_mtu_exceeded', int),
            ('packets_lost_ttl_expired', int),
        ):
            value = event_data.get(key, '-')
            if value == '-' or value == 'None' or value is None:
                value = None
            else:
                value = func(value)

            setattr(self, key, value)

    def __eq__(self, other):
        try:
            return all(
                getattr(self, x) == getattr(other, x) for x in self.__slots__
            )
        except AttributeError:
            return False

    def __hash__(self):
        return hash(tuple(getattr(self, x) for x in self.__slots__))

    def __str__(self):
        ret = []
        for key in self.__slots__:
            value = getattr(self, key)
            if value is not None:
                ret.append('{}: {}'.format(key, value))
        return ', '.join(ret)

    def to_dict(self):
        ret = {}
        for key in self.__slots__:
            value = getattr(self, key)
            if value is not None:
                ret[key] = value

        return ret

    def to_message(self):
        D_transform = {
            'start': lambda dt: str(timegm(dt.utctimetuple())),
            'end': lambda dt: str(timegm(dt.utctimetuple())),
        }

        ret = []
        for attr in self.__slots__:
            transform = D_transform.get(attr, lambda x: str(x) if x else '-')
            ret.append(transform(getattr(self, attr)))

        return ' '.join(ret)

    @classmethod
    def from_cwl_event(cls, cwl_event, fields=DEFAULT_FIELDS):
        data = cwl_event['message'].split()

        event_data = {}
        for key, value in zip(fields, data):
            event_data[key] = value

        return cls(event_data)


class BaseReader:
    def __init__(
        self,
        client_type,
        region_name=None,
        start_time=None,
        end_time=None,
        boto_client=None,
        raise_on_error=False
    ):
        self.region_name = region_name
        if boto_client is not None:
            self.boto_client = boto_client
        else:
            kwargs = {'region_name': region_name} if region_name else {}
            self.boto_client = boto3.client(client_type, **kwargs)

        # If no time filters are given use the last hour
        now = datetime.utcnow()
        self.start_time = start_time or now - timedelta(hours=1)
        self.end_time = end_time or now

        self.bytes_processed = 0
        self.iterator = self._reader()

        self.skipped_records = 0
        self.raise_on_error = raise_on_error

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.iterator)


class FlowLogsReader(BaseReader):
    """
    Returns an object that will yield VPC Flow Log records as Python objects.
    * `log_group_name` is the name of the CloudWatch Logs group that stores
    your VPC flow logs.
    * `region_name` is the AWS region.
    * `profile_name` is the AWS boto3 configuration profile to use.
    * `start_time` is a Python datetime.datetime object; only the log events
    from at or after this time will be considered.
    * `end_time` is a Python datetime.datetime object; only the log events
    before this time will be considered.
    * `filter_pattern` is a string passed to CloudWatch as a filter pattern
    * `boto_client_kwargs` - keyword arguments to pass to the boto3 client
    * `boto_client` - your own boto3 client object. If given then region_name,
    profile_name, and boto_client_kwargs will be ignored.
    """

    def __init__(
        self,
        log_group_name,
        filter_pattern=None,
        thread_count=0,
        fields=DEFAULT_FIELDS,
        ec2_client=None,
        **kwargs,
    ):
        super().__init__('logs', **kwargs)
        self.log_group_name = log_group_name

        self.paginator_kwargs = {}
        if filter_pattern is not None:
            self.paginator_kwargs['filterPattern'] = filter_pattern

        self.thread_count = thread_count

        if fields is None:
            fields = self._get_fields(
                self.region_name, self.log_group_name, ec2_client=ec2_client
            )
        self.fields = tuple(f.replace('-', '_') for f in fields)

        self.start_ms = timegm(self.start_time.utctimetuple()) * 1000
        self.end_ms = timegm(self.end_time.utctimetuple()) * 1000
        self.skipped_records = 0

    def _get_fields(self, region_name, log_group_name, ec2_client=None):
        if ec2_client is None:
            kwargs = {'region_name': region_name} if region_name else {}
            ec2_client = boto3.client('ec2', **kwargs)

        resp = ec2_client.describe_flow_logs(
            Filters=[{'Name': 'log-group-name', 'Values': [log_group_name]}]
        )
        log_format = resp['FlowLogs'][0]['LogFormat']
        return tuple(x.strip('${}') for x in log_format.split())

    def _get_log_streams(self):
        paginator = self.boto_client.get_paginator('describe_log_streams')
        all_pages = paginator.paginate(
            logGroupName=self.log_group_name,
            orderBy='LastEventTime',
            descending=True,
        )
        for page in all_pages:
            for log_stream in page.get('logStreams', []):
                # Skip streams that start after the time we care about.
                if log_stream['firstEventTimestamp'] >= self.end_ms:
                    continue

                # Since we're ordering by last event timestamp, we're finished
                # when we encounter a stream that ends before the time we
                # care about.
                last_event_ms = (
                    log_stream['lastEventTimestamp'] + LAST_EVENT_DELAY_MSEC
                )
                if last_event_ms < self.start_ms:
                    break

                yield log_stream['logStreamName']

    def _read_streams(self, stream_name=None):
        kwargs = self.paginator_kwargs.copy()
        if stream_name is not None:
            kwargs['logStreamNames'] = [stream_name]

        paginator = self.boto_client.get_paginator('filter_log_events')
        response_iterator = paginator.paginate(
            logGroupName=self.log_group_name,
            startTime=self.start_ms,
            endTime=self.end_ms,
            interleaved=True,
            **kwargs,
        )

        try:
            for page in response_iterator:
                page_bytes = 0
                for event in page.get('events', []):
                    page_bytes += len(event['message'])
                    yield event

                with THREAD_LOCK:
                    self.bytes_processed += page_bytes
        except PaginationError as e:
            if e.kwargs['message'].startswith(DUPLICATE_NEXT_TOKEN_MESSAGE):
                pass
            else:
                raise

    def _reader(self):
        if self.thread_count:
            all_streams = self._get_log_streams()
            with ThreadPoolExecutor(max_workers=self.thread_count) as executor:
                func = lambda x: list(self._read_streams(x))
                for events in executor.map(func, all_streams):
                    for event in events:
                        try: 
                            yield FlowRecord.from_cwl_event(event, self.fields)
                        except Exception:
                            self.skipped_records += 1
                            if self.raise_on_error:
                                raise  
        else:
            for event in self._read_streams():
                try: 
                    yield FlowRecord.from_cwl_event(event, self.fields)
                except Exception:
                    self.skipped_records += 1
                    if self.raise_on_error:
                        raise



class S3FlowLogsReader(BaseReader):
    def __init__(
        self,
        location,
        include_accounts=None,
        include_regions=None,
        thread_count=0,
        **kwargs,
    ):
        super().__init__('s3', **kwargs)

        location_parts = (location.rstrip('/') + '/').split('/', 1)
        self.bucket, self.prefix = location_parts
        self.thread_count = thread_count

        self.compressed_bytes_processed = 0

        self.include_accounts = (
            None if include_accounts is None else set(include_accounts)
        )
        self.include_regions = (
            None if include_regions is None else set(include_regions)
        )

    def _read_file(self, key):
        resp = self.boto_client.get_object(Bucket=self.bucket, Key=key)
        if key.endswith('.parquet'):
            body = resp['Body'].read()
            reader = parquet_dict_reader(io.BytesIO(body))
            yield from reader
            with THREAD_LOCK:
                self.bytes_processed += len(body)
                self.compressed_bytes_processed += resp['ContentLength']
        else:
            with gz_open(resp['Body'], mode='rt') as gz_f:
                reader = csv_dict_reader(gz_f, delimiter=' ')
                reader.fieldnames = [
                    f.replace('-', '_') for f in reader.fieldnames
                ]
                yield from reader
                with THREAD_LOCK:
                    self.bytes_processed += gz_f.tell()
                    self.compressed_bytes_processed += resp['ContentLength']

    def _get_keys(self, prefix):
        # S3 keys have a file name like:
        # account_vpcflowlogs_region_flow-logs-id_datetime_hash.log.gz
        # Yield the keys for files relevant to our time range
        paginator = self.boto_client.get_paginator('list_objects_v2')
        all_pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        for page in all_pages:
            for item in page.get('Contents', []):
                key = item['Key']
                file_name = basename(key)
                try:
                    dt = datetime.strptime(
                        file_name.rsplit('_', 2)[1], '%Y%m%dT%H%MZ'
                    )
                except (IndexError, ValueError):
                    continue

                if self.start_time <= dt < self.end_time:
                    yield key

    def _get_date_prefixes(self):
        # Each base_location/AWSLogs/account_number/vpcflowlogs/region_name/
        # prefix has files organized in year/month/day directories.
        # Yield the year/month/day/ fragments that are relevant to our
        # time range
        dtstart = self.start_time.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        until = self.end_time.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        for dt in rrule(freq=DAILY, dtstart=dtstart, until=until):
            yield dt.strftime('%Y/%m/%d/')

    def _get_region_prefixes(self, account_prefix):
        # Yield each prefix of the type:
        # base_location/AWSLogs/account_number/vpcflowlogs/region_name/
        resp = self.boto_client.list_objects_v2(
            Bucket=self.bucket,
            Delimiter='/',
            Prefix=account_prefix + 'vpcflowlogs/',
        )
        for item in resp.get('CommonPrefixes', []):
            prefix = item['Prefix']
            if self.include_regions is not None:
                region_name = prefix.rsplit('/', 2)[1]
                if region_name not in self.include_regions:
                    continue

            yield prefix

    def _get_account_prefixes(self):
        # Yield each prefix of the type:
        # base_location/AWSLogs/account_number/
        prefix = self.prefix.strip('/') + '/AWSLogs/'
        prefix = prefix.lstrip('/')
        resp = self.boto_client.list_objects_v2(
            Bucket=self.bucket, Delimiter='/', Prefix=prefix
        )
        for item in resp.get('CommonPrefixes', []):
            prefix = item['Prefix']
            if self.include_accounts is not None:
                account_id = prefix.rsplit('/', 2)[1]
                if account_id not in self.include_accounts:
                    continue

            yield prefix

    def _get_all_keys(self):
        for account_prefix in self._get_account_prefixes():
            for region_prefix in self._get_region_prefixes(account_prefix):
                for day_prefix in self._get_date_prefixes():
                    prefix = region_prefix + day_prefix
                    for key in self._get_keys(prefix):
                        yield key

    def _read_streams(self):
        all_keys = self._get_all_keys()
        if self.thread_count:
            with ThreadPoolExecutor(max_workers=self.thread_count) as executor:
                func = lambda x: list(self._read_file(x))
                for results in executor.map(func, all_keys):
                    yield from results
        else:
            for key in all_keys:
                yield from self._read_file(key)

    def _reader(self):
        for event_data in self._read_streams():
            try: 
                yield FlowRecord(event_data)
            except Exception:
                self.skipped_records += 1
                if self.raise_on_error:
                    raise

