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

from datetime import datetime
from gzip import compress
from io import BytesIO
from unittest import TestCase
from unittest.mock import MagicMock, patch

import boto3
from botocore.exceptions import PaginationError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from flowlogs_reader import (
    aggregated_records,
    FlowRecord,
    FlowLogsReader,
    S3FlowLogsReader,
)
from flowlogs_reader.flowlogs_reader import (
    DUPLICATE_NEXT_TOKEN_MESSAGE,
    LAST_EVENT_DELAY_MSEC,
)


V2_RECORDS = [
    (
        '2 123456789010 eni-102010ab 198.51.100.1 192.0.2.1 '
        '443 49152 6 10 840 1439387263 1439387264 ACCEPT OK'
    ),
    (
        '2 123456789010 eni-102010ab 192.0.2.1 198.51.100.1 '
        '49152 443 6 20 1680 1439387264 1439387265 ACCEPT OK'
    ),
    (
        '2 123456789010 eni-102010cd 192.0.2.1 198.51.100.1 '
        '49152 443 6 20 1680 1439387263 1439387266 REJECT OK'
    ),
    (
        '2 123456789010 eni-1a2b3c4d - - - - - - - '
        '1431280876 1431280934 - NODATA'
    ),
    (
        '2 123456789010 eni-4b118871 - - - - - - - '
        '1431280876 1431280934 - SKIPDATA'
    ),
]

V3_FILE = (
    'account-id bytes dstaddr dstport end instance-id packets '
    'pkt-dstaddr pkt-srcaddr protocol srcaddr srcport start subnet-id '
    'tcp-flags type version vpc-id\n'
    '000000000000 6392 172.18.160.93 47460 1568300425 i-06f9249b 10 '
    '172.18.160.93 192.168.0.1 6 172.18.160.68 443 1568300367 subnet-089e7569 '
    '19 IPv4 3 vpc-0461a061\n'
    '000000000000 1698 172.18.160.68 443 1568300425 i-06f9249b 10 '
    '192.168.0.1 172.18.160.9 6 172.18.160.93 8088 1568300367 subnet-089e7569 '
    '3 IPv4 3 vpc-0461a061\n'
)

V4_FILE = (
    'account-id bytes dstaddr dstport end instance-id packets '
    'pkt-dstaddr pkt-srcaddr protocol srcaddr srcport start subnet-id '
    'tcp-flags type version vpc-id region az-id sublocation-type '
    'sublocation-id\n'
    '000000000000 6392 172.18.160.93 47460 1568300425 i-06f9249b 10 '
    '172.18.160.93 192.168.0.1 6 172.18.160.68 443 1568300367 subnet-089e7569 '
    '19 IPv4 4 vpc-0461a061 us-east-1 use1-az4 wavelength wlid04\n'
    '000000000000 1698 172.18.160.68 443 1568300425 i-06f9249b 10 '
    '192.168.0.1 172.18.160.9 6 172.18.160.93 8088 1568300367 subnet-089e7569 '
    '3 IPv4 4 vpc-0461a061 us-east-1 use1-az4 outpost outpostid04\n'
)

V5_FILE = (
    'account-id action az-id bytes dstaddr dstport end flow-direction '
    'instance-id interface-id log-status packets pkt-dst-aws-service '
    'pkt-dstaddr pkt-src-aws-service pkt-srcaddr protocol region srcaddr '
    'srcport start sublocation-id sublocation-type subnet-id tcp-flags '
    'traffic-path type version vpc-id\n'
    '999999999999 ACCEPT use2-az2 4895 192.0.2.156 50318 1614866511 '
    'ingress i-00123456789abcdef eni-00123456789abcdef OK 15 - 192.0.2.156 '
    'S3 198.51.100.6 6 us-east-2 198.51.100.7 443 1614866493 - - '
    'subnet-0123456789abcdef 19 - IPv4 5 vpc-04456ab739938ee3f\n'
    '999999999999 ACCEPT use2-az2 3015 198.51.100.6 443 1614866511 '
    'egress i-00123456789abcdef eni-00123456789abcdef OK 16 S3 198.51.100.7 '
    '- 192.0.2.156 6 us-east-2 192.0.2.156 50318 1614866493 - - '
    'subnet-0123456789abcdef 7 7 IPv4 5 vpc-04456ab739938ee3f\n'
    'bogus line\n'
)
V6_FILE = (
    'resource-type tgw-id tgw-attachment-id tgw-src-vpc-account-id '
    'tgw-dst-vpc-account-id tgw-src-vpc-id tgw-dst-vpc-id tgw-src-subnet-id '
    'tgw-dst-subnet-id tgw-src-eni tgw-dst-eni tgw-src-az-id tgw-dst-az-id '
    'tgw-pair-attachment-id packets-lost-no-route packets-lost-blackhole '
    'packets-lost-mtu-exceeded packets-lost-ttl-expired\n'
    'TransitGateway 00000000 00000001 00000002 00000003 00000004 00000005 '
    '00000006 00000007 00000008 00000009 00000010 00000011 00000012 13 14 15 '
    '16\n'
)

PARQUET_FILE = 'tests/data/flows.parquet'


class FlowRecordTestCase(TestCase):
    def test_parse(self):
        flow_record = FlowRecord.from_cwl_event({'message': V2_RECORDS[0]})
        actual = flow_record.to_dict()
        expected = {
            'account_id': '123456789010',
            'action': 'ACCEPT',
            'bytes': 840,
            'dstaddr': '192.0.2.1',
            'dstport': 49152,
            'end': datetime(2015, 8, 12, 13, 47, 44),
            'interface_id': 'eni-102010ab',
            'log_status': 'OK',
            'packets': 10,
            'protocol': 6,
            'srcaddr': '198.51.100.1',
            'srcport': 443,
            'start': datetime(2015, 8, 12, 13, 47, 43),
            'version': 2,
        }
        self.assertEqual(actual, expected)

    def test_eq(self):
        flow_record = FlowRecord.from_cwl_event({'message': V2_RECORDS[0]})
        equal_record = FlowRecord.from_cwl_event({'message': V2_RECORDS[0]})
        unequal_record = FlowRecord.from_cwl_event({'message': V2_RECORDS[1]})

        self.assertEqual(flow_record, equal_record)
        self.assertNotEqual(flow_record, unequal_record)
        self.assertNotEqual(flow_record, Ellipsis)

    def test_hash(self):
        record_set = {
            FlowRecord.from_cwl_event({'message': V2_RECORDS[0]}),
            FlowRecord.from_cwl_event({'message': V2_RECORDS[0]}),
            FlowRecord.from_cwl_event({'message': V2_RECORDS[1]}),
            FlowRecord.from_cwl_event({'message': V2_RECORDS[1]}),
            FlowRecord.from_cwl_event({'message': V2_RECORDS[2]}),
            FlowRecord.from_cwl_event({'message': V2_RECORDS[2]}),
        }
        self.assertEqual(len(record_set), 3)

    def test_str(self):
        flow_record = FlowRecord.from_cwl_event({'message': V2_RECORDS[0]})
        actual = str(flow_record)
        expected = (
            'version: 2, account_id: 123456789010, '
            'interface_id: eni-102010ab, srcaddr: 198.51.100.1, '
            'dstaddr: 192.0.2.1, srcport: 443, dstport: 49152, protocol: 6, '
            'packets: 10, bytes: 840, start: 2015-08-12 13:47:43, '
            'end: 2015-08-12 13:47:44, action: ACCEPT, log_status: OK'
        )
        self.assertEqual(actual, expected)

    def test_to_dict(self):
        flow_record = FlowRecord.from_cwl_event({'message': V2_RECORDS[2]})
        actual = flow_record.to_dict()
        expected = {
            'account_id': '123456789010',
            'action': 'REJECT',
            'bytes': 1680,
            'dstaddr': '198.51.100.1',
            'dstport': 443,
            'end': datetime(2015, 8, 12, 13, 47, 46),
            'interface_id': 'eni-102010cd',
            'log_status': 'OK',
            'packets': 20,
            'protocol': 6,
            'srcaddr': '192.0.2.1',
            'srcport': 49152,
            'start': datetime(2015, 8, 12, 13, 47, 43),
            'version': 2,
        }
        self.assertEqual(actual, expected)

    def test_millisecond_timestamp(self):
        # This record has millisecond timestamps
        record = (
            '2 123456789010 eni-4b118871 - - - - - - - '
            '1512564058000 1512564059000 - SKIPDATA'
        )
        flow_record = FlowRecord.from_cwl_event({'message': record})
        self.assertEqual(flow_record.start, datetime(2017, 12, 6, 12, 40, 58))
        self.assertEqual(flow_record.end, datetime(2017, 12, 6, 12, 40, 59))

    def test_missing_timestamps(self):
        event_data = {
            'version': '3',
            'srcaddr': '192.0.2.0',
            'dstaddr': '198.51.100.0',
            'bytes': '200',
        }
        flow_record = FlowRecord(event_data)
        self.assertEqual(
            flow_record.to_dict(),
            {
                'version': 3,
                'srcaddr': '192.0.2.0',
                'dstaddr': '198.51.100.0',
                'bytes': 200,
            },
        )
        self.assertIsNone(flow_record.start)
        self.assertIsNone(flow_record.end)


class FlowLogsReaderTestCase(TestCase):
    def setUp(self):
        self.mock_client = MagicMock()

        self.start_time = datetime(2015, 8, 12, 12, 0, 0)
        self.end_time = datetime(2015, 8, 12, 13, 0, 0)

        self.inst = FlowLogsReader(
            'group_name',
            start_time=self.start_time,
            end_time=self.end_time,
            filter_pattern='REJECT',
            boto_client=self.mock_client,
        )

    def test_init(self):
        self.assertEqual(self.inst.log_group_name, 'group_name')

        self.assertEqual(
            datetime.utcfromtimestamp(self.inst.start_ms // 1000),
            self.start_time,
        )

        self.assertEqual(
            datetime.utcfromtimestamp(self.inst.end_ms // 1000), self.end_time
        )

        self.assertEqual(self.inst.paginator_kwargs['filterPattern'], 'REJECT')

    @patch('flowlogs_reader.flowlogs_reader.boto3.client', autospec=True)
    def test_region_name(self, mock_client):
        # Region specified for session
        FlowLogsReader('some_group', region_name='some-region')
        mock_client.assert_called_with('logs', region_name='some-region')

        # None specified
        FlowLogsReader('some_group')
        mock_client.assert_called_with('logs')

    @patch('flowlogs_reader.flowlogs_reader.boto3.client', autospec=True)
    def test_get_fields(self, mock_client):
        cwl_client = MagicMock()
        ec2_client = mock_client.return_value
        ec2_client.describe_flow_logs.return_value = {
            'FlowLogs': [
                {'LogFormat': '${srcaddr} ${dstaddr} ${start} ${log-status}'}
            ]
        }
        reader = FlowLogsReader(
            'some_group',
            boto_client=cwl_client,
            fields=None,
        )
        self.assertEqual(
            reader.fields, ('srcaddr', 'dstaddr', 'start', 'log_status')
        )
        ec2_client.describe_flow_logs.assert_called_once_with(
            Filters=[{'Name': 'log-group-name', 'Values': ['some_group']}]
        )

    def test_read_streams(self):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                'events': [
                    {'logStreamName': 'log_0', 'message': V2_RECORDS[0]},
                    {'logStreamName': 'log_0', 'message': V2_RECORDS[1]},
                ],
            },
            {
                'events': [
                    {'logStreamName': 'log_0', 'message': V2_RECORDS[2]},
                    {'logStreamName': 'log_1', 'message': V2_RECORDS[3]},
                    {'logStreamName': 'log_2', 'message': V2_RECORDS[4]},
                ],
            },
        ]

        self.mock_client.get_paginator.return_value = paginator

        actual = list(self.inst._read_streams())
        expected = []
        for page in paginator.paginate.return_value:
            expected += page['events']
        self.assertEqual(actual, expected)

    def test_iteration(self):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                'events': [
                    {'logStreamName': 'log_0', 'message': V2_RECORDS[0]},
                    {'logStreamName': 'log_0', 'message': V2_RECORDS[1]},
                ],
            },
            {
                'events': [
                    {'logStreamName': 'log_0', 'message': V2_RECORDS[2]},
                    {'logStreamName': 'log_1', 'message': V2_RECORDS[3]},
                    {'logStreamName': 'log_2', 'message': V2_RECORDS[4]},
                    {'logStreamName': 'log_2', 'message': 'bogus!'},
                ],
            },
        ]

        self.mock_client.get_paginator.return_value = paginator

        # Calling list on the instance causes it to iterate through all records
        actual = [next(self.inst)] + list(self.inst)
        expected = [
            FlowRecord.from_cwl_event({'message': x}) for x in V2_RECORDS
        ]
        self.assertEqual(actual, expected)
        self.assertEqual(self.inst.skipped_records, 1)

        expected_bytes = 0
        all_pages = paginator.paginate.return_value
        expected_bytes = sum(
            len(e['message']) for p in all_pages for e in p['events']
        )
        self.assertEqual(self.inst.bytes_processed, expected_bytes)

    def test_iteration_error(self):
        # Simulate the paginator failing
        def _get_paginator(*args, **kwargs):
            event_0 = {'logStreamName': 'log_0', 'message': V2_RECORDS[0]}
            event_1 = {'logStreamName': 'log_0', 'message': V2_RECORDS[1]}
            for item in [{'events': [event_0, event_1]}]:
                yield item

            err_msg = '{}: {}'.format(DUPLICATE_NEXT_TOKEN_MESSAGE, 'token')
            raise PaginationError(message=err_msg)

        self.mock_client.get_paginator.return_value.paginate.side_effect = (
            _get_paginator
        )

        # Don't fail if botocore's paginator raises a PaginationError
        actual = [next(self.inst)] + list(self.inst)
        records = V2_RECORDS[:2]
        expected = [FlowRecord.from_cwl_event({'message': x}) for x in records]
        self.assertEqual(actual, expected)

    def test_iteration_unexpecetd_error(self):
        # Simulate the paginator failing
        def _get_paginator(*args, **kwargs):
            event_0 = {'logStreamName': 'log_0', 'message': V2_RECORDS[0]}
            yield {'events': [event_0]}
            raise PaginationError(message='other error')

        self.mock_client.get_paginator.return_value.paginate.side_effect = (
            _get_paginator
        )

        # Fail for unexpected PaginationError
        self.assertRaises(PaginationError, lambda: list(self.inst))

    def test_threads(self):
        inst = FlowLogsReader(
            'group_name',
            start_time=self.start_time,
            end_time=self.end_time,
            filter_pattern='REJECT',
            boto_client=self.mock_client,
            thread_count=1,
        )

        paginators = []

        def _get_paginator(operation):
            nonlocal paginators

            paginator = MagicMock()
            if operation == 'describe_log_streams':
                paginator.paginate.return_value = [
                    {
                        'logStreams': [
                            {
                                'logStreamName': 'too_late',
                                'firstEventTimestamp': inst.end_ms,
                                'lastEventTimestamp': inst.start_ms,
                            },
                            {
                                'logStreamName': 'too_late',
                                'firstEventTimestamp': inst.end_ms - 1,
                                'lastEventTimestamp': (
                                    inst.start_ms - LAST_EVENT_DELAY_MSEC - 1
                                ),
                            },
                        ],
                    },
                    {
                        'logStreams': [
                            {
                                'logStreamName': 'first_stream',
                                'firstEventTimestamp': inst.start_ms,
                                'lastEventTimestamp': inst.end_ms,
                            },
                            {
                                'logStreamName': 'second_stream',
                                'firstEventTimestamp': inst.start_ms,
                                'lastEventTimestamp': inst.end_ms,
                            },
                        ],
                    },
                ]
            elif operation == 'filter_log_events':
                paginator.paginate.return_value = [
                    {
                        'events': [
                            {'message': V2_RECORDS[0]},
                            {'message': V2_RECORDS[1]},
                        ],
                    },
                    {
                        'events': [
                            {'message': V2_RECORDS[2]},
                            {'message': V2_RECORDS[3]},
                        ],
                    },
                ]
            else:
                self.fail('invalid operation')

            paginators.append(paginator)
            return paginator

        self.mock_client.get_paginator.side_effect = _get_paginator
        events = list(inst)
        self.assertEqual(len(events), 8)

        paginators[0].paginate.assert_called_once_with(
            logGroupName='group_name',
            orderBy='LastEventTime',
            descending=True,
        )
        paginators[1].paginate.assert_called_once_with(
            logGroupName='group_name',
            startTime=inst.start_ms,
            endTime=inst.end_ms,
            interleaved=True,
            filterPattern='REJECT',
            logStreamNames=['first_stream'],
        )
        paginators[2].paginate.assert_called_once_with(
            logGroupName='group_name',
            startTime=inst.start_ms,
            endTime=inst.end_ms,
            interleaved=True,
            filterPattern='REJECT',
            logStreamNames=['second_stream'],
        )


class S3FlowLogsReaderTestCase(TestCase):
    def setUp(self):
        self.start_time = datetime(2015, 8, 12, 12, 0, 0)
        self.end_time = datetime(2015, 8, 12, 13, 0, 0)
        self.thread_count = 0

    def tearDown(self):
        pass

    def _test_iteration(self, data, expected):
        boto_client = boto3.client('s3')
        with Stubber(boto_client) as stubbed_client:
            # Accounts call
            accounts_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'CommonPrefixes': [
                    # This one is used
                    {'Prefix': 'AWSLogs/123456789010/'},
                    # This one is ignored
                    {'Prefix': 'AWSLogs/123456789011/'},
                ],
            }
            accounts_params = {
                'Bucket': 'example-bucket',
                'Delimiter': '/',
                'Prefix': 'AWSLogs/',
            }
            stubbed_client.add_response(
                'list_objects_v2', accounts_response, accounts_params
            )
            # Regions call
            regions_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'CommonPrefixes': [
                    # This one is used
                    {'Prefix': 'AWSLogs/123456789010/vpcflowlogs/pangaea-1/'},
                    # This one is ignored
                    {'Prefix': 'AWSLogs/123456789010/vpcflowlogs/pangaea-2/'},
                ],
            }
            regions_params = {
                'Bucket': 'example-bucket',
                'Delimiter': '/',
                'Prefix': 'AWSLogs/123456789010/vpcflowlogs/',
            }
            stubbed_client.add_response(
                'list_objects_v2', regions_response, regions_params
            )
            # List objects call
            list_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'Contents': [
                    # Too early - not downloaded
                    {
                        'Key': (
                            'AWSLogs/123456789010/vpcflowlogs/pangaea-1/'
                            '2015/08/12/'
                            '123456789010_vpcflowlogs_'
                            'pangaea-1_fl-102010_'
                            '20150812T1155Z_'
                            'h45h.log.gz'
                        ),
                    },
                    # Right on time
                    {
                        'Key': (
                            'AWSLogs/123456789010/vpcflowlogs/pangaea-1/'
                            '2015/08/12/'
                            '123456789010_vpcflowlogs_'
                            'pangaea-1_fl-102010_'
                            '20150812T1200Z_'
                            'h45h.log.gz'
                        ),
                    },
                    # Some fool put a different key here
                    {
                        'Key': (
                            'AWSLogs/123456789010/vpcflowlogs/pangaea-1/'
                            '2015/08/12/test_file.log.gz'
                        ),
                    },
                ],
            }
            list_params = {
                'Bucket': 'example-bucket',
                'Prefix': (
                    'AWSLogs/123456789010/vpcflowlogs/pangaea-1/2015/08/12/'
                ),
            }
            stubbed_client.add_response(
                'list_objects_v2', list_response, list_params
            )
            # Get object call
            data = compress(data.encode('utf-8'))

            get_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'Body': StreamingBody(BytesIO(data), len(data)),
                'ContentLength': len(data),
            }
            get_params = {
                'Bucket': 'example-bucket',
                'Key': (
                    'AWSLogs/123456789010/vpcflowlogs/pangaea-1/'
                    '2015/08/12/'
                    '123456789010_vpcflowlogs_'
                    'pangaea-1_fl-102010_'
                    '20150812T1200Z_'
                    'h45h.log.gz'
                ),
            }
            stubbed_client.add_response('get_object', get_response, get_params)
            # Do the deed
            stubbed_client.activate()
            reader = S3FlowLogsReader(
                'example-bucket',
                start_time=self.start_time,
                end_time=self.end_time,
                thread_count=self.thread_count,
                include_accounts={'123456789010'},
                include_regions={'pangaea-1'},
                boto_client=boto_client,
            )
            actual = [record.to_dict() for record in reader]
            self.assertEqual(actual, expected)

        return reader

    def test_serial(self):
        expected = [
            {
                'version': 3,
                'account_id': '000000000000',
                'srcaddr': '172.18.160.68',
                'dstaddr': '172.18.160.93',
                'srcport': 443,
                'dstport': 47460,
                'protocol': 6,
                'packets': 10,
                'bytes': 6392,
                'start': datetime(2019, 9, 12, 14, 59, 27),
                'end': datetime(2019, 9, 12, 15, 0, 25),
                'vpc_id': 'vpc-0461a061',
                'subnet_id': 'subnet-089e7569',
                'instance_id': 'i-06f9249b',
                'tcp_flags': 19,
                'type': 'IPv4',
                'pkt_srcaddr': '192.168.0.1',
                'pkt_dstaddr': '172.18.160.93',
            },
            {
                'version': 3,
                'account_id': '000000000000',
                'srcaddr': '172.18.160.93',
                'dstaddr': '172.18.160.68',
                'srcport': 8088,
                'dstport': 443,
                'protocol': 6,
                'packets': 10,
                'bytes': 1698,
                'start': datetime(2019, 9, 12, 14, 59, 27),
                'end': datetime(2019, 9, 12, 15, 0, 25),
                'vpc_id': 'vpc-0461a061',
                'subnet_id': 'subnet-089e7569',
                'instance_id': 'i-06f9249b',
                'tcp_flags': 3,
                'type': 'IPv4',
                'pkt_srcaddr': '172.18.160.9',
                'pkt_dstaddr': '192.168.0.1',
            },
        ]
        reader = self._test_iteration(V3_FILE, expected)
        self.assertEqual(reader.bytes_processed, len(V3_FILE.encode()))
        self.assertEqual(
            reader.compressed_bytes_processed, len(compress(V3_FILE.encode()))
        )

    def test_serial_v4(self):
        expected = [
            {
                'version': 4,
                'account_id': '000000000000',
                'srcaddr': '172.18.160.68',
                'dstaddr': '172.18.160.93',
                'srcport': 443,
                'dstport': 47460,
                'protocol': 6,
                'packets': 10,
                'bytes': 6392,
                'start': datetime(2019, 9, 12, 14, 59, 27),
                'end': datetime(2019, 9, 12, 15, 0, 25),
                'vpc_id': 'vpc-0461a061',
                'subnet_id': 'subnet-089e7569',
                'instance_id': 'i-06f9249b',
                'tcp_flags': 19,
                'type': 'IPv4',
                'pkt_srcaddr': '192.168.0.1',
                'pkt_dstaddr': '172.18.160.93',
                'vpc_id': 'vpc-0461a061',
                'region': 'us-east-1',
                'az_id': 'use1-az4',
                'sublocation_type': 'wavelength',
                'sublocation_id': 'wlid04',
            },
            {
                'version': 4,
                'account_id': '000000000000',
                'srcaddr': '172.18.160.93',
                'dstaddr': '172.18.160.68',
                'srcport': 8088,
                'dstport': 443,
                'protocol': 6,
                'packets': 10,
                'bytes': 1698,
                'start': datetime(2019, 9, 12, 14, 59, 27),
                'end': datetime(2019, 9, 12, 15, 0, 25),
                'vpc_id': 'vpc-0461a061',
                'subnet_id': 'subnet-089e7569',
                'instance_id': 'i-06f9249b',
                'tcp_flags': 3,
                'type': 'IPv4',
                'pkt_srcaddr': '172.18.160.9',
                'pkt_dstaddr': '192.168.0.1',
                'region': 'us-east-1',
                'az_id': 'use1-az4',
                'sublocation_type': 'outpost',
                'sublocation_id': 'outpostid04',
            },
        ]
        reader = self._test_iteration(V4_FILE, expected)
        self.assertEqual(reader.bytes_processed, len(V4_FILE.encode()))
        self.assertEqual(
            reader.compressed_bytes_processed, len(compress(V4_FILE.encode()))
        )

    def test_serial_v5(self):
        expected = [
            {
                'account_id': '999999999999',
                'action': 'ACCEPT',
                'az_id': 'use2-az2',
                'bytes': 4895,
                'dstaddr': '192.0.2.156',
                'dstport': 50318,
                'end': datetime(2021, 3, 4, 14, 1, 51),
                'flow_direction': 'ingress',
                'instance_id': 'i-00123456789abcdef',
                'interface_id': 'eni-00123456789abcdef',
                'log_status': 'OK',
                'packets': 15,
                'pkt_dstaddr': '192.0.2.156',
                'pkt_src_aws_service': 'S3',
                'pkt_srcaddr': '198.51.100.6',
                'protocol': 6,
                'region': 'us-east-2',
                'srcaddr': '198.51.100.7',
                'srcport': 443,
                'start': datetime(2021, 3, 4, 14, 1, 33),
                'subnet_id': 'subnet-0123456789abcdef',
                'tcp_flags': 19,
                'type': 'IPv4',
                'version': 5,
                'vpc_id': 'vpc-04456ab739938ee3f',
            },
            {
                'account_id': '999999999999',
                'action': 'ACCEPT',
                'az_id': 'use2-az2',
                'bytes': 3015,
                'dstaddr': '198.51.100.6',
                'dstport': 443,
                'end': datetime(2021, 3, 4, 14, 1, 51),
                'flow_direction': 'egress',
                'instance_id': 'i-00123456789abcdef',
                'interface_id': 'eni-00123456789abcdef',
                'log_status': 'OK',
                'packets': 16,
                'pkt_dst_aws_service': 'S3',
                'pkt_dstaddr': '198.51.100.7',
                'pkt_srcaddr': '192.0.2.156',
                'protocol': 6,
                'region': 'us-east-2',
                'srcaddr': '192.0.2.156',
                'srcport': 50318,
                'start': datetime(2021, 3, 4, 14, 1, 33),
                'subnet_id': 'subnet-0123456789abcdef',
                'tcp_flags': 7,
                'traffic_path': 7,
                'type': 'IPv4',
                'version': 5,
                'vpc_id': 'vpc-04456ab739938ee3f',
            },
        ]
        reader = self._test_iteration(V5_FILE, expected)
        self.assertEqual(reader.bytes_processed, len(V5_FILE.encode()))
        self.assertEqual(
            reader.compressed_bytes_processed, len(compress(V5_FILE.encode()))
        )

    def test_serial_v6(self):
        expected = [
            {
                'resource_type': 'TransitGateway',
                'tgw_id': '00000000',
                'tgw_attachment_id': '00000001',
                'tgw_src_vpc_account_id': '00000002',
                'tgw_dst_vpc_account_id': '00000003',
                'tgw_src_vpc_id': '00000004',
                'tgw_dst_vpc_id': '00000005',
                'tgw_src_subnet_id': '00000006',
                'tgw_dst_subnet_id': '00000007',
                'tgw_src_eni': '00000008',
                'tgw_dst_eni': '00000009',
                'tgw_src_az_id': '00000010',
                'tgw_dst_az_id': '00000011',
                'tgw_pair_attachment_id': '00000012',
                'packets_lost_no_route': 13,
                'packets_lost_blackhole': 14,
                'packets_lost_mtu_exceeded': 15,
                'packets_lost_ttl_expired': 16,
            },
        ]
        self.maxDiff = None
        reader = self._test_iteration(V6_FILE, expected)
        self.assertEqual(reader.bytes_processed, len(V6_FILE.encode()))
        self.assertEqual(
            reader.compressed_bytes_processed, len(compress(V6_FILE.encode()))
        )

    def _test_parquet_reader(self, data, expected):
        boto_client = boto3.client('s3')
        with Stubber(boto_client) as stubbed_client:
            # Accounts call
            accounts_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'CommonPrefixes': [
                    # This one is used
                    {'Prefix': 'AWSLogs/123456789010/'},
                    # This one is ignored
                    {'Prefix': 'AWSLogs/123456789011/'},
                ],
            }
            accounts_params = {
                'Bucket': 'example-bucket',
                'Delimiter': '/',
                'Prefix': 'AWSLogs/',
            }
            stubbed_client.add_response(
                'list_objects_v2', accounts_response, accounts_params
            )
            # Regions call
            regions_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'CommonPrefixes': [
                    # This one is used
                    {'Prefix': 'AWSLogs/123456789010/vpcflowlogs/pangaea-1/'},
                    # This one is ignored
                    {'Prefix': 'AWSLogs/123456789010/vpcflowlogs/pangaea-2/'},
                ],
            }
            regions_params = {
                'Bucket': 'example-bucket',
                'Delimiter': '/',
                'Prefix': 'AWSLogs/123456789010/vpcflowlogs/',
            }
            stubbed_client.add_response(
                'list_objects_v2', regions_response, regions_params
            )
            # List objects call
            list_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'Contents': [
                    {
                        'Key': (
                            'AWSLogs/123456789010/vpcflowlogs/pangaea-1/'
                            '2015/08/12/'
                            '123456789010_vpcflowlogs_'
                            'pangaea-1_fl-102010_'
                            '20150812T1200Z_'
                            'h45h.log.parquet'
                        ),
                    },
                ],
            }
            list_params = {
                'Bucket': 'example-bucket',
                'Prefix': (
                    'AWSLogs/123456789010/vpcflowlogs/pangaea-1/2015/08/12/'
                ),
            }
            stubbed_client.add_response(
                'list_objects_v2', list_response, list_params
            )

            get_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'Body': StreamingBody(BytesIO(data), len(data)),
                'ContentLength': len(data),
            }
            get_params = {
                'Bucket': 'example-bucket',
                'Key': (
                    'AWSLogs/123456789010/vpcflowlogs/pangaea-1/'
                    '2015/08/12/'
                    '123456789010_vpcflowlogs_'
                    'pangaea-1_fl-102010_'
                    '20150812T1200Z_'
                    'h45h.log.parquet'
                ),
            }
            stubbed_client.add_response('get_object', get_response, get_params)
            # Do the deed
            stubbed_client.activate()
            reader = S3FlowLogsReader(
                'example-bucket',
                start_time=self.start_time,
                end_time=self.end_time,
                thread_count=self.thread_count,
                include_accounts={'123456789010'},
                include_regions={'pangaea-1'},
                boto_client=boto_client,
            )
            actual = [record.to_dict() for record in reader]
            self.assertEqual(actual, expected)

        return reader

    def test_serial_parquet(self):
        expected = [
            {
                'version': 2,
                'account_id': '123456789010',
                'interface_id': 'eni-102010ab',
                'srcaddr': '198.51.100.1',
                'dstaddr': '192.0.2.1',
                'srcport': 443,
                'dstport': 49152,
                'protocol': 6,
                'packets': 10,
                'bytes': 840,
                'start': datetime(2015, 8, 12, 13, 47, 43),
                'end': datetime(2015, 8, 12, 13, 47, 44),
                'action': 'ACCEPT',
                'log_status': 'OK',
            },
            {
                'version': 2,
                'account_id': '123456789010',
                'interface_id': 'eni-202010ab',
                'start': datetime(2015, 8, 12, 13, 47, 43),
                'end': datetime(2015, 8, 12, 13, 47, 44),
                'log_status': 'NODATA',
            },
            {
                'version': 2,
                'account_id': '123456789010',
                'interface_id': 'eni-202010ab',
                'srcaddr': '198.51.100.7',
                'dstaddr': '192.0.2.156',
                'srcport': 80,
                'dstport': 100,
                'protocol': 8080,
                'start': datetime(2015, 8, 12, 13, 47, 43),
                'end': datetime(2015, 8, 12, 13, 47, 44),
                'log_status': 'NODATA',
            },
        ]
        with open(PARQUET_FILE, "rb") as parquet_data:
            data = parquet_data.read()
            reader = self._test_parquet_reader(data, expected)
            self.assertEqual(reader.compressed_bytes_processed, len(data))
            self.assertEqual(reader.bytes_processed, parquet_data.tell())

    def test_threads(self):
        expected = [
            {
                'version': 3,
                'account_id': '000000000000',
                'srcaddr': '172.18.160.68',
                'dstaddr': '172.18.160.93',
                'srcport': 443,
                'dstport': 47460,
                'protocol': 6,
                'packets': 10,
                'bytes': 6392,
                'start': datetime(2019, 9, 12, 14, 59, 27),
                'end': datetime(2019, 9, 12, 15, 0, 25),
                'vpc_id': 'vpc-0461a061',
                'subnet_id': 'subnet-089e7569',
                'instance_id': 'i-06f9249b',
                'tcp_flags': 19,
                'type': 'IPv4',
                'pkt_srcaddr': '192.168.0.1',
                'pkt_dstaddr': '172.18.160.93',
            },
            {
                'version': 3,
                'account_id': '000000000000',
                'srcaddr': '172.18.160.93',
                'dstaddr': '172.18.160.68',
                'srcport': 8088,
                'dstport': 443,
                'protocol': 6,
                'packets': 10,
                'bytes': 1698,
                'start': datetime(2019, 9, 12, 14, 59, 27),
                'end': datetime(2019, 9, 12, 15, 0, 25),
                'vpc_id': 'vpc-0461a061',
                'subnet_id': 'subnet-089e7569',
                'instance_id': 'i-06f9249b',
                'tcp_flags': 3,
                'type': 'IPv4',
                'pkt_srcaddr': '172.18.160.9',
                'pkt_dstaddr': '192.168.0.1',
            },
        ]
        self.thread_count = 2
        self._test_iteration(V3_FILE, expected)


class AggregationTestCase(TestCase):
    def test_aggregated_records(self):
        # Aggregate by 5-tuple by default
        events = [
            {'message': V2_RECORDS[0]},
            {'message': V2_RECORDS[1]},
            {'message': V2_RECORDS[2].replace('REJECT', 'ACCEPT')},
            {'message': V2_RECORDS[3]},
        ]
        all_records = (FlowRecord.from_cwl_event(x) for x in events)
        results = aggregated_records(all_records)

        actual = sorted(results, key=lambda x: x['srcaddr'])
        expected = [
            {
                'srcaddr': '192.0.2.1',
                'srcport': 49152,
                'dstaddr': '198.51.100.1',
                'dstport': 443,
                'protocol': 6,
                'start': datetime(2015, 8, 12, 13, 47, 43),
                'end': datetime(2015, 8, 12, 13, 47, 46),
                'packets': 40,
                'bytes': 3360,
            },
            {
                'srcaddr': '198.51.100.1',
                'srcport': 443,
                'dstaddr': '192.0.2.1',
                'dstport': 49152,
                'protocol': 6,
                'start': datetime(2015, 8, 12, 13, 47, 43),
                'end': datetime(2015, 8, 12, 13, 47, 44),
                'packets': 10,
                'bytes': 840,
            },
        ]
        self.assertEqual(actual, expected)

    def test_aggregated_records_custom(self):
        # Aggregate by interface_id
        events = [
            {'message': V2_RECORDS[1]},
            {'message': V2_RECORDS[2].replace('REJECT', 'ACCEPT')},
        ]
        all_records = (FlowRecord.from_cwl_event(x) for x in events)
        key_fields = ('interface_id', 'srcaddr', 'srcport', 'dstport')
        results = aggregated_records(all_records, key_fields=key_fields)

        actual = sorted(results, key=lambda x: x['interface_id'])
        expected = [
            {
                'srcaddr': '192.0.2.1',
                'srcport': 49152,
                'interface_id': 'eni-102010ab',
                'dstport': 443,
                'start': datetime(2015, 8, 12, 13, 47, 44),
                'end': datetime(2015, 8, 12, 13, 47, 45),
                'packets': 20,
                'bytes': 1680,
            },
            {
                'srcaddr': '192.0.2.1',
                'srcport': 49152,
                'interface_id': 'eni-102010cd',
                'dstport': 443,
                'start': datetime(2015, 8, 12, 13, 47, 43),
                'end': datetime(2015, 8, 12, 13, 47, 46),
                'packets': 20,
                'bytes': 1680,
            },
        ]
        self.assertEqual(actual, expected)
