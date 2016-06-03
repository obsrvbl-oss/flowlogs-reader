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

from __future__ import division, print_function

from datetime import datetime
from unittest import TestCase


from botocore.exceptions import NoRegionError

try:
    from unittest.mock import MagicMock, patch
except ImportError:
    from mock import MagicMock, patch

from flowlogs_reader import FlowRecord, FlowLogsReader
from flowlogs_reader.flowlogs_reader import DEFAULT_REGION_NAME


SAMPLE_RECORDS = [
    (
        '2 123456789010 eni-102010ab 198.51.100.1 192.0.2.1 '
        '443 49152 6 10 840 1439387263 1439387264 ACCEPT OK'
    ),
    (
        '2 123456789010 eni-102010ab 192.0.2.1 198.51.100.1 '
        '49152 443 6 20 1680 1439387264 1439387265 ACCEPT OK'
    ),
    (
        '2 123456789010 eni-102010ab 192.0.2.1 198.51.100.1 '
        '49152 443 6 20 1680 1439387265 1439387266 REJECT OK'
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


class FlowRecordTestCase(TestCase):
    def test_parse(self):
        flow_record = FlowRecord({'message': SAMPLE_RECORDS[0]})
        actual = {x: getattr(flow_record, x) for x in FlowRecord.__slots__}
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
        flow_record = FlowRecord({'message': SAMPLE_RECORDS[1]})
        actual = {x: getattr(flow_record, x) for x in FlowRecord.__slots__}
        expected = {
            'account_id': '123456789010',
            'action': 'ACCEPT',
            'bytes': 1680,
            'dstaddr': '198.51.100.1',
            'dstport': 443,
            'end': datetime(2015, 8, 12, 13, 47, 45),
            'interface_id': 'eni-102010ab',
            'log_status': 'OK',
            'packets': 20,
            'protocol': 6,
            'srcaddr': '192.0.2.1',
            'srcport': 49152,
            'start': datetime(2015, 8, 12, 13, 47, 44),
            'version': 2,
        }
        self.assertEqual(actual, expected)

    def test_hash(self):
        record_set = {
            FlowRecord.from_message(SAMPLE_RECORDS[0]),
            FlowRecord.from_message(SAMPLE_RECORDS[0]),
            FlowRecord.from_message(SAMPLE_RECORDS[1]),
            FlowRecord.from_message(SAMPLE_RECORDS[1]),
            FlowRecord.from_message(SAMPLE_RECORDS[2]),
            FlowRecord.from_message(SAMPLE_RECORDS[2]),
        }
        self.assertEqual(len(record_set), 3)

    def test_str(self):
        flow_record = FlowRecord({'message': SAMPLE_RECORDS[0]})
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
        flow_record = FlowRecord({'message': SAMPLE_RECORDS[2]})
        actual = flow_record.to_dict()
        expected = {
            'account_id': '123456789010',
            'action': 'REJECT',
            'bytes': 1680,
            'dstaddr': '198.51.100.1',
            'dstport': 443,
            'end': datetime(2015, 8, 12, 13, 47, 46),
            'interface_id': 'eni-102010ab',
            'log_status': 'OK',
            'packets': 20,
            'protocol': 6,
            'srcaddr': '192.0.2.1',
            'srcport': 49152,
            'start': datetime(2015, 8, 12, 13, 47, 45),
            'version': 2,
        }
        self.assertEqual(actual, expected)

    def test_to_message(self):
        for message in SAMPLE_RECORDS:
            message_record = FlowRecord.from_message(message)
            self.assertEqual(message_record.to_message(), message)

    def test_from_message(self):
        event_record = FlowRecord({'message': SAMPLE_RECORDS[1]})
        message_record = FlowRecord.from_message(SAMPLE_RECORDS[1])
        self.assertEqual(event_record, message_record)


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
            self.start_time
        )

        self.assertEqual(
            datetime.utcfromtimestamp(self.inst.end_ms // 1000),
            self.end_time
        )

        self.assertEqual(
            self.inst.paginator_kwargs['filterPattern'],
            'REJECT'
        )

    @patch('flowlogs_reader.flowlogs_reader.boto3.session', autospec=True)
    def test_region_name(self, mock_session):
        # Region specified for session
        FlowLogsReader('some_group', region_name='some-region')
        mock_session.Session.assert_called_with(region_name='some-region')

        # Region specified for client, not for session
        FlowLogsReader(
            'some_group', boto_client_kwargs={'region_name': 'my-region'}
        )
        mock_session.Session().client.assert_called_with(
            'logs', region_name='my-region'
        )

        # No region specified for session or client - use the default
        def mock_response(*args, **kwargs):
            if 'region_name' not in kwargs:
                raise NoRegionError
        mock_session.Session().client.side_effect = mock_response

        FlowLogsReader('some_group')
        mock_session.Session().client.assert_called_with(
            'logs', region_name=DEFAULT_REGION_NAME
        )

    @patch('flowlogs_reader.flowlogs_reader.boto3.session', autospec=True)
    def test_profile_name(self, mock_session):
        # profile_name specified
        FlowLogsReader('some_group', profile_name='my-profile')
        mock_session.Session.assert_called_with(profile_name='my-profile')

        # No profile specified
        FlowLogsReader('some_group')
        mock_session.Session.assert_called_with()

    def test_read_streams(self):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {'events': [0]}, {'events': [1, 2]}, {'events': [3, 4, 5]},
        ]

        self.mock_client.get_paginator.return_value = paginator

        actual = list(self.inst._read_streams())
        expected = [0, 1, 2, 3, 4, 5]
        self.assertEqual(actual, expected)

    def test_iteration(self):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                'events': [
                    {'logStreamName': 'log_0', 'message': SAMPLE_RECORDS[0]},
                    {'logStreamName': 'log_0', 'message': SAMPLE_RECORDS[1]},
                ],
            },
            {
                'events': [
                    {'logStreamName': 'log_0', 'message': SAMPLE_RECORDS[2]},
                    {'logStreamName': 'log_1', 'message': SAMPLE_RECORDS[3]},
                    {'logStreamName': 'log_2', 'message': SAMPLE_RECORDS[4]},
                ],
            },
        ]

        self.mock_client.get_paginator.return_value = paginator

        # Calling list on the instance causes it to iterate through all records
        actual = [next(self.inst)] + list(self.inst)
        expected = [FlowRecord.from_message(x) for x in SAMPLE_RECORDS]
        self.assertEqual(actual, expected)
