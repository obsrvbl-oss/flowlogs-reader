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

from __future__ import print_function

import io
from datetime import datetime
from unittest import TestCase

try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from flowlogs_reader import FlowRecord
from flowlogs_reader.__main__ import main, actions


SAMPLE_INPUT = [
    (
        '2 123456789010 eni-102010ab 198.51.100.1 192.0.2.1 '
        '443 49152 6 10 840 1439387263 1439387264 ACCEPT OK'
    ),
    (
        '2 123456789010 eni-102010ab 192.0.2.1 198.51.100.1 '
        '49152 443 6 20 1680 1439387264 1439387265 ACCEPT OK'
    ),
    (
        '2 123456789010 eni-102010ab 192.0.2.1 198.51.100.2 '
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
SAMPLE_RECORDS = [FlowRecord.from_message(m) for m in SAMPLE_INPUT]


class MainTestCase(TestCase):
    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    def test_main(self, mock_reader):
        main(['mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup', region_name=None, profile_name=None
        )

        main(['-s', '2015-05-05 14:20:00', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup', region_name=None,
            start_time=datetime(2015, 5, 5, 14, 20), profile_name=None
        )

        main(['--end-time', '2015-05-05 14:20:00', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup', region_name=None,
            end_time=datetime(2015, 5, 5, 14, 20), profile_name=None
        )

        main([
            '--time-format', '%Y-%m-%d',
            '--start-time', '2015-05-05',
            'mygroup'
        ])
        mock_reader.assert_called_with(
            log_group_name='mygroup', region_name=None,
            start_time=datetime(2015, 5, 5), profile_name=None
        )

        main(['--region', 'us-west-1', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup', region_name='us-west-1',
            profile_name=None
        )

        main(['--profile', 'my-profile', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup', region_name=None,
            profile_name='my-profile'
        )

        main(['--filter-pattern', 'REJECT', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup', region_name=None,
            profile_name=None, filter_pattern='REJECT'
        )

    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.print', create=True)
    def test_main_print(self, mock_out, mock_reader):
        mock_out.stdout = io.BytesIO()
        mock_reader.return_value = SAMPLE_RECORDS
        main(['mygroup'])
        for call, record in zip_longest(mock_out.mock_calls, SAMPLE_INPUT):
            __, args, kwargs = call
            line = args[0]
            self.assertEqual(line, record)

    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.print', create=True)
    def test_main_print_count(self, mock_out, mock_reader):
        mock_out.stdout = io.BytesIO()
        mock_reader.return_value = SAMPLE_RECORDS

        with self.assertRaises(ValueError):
            main(['mygroup', 'print', 'two'])

        with self.assertRaises(RuntimeError):
            main(['mygroup', 'print', '2', '3'])

        main(['mygroup', 'print', '2'])
        for call, record in zip_longest(mock_out.mock_calls, SAMPLE_INPUT[:2]):
            __, args, kwargs = call
            line = args[0]
            self.assertEqual(line, record)

    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.print', create=True)
    def test_main_ipset(self, mock_out, mock_reader):
        mock_out.stdout = io.BytesIO()
        mock_reader.return_value = SAMPLE_RECORDS
        main(['mygroup', 'ipset'])

        expected_set = set()
        for record in SAMPLE_INPUT:
            data = record.split()
            expected_set.add(data[3])
            expected_set.add(data[4])
        # don't include SKIPDATA/NODATA in results
        expected_set.remove('-')

        # make sure the number of lines are the same as the size of the set
        self.assertEqual(len(mock_out.mock_calls), len(expected_set))

        actual_set = set()
        for __, args, kwargs in mock_out.mock_calls:
            line = args[0]
            actual_set.add(line)
        self.assertEqual(actual_set, expected_set)

    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.print', create=True)
    def test_main_findip(self, mock_out, mock_reader):
        mock_out.stdout = io.BytesIO()
        mock_reader.return_value = SAMPLE_RECORDS
        main(['mygroup', 'findip', '198.51.100.2'])

        expected_result = [SAMPLE_INPUT[2]]
        for call, record in zip_longest(mock_out.mock_calls, expected_result):
            __, args, kwargs = call
            line = args[0]
            self.assertEqual(line, record)

    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.print', create=True)
    def test_main_bad_action(self, mock_out, mock_reader):
        mock_out.stdout = io.BytesIO()
        mock_reader.return_value = SAMPLE_RECORDS
        main(['mygroup', '__'])

        expected_result = [
            'unknown action: __',
            'known actions: {}'.format(', '.join(actions)),
        ]
        for call, result in zip_longest(mock_out.mock_calls, expected_result):
            __, args, kwargs = call
            line = args[0]
            self.assertEqual(line, result)
