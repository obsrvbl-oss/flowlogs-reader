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
import io

from datetime import datetime
from itertools import zip_longest
from io import StringIO
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flowlogs_reader import FlowRecord
from flowlogs_reader.__main__ import main, actions


SAMPLE_INPUT = [
    (
        '2 123456789010 eni-102010ab 198.51.100.1 192.0.2.1 '
        '443 49152 6 10 840 1439387263 1439387264 ACCEPT OK '
        '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -'
    ),
    (
        '2 123456789010 eni-102010ab 192.0.2.1 198.51.100.1 '
        '49152 443 6 20 1680 1439387264 1439387265 ACCEPT OK '
        '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -'
    ),
    (
        '2 123456789010 eni-102010ab 192.0.2.1 198.51.100.2 '
        '49152 443 6 20 1680 1439387265 1439387266 REJECT OK '
        '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -'
    ),
    (
        '2 123456789010 eni-1a2b3c4d - - - - - - - '
        '1431280876 1431280934 - NODATA '
        '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -'
    ),
    (
        '2 123456789010 eni-4b118871 - - - - - - - '
        '1431280876 1431280934 - SKIPDATA '
        '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -'
    ),
]
SAMPLE_RECORDS = [
    FlowRecord.from_cwl_event({'message': m}) for m in SAMPLE_INPUT
]


class MainTestCase(TestCase):
    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    def test_main(self, mock_reader):
        main(['mygroup'])
        mock_reader.assert_called_with(log_group_name='mygroup', fields=None)

        main(['-s', '2015-05-05 14:20:00', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup',
            fields=None,
            start_time=datetime(2015, 5, 5, 14, 20),
        )

        main(['--end-time', '2015-05-05 14:20:00', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup',
            fields=None,
            end_time=datetime(2015, 5, 5, 14, 20),
        )

        main(
            [
                '--time-format',
                '%Y-%m-%d',
                '--start-time',
                '2015-05-05',
                'mygroup',
                '--thread-count',
                '2',
            ]
        )
        mock_reader.assert_called_with(
            log_group_name='mygroup',
            fields=None,
            start_time=datetime(2015, 5, 5),
            thread_count=2,
        )

        main(['--region', 'us-west-1', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup',
            fields=None,
            region_name='us-west-1',
        )

        main(['--profile', 'my-profile', 'mygroup'])
        mock_reader.assert_called_with(
            log_group_name='mygroup', fields=None, profile_name='my-profile'
        )

        main(
            [
                '--filter-pattern',
                'REJECT',
                '--fields',
                '${account-id} ${action}',
                'mygroup',
            ]
        )
        mock_reader.assert_called_with(
            log_group_name='mygroup',
            fields=('account-id', 'action'),
            filter_pattern='REJECT',
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

    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.print', create=True)
    def test_main_missing_arn(self, mock_out, mock_reader):
        mock_out.stdout = io.BytesIO()
        mock_reader.return_value = SAMPLE_RECORDS
        main(['--external-id', 'uuid4', 'mygroup'])

        expected_result = [
            'must give a --role-arn if an --external-id is given',
        ]
        for call, result in zip_longest(mock_out.mock_calls, expected_result):
            __, args, kwargs = call
            line = args[0]
            self.assertEqual(line, result)

    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.boto3', autospec=True)
    def test_main_assume_role(self, mock_boto3, mock_reader):
        mock_boto3.client.return_value.assume_role.return_value = {
            'Credentials': {
                'AccessKeyId': 'myaccesskeyid',
                'SecretAccessKey': 'mysecretaccesskey',
                'SessionToken': 'mysessiontoken',
            }
        }
        mock_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = (
            mock_client
        )
        mock_reader.return_value = []
        main(['--role-arn', 'myarn', '--external-id', 'uuid4', 'mygroup'])

        session = mock_boto3.session.Session
        session.assert_called_once_with(
            aws_access_key_id='myaccesskeyid',
            aws_secret_access_key='mysecretaccesskey',
            aws_session_token='mysessiontoken',
        )
        session.return_value.client.assert_called_once_with('logs')
        mock_reader.assert_called_once_with(
            log_group_name='mygroup', fields=None, boto_client=mock_client
        )

    @patch('flowlogs_reader.__main__.S3FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.boto3', autospec=True)
    def test_main_assume_role_s3(self, mock_boto3, mock_reader):
        mock_boto3.client.return_value.assume_role.return_value = {
            'Credentials': {
                'AccessKeyId': 'myaccesskeyid',
                'SecretAccessKey': 'mysecretaccesskey',
                'SessionToken': 'mysessiontoken',
            }
        }
        mock_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = (
            mock_client
        )
        mock_reader.return_value = []
        args = [
            '--role-arn',
            'myarn',
            '--external-id',
            'uuid4',
            '--location-type',
            's3',
            'mybucket',
        ]
        main(args)

        session = mock_boto3.session.Session
        session.assert_called_once_with(
            aws_access_key_id='myaccesskeyid',
            aws_secret_access_key='mysecretaccesskey',
            aws_session_token='mysessiontoken',
        )
        session.return_value.client.assert_called_once_with('s3')
        mock_reader.assert_called_once_with(
            'mybucket', boto_client=mock_client
        )

    @patch('flowlogs_reader.__main__.FlowLogsReader', autospec=True)
    def test_main_aggregate(self, mock_reader):
        mock_reader.return_value = [SAMPLE_RECORDS[0], SAMPLE_RECORDS[0]]
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            main(['mygroup', 'aggregate'])
            output = mock_stdout.getvalue().splitlines()

        actual_header = output[0].split('\t')
        expected_header = [
            'bytes',
            'dstaddr',
            'dstport',
            'end',
            'packets',
            'protocol',
            'srcaddr',
            'srcport',
            'start',
        ]
        self.assertEqual(actual_header, expected_header)

        actual_line = output[1].split('\t')
        expected_line = [
            '1680',
            '192.0.2.1',
            '49152',
            '2015-08-12 13:47:44',
            '20',
            '6',
            '198.51.100.1',
            '443',
            '2015-08-12 13:47:43',
        ]
        self.assertEqual(actual_line, expected_line)

    @patch('flowlogs_reader.__main__.S3FlowLogsReader', autospec=True)
    @patch('flowlogs_reader.__main__.print', create=True)
    def test_s3_destination(self, mock_out, mock_reader):
        mock_out.stdout = io.BytesIO()
        mock_reader.return_value = SAMPLE_RECORDS
        main(
            [
                'mybucket/myprefix',
                '--location-type',
                's3',
                '--include-accounts',
                '999999999998, 999999999999',
                '--include-regions',
                'us-east-1,us-east-2',
            ]
        )
        mock_reader.assert_called_once_with(
            location='mybucket/myprefix',
            include_accounts=['999999999998', '999999999999'],
            include_regions=['us-east-1', 'us-east-2'],
        )
        for call, record in zip_longest(mock_out.mock_calls, SAMPLE_INPUT):
            __, args, kwargs = call
            line = args[0]
            self.assertEqual(line, record)

    @patch('flowlogs_reader.__main__.LocalFileReader', autospec=True)
    @patch('flowlogs_reader.__main__.print', create=True)
    def test_file_destination(self, mock_out, mock_reader):
        mock_out.stdout = io.BytesIO()
        mock_reader.return_value = SAMPLE_RECORDS
        main(['--location-type', 'file', '/tmp/test-file.csv.gz'])
        mock_reader.assert_called_once_with(location='/tmp/test-file.csv.gz')
        for call, record in zip_longest(mock_out.mock_calls, SAMPLE_INPUT):
            __, args, kwargs = call
            line = args[0]
            self.assertEqual(line, record)
