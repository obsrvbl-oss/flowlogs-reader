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
import sys

from argparse import ArgumentParser
from datetime import datetime
from itertools import chain
from uuid import uuid4

import boto3

from .aggregation import aggregated_records
from .flowlogs_reader import (
    FlowLogsReader,
    NODATA,
    S3FlowLogsReader,
    SKIPDATA,
)

actions = {}


def action_print(reader, *args):
    """Simply print the Flow Log records to output."""
    arg_count = len(args)
    if arg_count == 0:
        stop_after = 0
    elif arg_count == 1:
        stop_after = int(args[0])
    else:
        raise RuntimeError("0 or 1 arguments expected for action 'print'")

    for i, record in enumerate(reader, 1):
        print(record.to_message())
        if i == stop_after:
            break


actions['print'] = action_print


def action_ipset(reader, *args):
    """Show the set of IPs seen in Flow Log records."""
    ip_set = set()
    for record in reader:
        if record.log_status in (SKIPDATA, NODATA):
            continue
        ip_set.add(record.srcaddr)
        ip_set.add(record.dstaddr)

    for ip in ip_set:
        print(ip)


actions['ipset'] = action_ipset


def action_findip(reader, *args):
    """Find Flow Log records involving a specific IP or IPs."""
    target_ips = set(args)
    for record in reader:
        if (record.srcaddr in target_ips) or (record.dstaddr in target_ips):
            print(record.to_message())


actions['findip'] = action_findip


def action_aggregate(reader, *args):
    """Aggregate flow records by 5-tuple and print a tab-separated stream"""
    all_aggregated = aggregated_records(reader)
    first_row = next(all_aggregated)
    keys = sorted(first_row.keys())
    print(*keys, sep='\t')

    # Join the first row with the rest of the rows and print them
    iterable = chain([first_row], all_aggregated)
    for item in iterable:
        print(*[item[k] for k in keys], sep='\t')


actions['aggregate'] = action_aggregate


def get_reader(args):
    kwargs = {}
    time_format = args.time_format

    if args.location_type == 'cwl':
        cls = FlowLogsReader
        client_type = 'logs'
    elif args.location_type == 's3':
        cls = S3FlowLogsReader
        client_type = 's3'

    if args.region:
        kwargs['region_name'] = args.region

    if args.profile:
        kwargs['profile_name'] = args.profile

    if args.start_time:
        kwargs['start_time'] = datetime.strptime(args.start_time, time_format)

    if args.end_time:
        kwargs['end_time'] = datetime.strptime(args.end_time, time_format)

    if args.location_type == 'cwl' and args.filter_pattern:
        kwargs['filter_pattern'] = args.filter_pattern

    if args.location_type == 's3' and args.include_accounts:
        kwargs['include_accounts'] = [
            x.strip() for x in args.include_accounts.split(',')
        ]

    if args.location_type == 's3' and args.include_regions:
        kwargs['include_regions'] = [
            x.strip() for x in args.include_regions.split(',')
        ]

    if args.thread_count:
        kwargs['thread_count'] = args.thread_count

    # Switch roles for access to another account
    if args.role_arn:
        assume_role_kwargs = {}
        assume_role_kwargs['RoleArn'] = args.role_arn
        assume_role_kwargs['RoleSessionName'] = str(uuid4())[:32]
        if args.external_id:
            assume_role_kwargs['ExternalId'] = args.external_id

        sts_client = boto3.client('sts')
        resp = sts_client.assume_role(**assume_role_kwargs)
        session_kwargs = {
            'aws_access_key_id': resp['Credentials']['AccessKeyId'],
            'aws_secret_access_key': resp['Credentials']['SecretAccessKey'],
            'aws_session_token': resp['Credentials']['SessionToken'],
        }
        session = boto3.session.Session(**session_kwargs)
        boto_client = session.client(client_type)
        kwargs['boto_client'] = boto_client

    return cls(args.location, **kwargs)


def main(argv=None):
    argv = argv or sys.argv[1:]
    parser = ArgumentParser(description='Read VPC Flow Log Records')
    # Required paramters
    parser.add_argument(
        'location',
        type=str,
        help='CloudWatch Logs group name or S3 bucket/prefix',
    )
    parser.add_argument(
        'action',
        type=str,
        nargs='*',
        default=['print'],
        help='action to take on log records',
    )
    # Location paramters
    parser.add_argument(
        '--location-type',
        type=str,
        help='location type (CloudWatch Logs or S3), default is cwl',
        choices=['cwl', 's3'],
        default='cwl',
    )
    parser.add_argument(
        '--region', type=str, default='', help='AWS region for the location'
    )
    # Time filter paramters
    parser.add_argument(
        '--start-time',
        '-s',
        type=str,
        help='return records at or after this time',
    )
    parser.add_argument(
        '--end-time', '-e', type=str, help='return records before this time'
    )
    parser.add_argument(
        '--time-format',
        type=str,
        default='%Y-%m-%d %H:%M:%S',
        help='format of time to parse',
    )
    # Other filtering parameters
    parser.add_argument(
        '--filter-pattern',
        type=str,
        help='return records that match this pattern (CWL only)',
    )
    parser.add_argument(
        '--include-accounts',
        type=str,
        help='comma-separated list of accounts to consider (S3 only)',
    )
    parser.add_argument(
        '--include-regions',
        type=str,
        help='comma-separated list of regions to consider (S3 only)',
    )
    # AWS paramters
    parser.add_argument(
        '--profile',
        type=str,
        default='',
        help='boto3 configuration profile to use',
    )
    parser.add_argument(
        '--role-arn', type=str, help='assume role specified by this ARN'
    )
    parser.add_argument(
        '--external-id',
        type=str,
        help='use this external ID for cross-account acesss',
    )
    parser.add_argument(
        '--thread-count', type=int, help='number of threads used when reading'
    )
    args = parser.parse_args(argv)

    # Confirm the specified action is valid
    action = args.action[0]
    try:
        action_method = actions[action]
    except KeyError:
        print('unknown action: {}'.format(action), file=sys.stderr)
        print('known actions: {}'.format(', '.join(actions)), file=sys.stderr)
        return

    # Confirm the specified boto session arguments are valid
    if args.external_id and not args.role_arn:
        print('must give a --role-arn if an --external-id is given')
        return

    reader = get_reader(args)
    action_method(reader, *args.action[1:])


if __name__ == '__main__':
    main()
