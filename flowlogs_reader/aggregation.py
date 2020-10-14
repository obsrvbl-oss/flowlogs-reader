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
from collections import defaultdict
from datetime import datetime

KEY_FIELDS = ('srcaddr', 'dstaddr', 'srcport', 'dstport', 'protocol')


class _FlowStats:
    """
    An aggregator for flow records. Sums bytes and packets and keeps track of
    the active time window.
    """

    __slots__ = ['packets', 'bytes', 'start', 'end']

    def __init__(self):
        self.start = datetime.max
        self.end = datetime.min
        self.packets = 0
        self.bytes = 0

    def update(self, flow_record):
        if flow_record.start < self.start:
            self.start = flow_record.start
        if flow_record.end > self.end:
            self.end = flow_record.end
        self.packets += flow_record.packets
        self.bytes += flow_record.bytes

    def to_dict(self):
        return {x: getattr(self, x) for x in self.__slots__}


def aggregated_records(all_records, key_fields=KEY_FIELDS):
    """
    Yield dicts that correspond to aggregates of the flow records given by
    the sequence of FlowRecords in `all_records`. Skips incomplete records.
    This will consume the `all_records` iterator, and requires enough memory to
    be able to read it entirely.
    `key_fields` optionally contains the fields over which to aggregate. By
    default it's the typical flow 5-tuple.
    """
    flow_table = defaultdict(_FlowStats)
    for flow_record in all_records:
        key = tuple(getattr(flow_record, attr) for attr in key_fields)
        if any(x is None for x in key):
            continue
        flow_table[key].update(flow_record)

    for key in flow_table:
        item = {k: v for k, v in zip(key_fields, key)}
        item.update(flow_table[key].to_dict())
        yield item
