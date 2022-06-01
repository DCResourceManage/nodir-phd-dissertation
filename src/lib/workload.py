# Copyright (c) 2021 Nodir Kodirov
# 
# Licensed under the MIT License (the "License").
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

""" A helper class for workload processing """

import csv
import glog
from collections import OrderedDict
from collections import deque
from typing import Dict, Tuple, List

import lib.constants as const

class WorkloadPreprocesing:
    """ A class to contain functions to preprocess the Azure workload traces.
    We keep these functions here as they are commonly used by the workload
    generation and plotting scripts. """

    SORTED_CSV_TYPE = Dict[Tuple[int, str], str]

    @staticmethod
    def _validate_event_time(event_type: str, event_time: int) -> (int, bool):
        """ The Azure dataset is sampled every 5 minutes which makes all event times
        multiplier of 300 seconds. However, there are 27 events with invalid
        timestamps. For all events if create_time is invalid then delete_time is
        invalid, too.
        Note that some VM create and some VM delete events have the same timestamp.
        Looking at these timestamp, we can just deduct 100 seconds and
        they become multiplier of 300. This is a simple correction we do to make
        all timestamps valid.
        :event_type: indicates whether this is create or delete event
        :event_time: timestamp when event happened
        :returns: (event_time, status) tuple. The event_time contains the corrected timestamp
        status indicates whether original timestamp was invalid or not """
        time_drift = const.TIME_DRIFT
        invalid_set = const.INVALID_TIMESTAMPS
        if event_time in invalid_set:
            event_time -= time_drift
            glog.debug('{} event had an invalid time {}; changed to {}'.format(
                event_type, (event_time + time_drift), event_time))
            return event_time, True
        return event_time, False

    @staticmethod
    def sort_events(vmtable_fname: str) -> SORTED_CSV_TYPE:
        """ Sort events by their create tick.
        We do not know if Azure CSV file is sorted by event timestamp.
        In fact, eyeballing full vmtable.csv reveals that it is not sorted,
        i.e., a VM with earlier create timestamp does not appear earlier (line #)
        in the CSV file. This is probably a noise during workload trace collection.
        When we create a VDC workload we have to make sure that events are
        processed in the temporal order. Otherwise, the VDC scheduler considers
        VMs with earlier create time in the wrong order that can result in an
        invalid scheduler evaluation compared to the real one. This function
        removes the data capturing noise by sorting the events in the temporal order.

        We walk through the CSV file, create a dictionary with the (VM create time,
        VM UUID) tuple as the key, sort the dict by key, and return the sorted dict.
        A value in dict is the whole line at CSV.
        Note that we need to add VM UUID as the part of the tuple key to
        ensure uniqueness of the dict keys. Since the first item of the tuple
        is the VM create time, the sorted dict is guaranteed to have the temporal
        order. Also note that this function assumes VM UUIDs are unique,
        a requirement that holds in the Azure dataset.
        :vmtable_fname: name of the csv file with vmtable
        :returns: dict sorted by keys """
        vmtable_fd = open(vmtable_fname, mode='r')
        vmtable_reader = csv.reader(vmtable_fd)

        # For sorted_csv, we make Tuple[int, str] the dictionary key
        # (although we only need an int for sorting) to make the keys unique.
        # I.e., there are many VMs created in each timestamp/tick,
        # but if we combine (create_time, vm_uuid) it is guaranteed to be unique.
        sorted_csv = dict()  # type: SORTED_CSV_TYPE

        for row in vmtable_reader:
            if len(row) == 0:  # skip an empty line
                continue

            create_time = int(row[3])  # this should be an int for sorting to work
            vm_uuid = row[0]
            sorted_csv[(create_time, vm_uuid)] = row

        sorted_csv = OrderedDict(sorted(sorted_csv.items(), key=lambda key: key[0]))
        # close the .csv file
        vmtable_fd.close()

        # sanity check the event timestamp for the temporal order
        prev_tick = 0
        for event_key, row in sorted_csv.items():
            tick_val = event_key[0]
            # each 'row' is a single event (either create VM or delete VM)
            if len(row) == 0:  # skip an empty line
                continue
            create_time = int(row[3])
            assert(tick_val == create_time)
            assert(tick_val >= prev_tick)
            prev_tick = tick_val

        return sorted_csv

    @staticmethod
    def _get_vdc_stats_before(sorted_csv: SORTED_CSV_TYPE) -> int:
        """ Get VDC stats before sanitizing the events.
        :sorted_csv: dict with Azure CSV lines sorted by the VM create tick
        :returns: number of VMs """
        nvms_total = 0
        unique_vdc_uuids = set()
        for row in sorted_csv.values():
            # each 'row' is a single event (either create VM or delete VM)
            if len(row) == 0:  # skip an empty line
                continue
            nvms_total += 1
            dep_id = row[2]
            unique_vdc_uuids.add(dep_id)
        glog.info(f'before sanitize: there are {nvms_total} VMs in {len(unique_vdc_uuids)} VDCs')
        return nvms_total

    @staticmethod
    def get_vdc_stats(events_sorted: const.EVENT_LIST_TYPE) -> int:
        """ Get VDC stats after sanitizing the events.
        :events_sorted: VDC events in ticks
        :returns: number of VMs """
        nvms_total = 0
        unique_vdc_uuids = set()
        for tick_val, vm_events in events_sorted.items():
            if len(vm_events) == 0: # skip the ticks with no events
                continue

            for vm_event in vm_events:
                if vm_event[const.TYPE_STR] == const.VM_CREATE_STR:
                    nvms_total += 1
                    unique_vdc_uuids.add(vm_event[const.VDC_UUID_STR])
                else:
                    assert(vm_event[const.TYPE_STR] == const.VM_DELETE_STR)

        glog.info(f'after sanitize: there are {nvms_total} VMs in {len(unique_vdc_uuids)} VDCs')
        return nvms_total

    @staticmethod
    def sanitize(sorted_csv: SORTED_CSV_TYPE) -> const.EVENT_LIST_TYPE:
        """ Sanitize the Azure workload by (1) removing VMs that have
        identical create and delete ticks (aka insta-VMs) and (2) rounding the
        VM create/delete ticks to the nearest valid tick. The generated workload
        has VMs with the create/delete times and CPU/RAM information.
        :sorted_csv: dict with Azure CSV lines sorted by the VM create tick
        :returns: event list without network requirements """
        # insta_vms are the VMs with equal create and delete times
        num_of_insta_vms = 0
        vm_creates_total, vm_creates_in = 0, 0
        invalid_creates, invalid_deletes = 0, 0
        events_sorted = OrderedDict()  # type: const.EVENT_LIST_TYPE
        # dict that contains the numbers of seconds the VM lives (delete-create)
        vm_lifetime = {} # type: Dict[List[str, int]]

        sec_in_5mins = 5 * 60
        # number of 5min units within a month. Sec. 6.2 of the Resource Central
        # paper says there are 336k VM arrivals over a period of 1 month.
        # Note that this VM arrival workload is not for the entire 3month duration as
        # reported in other parts of the paper (from Nov 16, 2016 to Feb. 16, 2017).
        total_items = 12 * 24 * 30 # 12 units in 1hour, 24h in day, 30 days.

        # fill up the timeline with zeroes
        for time_unit in range(total_items):
            events_sorted[time_unit * sec_in_5mins] = []

        nvms_total_before = WorkloadPreprocesing._get_vdc_stats_before(sorted_csv)

        for row in sorted_csv.values():
            # each 'row' is a single event (either create VM or delete VM)
            if len(row) == 0:  # skip an empty line
                continue
            vm_creates_total += 1
            vm_id, dep_id = row[0], row[2]
            create_time, delete_time = int(row[3]), int(row[4])
            vm_cpu_cores, vm_mem_in_gb = float(row[9]), float(row[10])

            if create_time == delete_time:
                glog.debug('VM %s has equal create_time: %s and delete_time %s',
                        vm_id, create_time, delete_time)
                num_of_insta_vms += 1
                continue
            assert(create_time < delete_time)

            vm_creates_in += 1
            create_time, was_invalid = WorkloadPreprocesing._validate_event_time(
                    const.VM_CREATE_STR, create_time)
            if was_invalid:
                invalid_creates += 1

            delete_time, was_invalid = WorkloadPreprocesing._validate_event_time(
                    const.VM_DELETE_STR, delete_time)
            if was_invalid:
                invalid_deletes += 1

            event = {const.TYPE_STR: const.VM_CREATE_STR, const.VDC_UUID_STR: dep_id,
                    const.VM_UUID_STR: vm_id, const.CORES_STR: vm_cpu_cores,
                    const.RAM_STR: vm_mem_in_gb}

            vm_lifetime[vm_id] = {'create_tick': create_time, 'delete_tick': delete_time,
                    'lifetime': (delete_time - create_time)}
            assert(vm_lifetime[vm_id]['lifetime'] > 0)

            events_sorted[create_time].append(event)

            # We only need vdc_uuid and vm_uuid for delete events. Note that
            # we should still create another dict object to avoid modifying the
            # `create` object above.
            event = {const.TYPE_STR: const.VM_DELETE_STR, const.VDC_UUID_STR: dep_id,
                    const.VM_UUID_STR: vm_id}

            # VM delete events are added to the left as they have to be considered
            # before the `create` events by the scheduler. Such ordering of
            # events is important to avoid overusing DC resources.
            events_sorted[delete_time].append(event)

        nvms_total_after = WorkloadPreprocesing.get_vdc_stats(events_sorted)
        assert(nvms_total_before == vm_creates_total)
        assert(nvms_total_after == vm_creates_in)

        glog.info('num_of_insta_vms: {}, vm_creates_total: {} (which makes insta_vms={}%%)'.format(
            num_of_insta_vms, vm_creates_total, round(100 * num_of_insta_vms / vm_creates_total, 2)))
        glog.info('sum(invalid_creates: {}, invalid_deletes: {}) = {}'.format(
            invalid_creates, invalid_deletes, (invalid_creates+invalid_deletes)))
        glog.info('vm_creates_in: {}, vm_creates_total: {} (which makes vm_creates_in={}%%)'.format(
            vm_creates_in, vm_creates_total, round(100 * vm_creates_in / vm_creates_total, 2)))

        return events_sorted, vm_lifetime

    @staticmethod
    def batch_vdcs(events_sorted: const.EVENT_LIST_TYPE) -> const.EVENT_LIST_TYPE:
        """ Batch the VDC VMs within the same tick, e.g., if two VMs in a tick belong
        to the same VDC, these two VMs should appear one after another
        (without VM of another VDC between them).
        :events_sorted: sorted events with their tick
        :returns: the same sorted events but with batched VDCs within each tick """
        batched = OrderedDict()  # type: const.EVENT_LIST_TYPE

        # For events_in_tick, we make Tuple[vdc_uuid, vm_uuid] the dictionary key
        # (although we only need vdc_uuid for sorting) to make the keys unique.
        # I.e., there could be many vdc_uuid in each timestamp/tick,
        # but if we combine (vdc_uuid, vm_uuid) it is guaranteed to be unique.
        events_in_tick = OrderedDict()

        for tick_val, vm_events in events_sorted.items():
            if len(vm_events) == 0: # skip the ticks with no events
                continue
            glog.debug('{}'.format(tick_val))
            events_in_tick = OrderedDict()
            glog.debug('{}'.format(vm_events))
            for vm_event in vm_events:
                events_in_tick[(vm_event[const.VDC_UUID_STR], vm_event[const.VM_UUID_STR])] = vm_event

            # sort by vdc_uuid (0th item in the tuple)
            batched_events = OrderedDict(sorted(events_in_tick.items(), key=lambda key: key[0]))
            batched[tick_val] = deque()  # deque: double-ended queue
            # delete events should appear before the create events in a tick
            for item_key, vm_event in batched_events.items():
                if vm_event[const.TYPE_STR] == const.VM_CREATE_STR:
                    batched[tick_val].append(vm_event)
                elif vm_event[const.TYPE_STR] == const.VM_DELETE_STR:
                    batched[tick_val].appendleft(vm_event)
                else:
                    glog.error('Unsupported event type. Exit.')
                    sys.exit(1)

            glog.debug('{}'.format(batched[tick_val]))

        return batched
