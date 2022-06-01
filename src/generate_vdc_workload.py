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

""" Generate VDC workload from Azure V1 dataset """

import argparse
import csv
import networkx
import os
import glog
import jsonlines
import sys
from collections import OrderedDict
from typing import Dict, Tuple, List

import lib.constants as const
from lib.workload import WorkloadPreprocesing


def _get_child_vdc_uuid(vdc_child_index: Dict[str, int], parent_vdc_uuid: str) -> str:
    """ Generate a child VDC UUID based on the child index.
    :vdc_child_index: a dict that contains all parent VDC UUIDs and their child index
    :parent_vdc_uuid: UUID of the parent VDC for which child index is being queried for
    :returns: child VDC UUID """
    if parent_vdc_uuid in vdc_child_index:
        vdc_child_index[parent_vdc_uuid] += 1
    else:
        vdc_child_index[parent_vdc_uuid] = 0
    return '{}{}{}'.format(
        parent_vdc_uuid, const.VDC_CONCAT_STR, vdc_child_index[parent_vdc_uuid])


def _chop_vdcs(events: const.EVENT_LIST_TYPE, max_vdc_size: int) -> Tuple[
        Dict[str, str], Dict[str, List[str]]]:
    """ Enforce max_vdc_size constrain by chopping VDCs to parent and child VDCs.
    Once VDC's peak size exceeds max_vdc_size we assign the rest of the VMs
    in that VDC to child VDCs. This function just generates VM membership
    information and workload generation generation is done in the caller function.
    :events: batched events
    :max_vdc_size: threshold for the peak VDC size
    :returns: (vm2vdc, vm_peers) -- vm2vdc is a dict that maps VM UUID to the
    VDC UUID that the VM belongs to. vm_peers captures all peer VMs that a VM
    should connect to in its creation time. """
    vdc_child_index = {} # type: Dict[str, int]; ex. {vdc_uuid: 3, ...}

    # ex. {vdc_uuid_orig: vdc_uuid_current, ...}
    vdc_uuid_map = {} # type: Dict[str, str]
    # ex. {vdc_uuid: [vm_uuid1, vm_uuid2, ...], ...}
    vdc_alive_vms = {} # type: Dict[str, List[str]]
    # ex. {vm_uuid: vdc_uuid, ...}
    vm2vdc = {} # type: Dict[str, str]
    # ex. {vm_uuid: [peer_vm_uuid1, ...], ...}
    vm_peers = {} # type: Dict[str, List[str]]

    # iterate through all ticks to enforce max_vdc_size constraint and
    # generate VM-to-VDC membership
    for tick, event_val in events.items():
        assert(len(event_val) != 0)
        event_val_list = list(event_val)

        # the first iteration over events to build the vm2vdc membership
        for event in event_val_list:
            if event[const.TYPE_STR] == const.VM_DELETE_STR:
                # we must have already seen create event for this
                vdc_uuid_current = vm2vdc[event[const.VM_UUID_STR]]
                assert(vdc_uuid_current in vdc_alive_vms)
                vdc_alive_vms[vdc_uuid_current].remove(event[const.VM_UUID_STR])
                continue

            # this is a create event: decide VDC membership for this VM
            if event[const.VDC_UUID_STR] not in vdc_uuid_map:
                # This is the first ever VM of this parent VDC.
                vdc_uuid_current = event[const.VDC_UUID_STR]
                vdc_uuid_map[event[const.VDC_UUID_STR]] = vdc_uuid_current
                vm2vdc[event[const.VM_UUID_STR]] = vdc_uuid_current
                vdc_alive_vms[vdc_uuid_current] = [event[const.VM_UUID_STR]]
                continue

            vdc_uuid_current = vdc_uuid_map[event[const.VDC_UUID_STR]]
            if len(vdc_alive_vms[vdc_uuid_current]) < max_vdc_size:
                vdc_alive_vms[vdc_uuid_current].append(event[const.VM_UUID_STR])
                vm2vdc[event[const.VM_UUID_STR]] = vdc_uuid_current
            else: # time to create (yet another) child VDC
                child_vdc_uuid = _get_child_vdc_uuid(vdc_child_index, event[const.VDC_UUID_STR])
                vdc_alive_vms[child_vdc_uuid] = [event[const.VM_UUID_STR]]
                vdc_uuid_map[event[const.VDC_UUID_STR]] = child_vdc_uuid
                vm2vdc[event[const.VM_UUID_STR]] = child_vdc_uuid

        # the second iteration over events to build vm_peers
        for event in event_val_list:
            if event[const.TYPE_STR] == const.VM_DELETE_STR:
                continue
            # this is a create event: add all alive VDC VM as peers, except itself
            # note that vdc_alive_vms operates on vdc_uuid_current
            # not on the event.vdc_uuid. This is an important distinction.
            all_vm_peers = vdc_alive_vms[vm2vdc[event[const.VM_UUID_STR]]]
            vm_peers[event[const.VM_UUID_STR]] = [
                pvm for pvm in all_vm_peers if pvm != event[const.VM_UUID_STR]]

    return vm2vdc, vm_peers


def _add_network(events: const.EVENT_LIST_TYPE, vm2vdc: Dict[str, str],
        vm_peers: Dict[str, List[str]], band_per_core: int) -> None:
    """ Add network connectivity to the workload.
    :events: batched events
    :vm2vdc: a dict that maps VM UUID to the VDC UUID that the VM belongs to
    :vm_peers: captures all peer VMs that a VM should connect to in its creation time
    :band_per_core: network bandwidth amount per VM core
    :returns: directly modifies the events parameter """
    networked_workload = OrderedDict()  # type: const.EVENT_LIST_TYPE
    # iterate through all ticks to extract VM cores
    vm_cores_dict = {} # type: Dict[str, int]
    for tick, event_val in events.items():
        assert(len(event_val) != 0)
        for event in event_val:
            if event[const.TYPE_STR] == const.VM_CREATE_STR:
                vm_cores_dict[event[const.VM_UUID_STR]] = event[const.CORES_STR]

    # iterate through all ticks to add VM network requirements
    for tick, event_val in events.items():
        assert(len(event_val) != 0)
        tick_str = '{}{}'.format(const.TICK_STR, tick)
        networked_workload[tick_str] = list(event_val)
        for event in networked_workload[tick_str]:
            # change the VDC UUID to reflect the chopped VDCs
            event[const.VDC_UUID_STR] = vm2vdc[event[const.VM_UUID_STR]]

            if event[const.TYPE_STR] == const.VM_DELETE_STR:
                continue # we don't modify the delete events

            conn = {}
            for peer_vm_uuid in vm_peers[event[const.VM_UUID_STR]]:
                min_cores = min(event[const.CORES_STR], vm_cores_dict[peer_vm_uuid])
                conn[peer_vm_uuid] = int(band_per_core * min_cores)
            event[const.BW_STR] = conn
    return networked_workload


def _check_virtual_network(workload: const.EVENT_LIST_TYPE) -> bool:
    """ Sanity check the virtual network topology of the workload.
    We primarily do a bandwidth value symmetry check in peer VMs to
    confirm that if we see weight(node1, node2)=X,
    we should also (later) see weight(node2, node1)=X.
    :workload: workload with ticks and all other properties
    :returns: True if check passes, fails otherwise """
    peer_vms = networkx.Graph()
    for tick, event_val in workload.items():
        assert(len(event_val) != 0)
        for event in event_val:
            src_vm = event[const.VM_UUID_STR]
            if event[const.TYPE_STR] == const.VM_DELETE_STR:
                # delete all edges from this node
                if peer_vms.has_node(src_vm):
                    peer_vms.remove_node(src_vm)
                continue

            # do bandwidth symmetry check
            for peer_vm, band in event[const.BW_STR].items():
                assert(band > 0)
                if peer_vms.has_edge(src_vm, peer_vm):
                    assert(peer_vms.edges[src_vm, peer_vm][const.WEIGHT_STR] == band)
                    peer_vms.remove_edge(src_vm, peer_vm)
                else:
                    peer_vms.add_weighted_edges_from([(src_vm, peer_vm, band)])

    # an empty network graph double confirms that all created nodes got deleted
    assert(len(peer_vms.nodes) == 0)
    assert(len(peer_vms.edges) == 0)
    glog.info('generated workload passes the bandwidth symmetry check')
    return True


def generate_ml(vmtable_fname: str, band_per_core: int) -> const.EVENT_LIST_TYPE:
    """ Generates ML VDC workload.
    :vmtable_fname: name of the csv file with vmtable
    :band_per_core: VM-to-VM bandwidth is proportional to number of VM cores
    :returns: event list with network """
    sorted_csv = WorkloadPreprocesing.sort_events(vmtable_fname)
    events_sorted, vm_lifetime = WorkloadPreprocesing.sanitize(sorted_csv)
    batched_vdcs = WorkloadPreprocesing.batch_vdcs(events_sorted)

    max_vdc_size = const.MAX_PEAK_VDC_SIZE
    vm2vdc, vm_peers = _chop_vdcs(batched_vdcs, max_vdc_size)
    ml_workload = _add_network(batched_vdcs, vm2vdc, vm_peers, band_per_core)
    assert(_check_virtual_network(ml_workload))
    # print number of VDCs in ML workload (after chopping)
    WorkloadPreprocesing.get_vdc_stats(events_sorted)
    return ml_workload


if __name__ == "__main__":
    """ Generate VDC workload and output to the JSON file. These VDC workloads
    have multiple VM create/delete requests per tick, all VM creates for particular
    VDC in a tick listed sequentially (aka batched), and in each tick creates are
    followed by the deletes. """

    CLI = argparse.ArgumentParser(description='Generate VDC workload. Run as' +
        '$ python generate_vdc_workload.py --help')

    group = CLI.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '-ml',
        '--machine-learning',
        action='store_true',
        default=False,
        help='ML workload where VMs have all-to-all connectivity.')

    CLI.add_argument(
        '-bw',
        '--bandwidth',
        default=1,
        help='Per-core bandwidth for VMs (in Mbps, should be >=1)')

    CLI.add_argument(
        '-vs',
        '--vdc-size',
        default=1,
        help='VDC size for oneshot workload')

    CLI.add_argument(
        '-i',
        '--input',
        default='sample_input/vmtable_sample_handmade.csv',
        help='Path to input workload')

    CLI.add_argument(
        '-o',
        '--output',
        default='workload.jsonl',
        # use this for Azure workload: ../../AzurePublicDataset/data/vmtable.csv
        help='Workload output file name')

    ARGS = CLI.parse_args()

    #vmtable_fname = os.path.join('../../../AzurePublicDataset/data', 'vmtable.csv')
    vmtable_fname = os.path.join('../../tests/data', 'vmtable_sample_handmade.csv')

    if ARGS.machine_learning:
        event_dict = generate_ml(ARGS.input, int(ARGS.bandwidth))
    else:
        glog.info('Only Machine Learning workload is supported. Exiting.')
        sys.exit(1)

    with jsonlines.open(ARGS.output, 'w') as writer:
        for k, v in event_dict.items():
            writer.write({k: v})
    glog.info('successfully wrote to output file {}'.format(ARGS.output))
