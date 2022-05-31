# Copyright (c) 2021 Nodir Kodirov
# 
# Licensed under the MIT Licens (the "License").
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

""" Generate Juputer datacenter topology. """

import argparse
import json
import glog
from collections import OrderedDict
from typing import Dict, List

import lib.constants as const


class Jupiter(object):
    """ A class to keep all datacenter related constants and variables """
    def __init__(self, npods: int, nracks: int, nspine_blocks: int):
        self.npods = npods
        self.nracks = nracks
        self.nspine_blocks = nspine_blocks
        self.num_of_mbs = 8

        self.num_of_servers_per_rack = 48
        # num_of_servers_per_rack = 3 # for debugging only

        self.server_tor_bandwidth = 40
        self.tor_mb_bandwidth = 20  # TOR to MB is 2x10G
        self.mb_sb_bandwidth = 40  # MB to SB is 1x40G

        self.locality = {}
        self.servers = {}
        self.edges = []
        # type: Dict[int, List[str]]; ex: {0: [p0_r0, ...], ...}
        self.tors = OrderedDict()
        # type: Dict[int, List[str]]; ex: {0: [p0_mb0, ...], ...}
        self.mbs = OrderedDict()
        # type: List[str]; ex: [sb0, ...]
        self.sbs = []

    def generate_servers(self):
        """ Generate servers with CPU and RAM resources """
        actual_server_cores, actual_server_mem = 60, 256
        cpu_multiplier, mem_multiplier = 1, 1

        server_cores = actual_server_cores * cpu_multiplier
        server_mem = actual_server_mem * mem_multiplier

        for pod_index in range(self.npods):
            for rack_index in range(self.nracks):
                for server_index in range(self.num_of_servers_per_rack):
                    self.servers[f'p{pod_index}_r{rack_index}_s{server_index}'] = [
                        server_cores, server_mem]

    def generate_tor_switches(self):
        # generate ToR switch names
        for pod_index in range(self.npods):
            self.tors[pod_index] = []
            for rack_index in range(self.nracks):
                self.tors[pod_index].append('{}{}_{}{}'.format(
                    const.POD_PREFIX, pod_index, const.RACK_PREFIX, rack_index))

    def wire_tor_switches(self):
        # connect servers to TOR switches
        for pod_index, tor_names in self.tors.items():
            for rack_index, tor_name in enumerate(tor_names):
                for server_index in range(self.num_of_servers_per_rack):
                    server_name = '{}{}_{}{}_{}{}'.format(const.POD_PREFIX, pod_index,
                        const.RACK_PREFIX, rack_index, const.SERVER_PREFIX, server_index)
                    assert(server_name in self.servers)
                    self.edges.append([server_name, tor_name, self.server_tor_bandwidth])
        assert(len(self.edges) == self.npods * self.nracks * self.num_of_servers_per_rack)

    def generate_mb_switches(self):
        # generate middle block switch names
        for pod_index in range(npods):
            self.mbs[pod_index] = []
            for mb_index in range(self.num_of_mbs):
                self.mbs[pod_index].append('{}{}_{}{}'.format(
                    const.POD_PREFIX, pod_index, const.JUPITER_MB_PREFIX, mb_index))

    def wire_mb2tor_switches(self):
        # connect TOR switches to MB switches in all-to-all fashion
        prev_edges_len = len(self.edges)
        for pod_index, mb_names in self.mbs.items():
            for mb_name in mb_names:
                for tor_name in self.tors[pod_index]:
                    self.edges.append([tor_name, mb_name, self.tor_mb_bandwidth])
        assert(len(self.edges) - prev_edges_len == self.npods * self.num_of_mbs * self.nracks)

    def generate_sb_switches(self):
        # generate spine block switch names
        for sb_index in range(self.nspine_blocks):
            self.sbs.append('{}{}'.format(const.JUPITER_SB_PREFIX, sb_index))

    def generate_locality(self):
        """ Generate server locality information """
        for pod_index in range(self.npods):
            pod_name = f'{const.POD_STR}{pod_index}'
            self.locality[pod_name] = {}
            for rack_index in range(nracks):
                rack_name = f'{const.POD_PREFIX}{pod_index}_{const.RACK_PREFIX}{rack_index}'
                self.locality[pod_name][rack_name] = []
                for server_index in range(self.num_of_servers_per_rack):
                    self.locality[pod_name][rack_name].append('{}{}_{}{}_{}{}'.format(
                        const.POD_PREFIX, pod_index, const.RACK_PREFIX, rack_index,
                        const.SERVER_PREFIX, server_index))

    def full_topology(self):
        """ Generate full Jupiter topology with 64 pods. """
        self.generate_servers()
        self.generate_tor_switches()
        self.wire_tor_switches()
        self.generate_mb_switches()
        self.wire_mb2tor_switches()
        self.generate_sb_switches()
        prev_edges_len = len(self.edges)

        # connect P*MB* to P*SB switches by "striping in a superblock of 4 MBs"
        # i.e., p(x)_mb(y) connects to sb(64*(y%4)+i) where 0<=x,y<8 and 0<=i<64;
        # "i" is the MB-to-SB repeat index. See Google doc for details.
        # mbs = {0: [p0_mb0, ...], ...}; sbs = [sb0, ...]
        repeats = 64
        for pod_index, mb_names in self.mbs.items():
            for mb_index, mb_name in enumerate(mb_names):
                for repeat_index in range(repeats):
                    sb_name = self.sbs[64*(mb_index % 4) + repeat_index]
                    self.edges.append([mb_name, sb_name, self.mb_sb_bandwidth])
        # these two asserts are equivalent
        # each SB accepts 2 connections per AB (or pod); thus total edges in mb-to-sb layer is
        # num_of_sbs (self.nspine_blocks) * num_of_abs (self.pods) * 2
        assert(len(self.edges) - prev_edges_len == self.nspine_blocks * self.npods * 2)
        # each MB connects to 64 SBs; thus (self.npods * self.num_of_mbs * repeats)
        assert(len(self.edges) - prev_edges_len == self.npods * self.num_of_mbs * repeats)

        self.generate_locality()
        return self.servers, self.edges, self.locality

    def four_pods(self):
        """ Generate partial Jupiter topology with 4 pods. """
        self.generate_servers()
        self.generate_tor_switches()
        self.wire_tor_switches()
        self.generate_mb_switches()
        self.wire_mb2tor_switches()
        self.generate_sb_switches()
        prev_edges_len = len(self.edges)

        # each MB-to-SB link is 4x40G in 4 pod topology
        self.mb_sb_bandwidth = 160
        # connect P*MB to every P*SB switch, i.e.,
        # p(x)_mb(y) connects to sb(i) where 0<=x<4, 0<=y<8 and 0<=i<16;
        # "i" is the MB-to-SB repeat index. See Google doc for details.
        # mbs = {0: [p0_mb0, ...], ...}; sbs = [sb0, ...]
        repeats = 16
        for pod_index, mb_names in self.mbs.items():
            for mb_index, mb_name in enumerate(mb_names):
                for repeat_index in range(repeats):
                    sb_name = self.sbs[repeat_index]
                    self.edges.append([mb_name, sb_name, self.mb_sb_bandwidth])
        # these two asserts are equivalent
        # an SB accepts connection from every MB; thus total edges in mb-to-sb layer is
        # num_of_sbs (self.nspine_blocks) * num_of_abs (self.pods) * self.num_of_mbs
        assert(len(self.edges) - prev_edges_len == self.nspine_blocks * self.npods * self.num_of_mbs)
        # each MB connects to 16 SBs; thus (self.npods * self.num_of_mbs * repeats)
        assert (len(self.edges) - prev_edges_len == self.npods * self.num_of_mbs * repeats)

        self.generate_locality()
        return self.servers, self.edges, self.locality

    def four_racks(self):
        """ Generate Jupiter that has four rack within an aggregation block. """
        self.num_of_mbs = 1
        self.generate_servers()
        self.generate_tor_switches()
        self.wire_tor_switches()
        self.generate_mb_switches()
        prev_edges_len = len(self.edges)

        # each TOR-to-MB link is 1x640G in 4 rack topology
        self.tor_mb_bandwidth = 640

        # connect TOR switches to the MB switch in all-to-all fashion
        for mb_names in self.mbs.values():
            for mb_name in mb_names:
                for tor_names in self.tors.values():
                    for tor_name in tor_names:
                        self.edges.append([tor_name, mb_name, self.tor_mb_bandwidth])
        assert(len(self.edges) - prev_edges_len == self.nracks)

        self.generate_locality()
        return self.servers, self.edges, self.locality


if __name__ == "__main__":
    """ Generate Google Jupiter datacenter topology based on SIGCOMM'15.
    https://conferences.sigcomm.org/sigcomm/2015/pdf/papers/p183.pdf
    See Sec. 3.5 Jupiter: A 40G Datacenter-scale Fabric """
    CLI = argparse.ArgumentParser(
        description='Select datacenter topology size to generate')

    group = CLI.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '-f',
        '--full',
        action='store_true',
        default=False,
        help='Generate full Jupiter topology with 64 pods (aggregation blocks)')

    group.add_argument(
        '-fp',
        '--four-pods',
        action='store_true',
        default=False,
        help='Generate four pod Jupiter topology with 4*1536=6144 servers')

    group.add_argument(
        '-fr',
        '--four-racks',
        action='store_true',
        default=False,
        help='Generate four rack datacenter with 4*48=192 servers')

    CLI.add_argument(
        '-o',
        '--output',
        default='jupiter-dell_spec.pn.json',
        help='JSON file to output the datacenter topology')

    edges = None
    ARGS = CLI.parse_args()
    if ARGS.full:
        npods, nracks, nspine_blocks = 64, 32, 256
        jupiter = Jupiter(npods, nracks, nspine_blocks)
        servers, edges, locality = jupiter.full_topology()
    elif ARGS.four_pods:
        npods, nracks, nspine_blocks = 4, 32, 16
        jupiter = Jupiter(npods, nracks, nspine_blocks)
        servers, edges, locality = jupiter.four_pods()
    elif ARGS.four_racks:
        npods, nracks, nspine_blocks = 1, 4, 0
        jupiter = Jupiter(npods, nracks, nspine_blocks)
        servers, edges, locality = jupiter.four_racks()

    pn_filename = f'jupiter-dell_spec-{npods}pod-{len(servers)}servers.pn.json'

    pn_comment = """ *PN* describes physical topology. It has following format:
    [source, destination, bandwidth_in_gbps]. For example, [s0, tor0, 20] means
    server0 is connected to tor0 with 20 Gbps link. """

    server_comment = """ *Servers* describes server capacity. It has following
    format: [cpu, mem]. For example, [32, 128] means the server has 32 cores and
    128 memory. """

    locality_comment = """ *Locality* describes server locality,
        how servers are relatively local to each other. For example, 
        {pod0: {rack0: [p0_r0_s0, p0_r0_s1]}} means that
        p0_r0_s0 and p0_r0_s1 servers are local to each other.
        We can use this information to achieve VDC allocation with rack-locality and 
        pod-locality, i.e., all VMs of the VDC allocated within a single rack.
        Note that a single rack can have multiple top-of-rack switches.
        We just enclose all servers within these racks in a single rack because
        locality information is about server locality (not switch). """

    comments = pn_comment + server_comment + locality_comment
    comments = " ".join(comments.split())

    datacenter = {'PN': edges, 'Servers': servers, 'Locality': locality, 'Comments': comments}
    # glog.info(f'datacenter = {datacenter}')

    # write result to the file
    out_file = open(pn_filename, 'w')
    out_file.write(json.dumps(datacenter, indent=4))
    out_file.close()
    glog.info(f'successfully wrote datacenter topology to {pn_filename}')
