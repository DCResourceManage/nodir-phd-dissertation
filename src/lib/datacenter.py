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

""" Datacenter object used for VM scheduling. """

import sys
import glog
import json
import networkx as nx
import uuid as uuid_lib

from typing import Any

import lib.constants as const
from lib.server import Server

class Datacenter:
    def __init__(self):
        self.conn = nx.Graph()
        self.conn_orig = nx.Graph()
        self.servers = {} # {server_name: server_object, ...}
        self.servers_orig = {} # {server_name: server_object, ...}

        # type: List[str]; ex: [pod1, pod2, ..., podN]
        self.pods = []

        # type: Dict[str, List[Server]]; ex: {rack1: [server1, server2, ...], ...}
        self.rack2servers = {}

        # type: Dict[str, List[Server]]; ex: {pod1: [server1, server2, ...], ...}
        self.pod2servers = {}

        # type: Dict[str, Dict[str, str]]; ex: {server1: {rack: rack1, pod: pod1}, ...}
        self.server_location = {}

    def load_by_json(self, json_fname: str) -> None:
        """ Loads datacenter topology into Datacenter.
        :json_fname: name of the file containing datacenter description """
        with open(json_fname) as ff:
            data = json.load(ff)
        assert(data)

        # parse servers
        for server_id, props in data['Servers'].items():
            self.servers[server_id] = Server(
                id=server_id, cores=props[0], ram=props[1])
            self.servers_orig[server_id] = Server(
                id=server_id, cores=props[0], ram=props[1])

        # parse physical network (PN). Note that PN belongs to the highest level
        # entity which is 'Datacenter' in our case. It does not belong to Pods since
        # there are physical connections between pods too. In other words, the
        # entity covering the PN should not have any outgoing connections.
        for items in data['PN']:
            src, dst, bandwidth = items[0], items[1], items[2]
            assert(bandwidth > 0)
            assert(self.conn.has_edge(src, dst) is False)
            bandwidth_in_mbps = bandwidth * 1000  # 1Gbps=1000Mbps
            self.conn.add_weighted_edges_from([(src, dst, bandwidth_in_mbps)])
            self.conn_orig.add_weighted_edges_from([(src, dst, bandwidth_in_mbps)])

        # glog.debug('nodes: {}'.format(self.conn.nodes()))
        # glog.debug('edges: {}'.format(self.conn.edges(data=const.WEIGHT_STR)))

        # load the locality field
        locality = data['Locality']  # type: Dict[str, Dict]
        for pod_id, pod_props in locality.items():
            self.pods.append(pod_id)
            self.pod2servers[pod_id] = []
            for rack_id, servers in pod_props.items():
                self.rack2servers[rack_id] = []
                for server_id in servers:
                    self.rack2servers[rack_id].append(self.servers[server_id])
                    self.pod2servers[pod_id].append(self.servers[server_id])
                    self.server_location[server_id] = {const.RACK_STR: rack_id, const.POD_STR: pod_id}

