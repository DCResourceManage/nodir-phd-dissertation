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

""" Visualize and verify datacenter topologies. """

import argparse
import glog
import sys
import networkx as nx
import matplotlib
import re
# keep the next line to avoid PyCharm complaining about missing `_tkinter` module
matplotlib.use('pdf')
import matplotlib.pyplot as plt

import lib.constants as const
from lib.datacenter import Datacenter


def plot_datacenter(dc_top_fname: str):
    """ Load datacenter topology from JSON file to visualize.
    We use this to confirm sanity of connectivity and bandwidth values.
    :dc_top_fname: path to datacenter topology JSON file """
    datacenter = Datacenter()
    datacenter.load_by_json(dc_top_fname)
    layers_and_colors = {'server': [0, 'tab:orange'], 'tor': [1, 'tab:green'],
                         'mb': [2, 'tab:pink'], 'sb': [3, 'tab:blue']}

    for node_name in datacenter.conn.nodes:
        if re.match(const.SERVER_REGEX, node_name):
            datacenter.conn.nodes[node_name].update(
                layer=layers_and_colors['server'][0], color=layers_and_colors['server'][1])
        elif re.match(const.TOR_REGEX, node_name):
            datacenter.conn.nodes[node_name].update(
                layer=layers_and_colors['tor'][0], color=layers_and_colors['tor'][1])
        elif re.match(const.MB_REGEX, node_name):
            datacenter.conn.nodes[node_name].update(
                layer=layers_and_colors['mb'][0], color=layers_and_colors['mb'][1])
        elif re.match(const.SB_REGEX, node_name):
            datacenter.conn.nodes[node_name].update(
                layer=layers_and_colors['sb'][0], color=layers_and_colors['sb'][1])
        else:
            glog.error(f'ERROR: invalid node name: {node_name}. Exit.')
            sys.exit(1)

    color_dict = nx.get_node_attributes(datacenter.conn, 'color')
    pos = nx.multipartite_layout(datacenter.conn, subset_key='layer', align='vertical')
    # plt.figure(figsize=(5, 10)) # four racks
    plt.figure(figsize=(50, 100)) # four pods
    # plt.figure(figsize=(100, 200)) # 64 pods

    # # draw nodes and edges with strongly visible colors
    # nx.draw(datacenter.conn, pos, node_size=1, node_color=color_dict.values(),
    #         width=0.1,
    #         alpha=0.1,
    #         with_labels=False)

    # draw nodes with dimmed color but labels are clearly visible
    # nx.draw(datacenter.conn, pos, node_size=1, node_color=color_dict.values(),
    #         width=0.1,
    #         alpha=0.1,
    #         with_labels=False)
    # nx.draw_networkx_labels(datacenter.conn, pos,
    #         alpha=1,
    #         font_size=1)

    # draw with edge weights
    labels = nx.get_edge_attributes(datacenter.conn, 'weight')
    nx.draw(datacenter.conn, pos, node_size=1, node_color=color_dict.values(),
            width=0.1,
            alpha=0.1,
            font_size=1,
            with_labels=True)
    nx.draw_networkx_edge_labels(datacenter.conn, pos, edge_labels=labels,
            font_size=1,
            alpha=0.5,
            rotate=True)
    plot_fname = 'networkx.pdf'
    plt.savefig(plot_fname, bbox_inches='tight', dpi=300)
    glog.info('plotting complete, see {}'.format(plot_fname))


if __name__ == "__main__":
    CLI = argparse.ArgumentParser(description='Verify generated datacenter topologies')

    CLI.add_argument(
        '-i',
        '--input',
        default="datacenter.json",
        help='Path to physical datacenter.json file')

    opt_group = CLI.add_mutually_exclusive_group(required=True)
    opt_group.add_argument(
        '-dc',
        '--datacenter',
        action='store_true',
        default=False,
        help='Plot datacenter topology by loading it to networkx from datacenter.json')

    ARGS = CLI.parse_args()

    if ARGS.datacenter:
        plot_datacenter(ARGS.input)
    else:
        glog.error('Choose a valid option. Exiting.')
        sys.exit(1)
