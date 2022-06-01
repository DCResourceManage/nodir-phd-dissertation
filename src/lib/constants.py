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

""" Constants that are used across the entire project. """

from typing import Dict, List, Union, Tuple

POD_PREFIX = 'p'
RACK_PREFIX = 'r'
SERVER_PREFIX = 's'
JUPITER_CHIP_PREFIX = 'cc'
JUPITER_SOUTH_PREFIX = 's'
JUPITER_NORTH_PREFIX = 'n'
JUPITER_MB_PREFIX = 'mb'
JUPITER_SB_PREFIX = 'sb'

RACK_STR = 'rack'
POD_STR = 'pod'

# example: 'p0_r0_s0'
SERVER_REGEX = f'{POD_PREFIX}[0-9]+_{RACK_PREFIX}[0-9]+_{SERVER_PREFIX}[0-9]'
# example: 'p0_r0'
TOR_REGEX = f'{POD_PREFIX}[0-9]+_{RACK_PREFIX}[0-9]'
# example: 'p0_mb0'
MB_REGEX = f'{POD_PREFIX}[0-9]+_{JUPITER_MB_PREFIX}[0-9]'
# example: 'sb0'
SB_REGEX = f'{JUPITER_SB_PREFIX}[0-9]'

# example: ('p0_r0_s0', 'p0_r0')
SERVER2TOR_REGEX = f'\(\'{SERVER_REGEX}+\',\s\'{TOR_REGEX}+\'\)'
# example: ('p0_r0', 'p0_r0_s0')
SERVER2TOR_REGEX_FLIPPED = f'\(\'{TOR_REGEX}+\',\s\'{SERVER_REGEX}+\'\)'
# example: ('p0_r0', 'p0_mb0')
TOR2MB_REGEX = f'\(\'{TOR_REGEX}+\',\s\'{MB_REGEX}+\'\)'
# example: ('p0_mb0', 'p0_r0')
TOR2MB_REGEX_FLIPPED = f'\(\'{MB_REGEX}+\',\s\'{TOR_REGEX}+\'\)'
# example: ('p0_mb0', 'p0_sb0')
MB2SB_REGEX = f'\(\'{MB_REGEX}+\',\s\'{SB_REGEX}+\'\)'
# example: ('p0_sb0', 'p0_mb0')
MB2SB_REGEX_FLIPPED = f'\(\'{SB_REGEX}+\',\s\'{MB_REGEX}+\'\)'

VM_CREATE_STR = 'create'
VM_DELETE_STR = 'delete'

TYPE_STR = 'type'
VDC_UUID_STR = 'vdc_uuid'
VM_UUID_STR = 'vm_uuid'

CORES_STR = 'cores'
RAM_STR = 'ram_in_gb'

# Max VDC peak size: we make sure azure.csv VDCs to be up to this size
MAX_PEAK_VDC_SIZE = 30

TICK_STR = 'tick_'
BW_STR = 'net_conns_in_mbps'
WEIGHT_STR = 'weight'

TIME_DRIFT = 100
INVALID_TIMESTAMPS = [2556100, 2002300, 2001400, 2559700, 2393800, 2312200,
        2580700, 2001100, 2458900, 1728700, 2271700, 2278900,
        # delete_event times below
        2557600, 2005900, 2004400, 2569000, 2395300, 2313100, 2581300, 2079100, 2591500]
VDC_CONCAT_STR = '__'

# example: 'net_conn_in_mbps': {dst_vm_uuid: 100, ...}
NET_CONN_TYPE = Dict[str, int]

# example: {'type': 'create', 'vdc_uuid': dep_id, 'vm_uuid': vm_id,
# 'cores': 4, 'ram_in_gb': 8, 'net_conns_in_mbps': {}}
EVENT_TYPE = Dict[str, Union[str, float, NET_CONN_TYPE]]

EVENT_LIST_TYPE = Dict[int, List[EVENT_TYPE]]

