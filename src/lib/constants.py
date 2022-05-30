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

