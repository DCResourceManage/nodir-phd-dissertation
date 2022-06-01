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

""" Server object used for VM scheduling. """

import lib.constants as const

class Server(object):
    def __init__(self, id, cores, ram):
        self.id = id
        self.cores = cores
        self.ram = ram

        self._cores_remaining = cores
        self._ram_remaining = ram

    def has_cores_capacity(self, cores=0) -> bool:
        return self._cores_remaining - cores >= 0

    def has_ram_capacity(self, ram=0.0) -> bool:
        return self._ram_remaining - ram >= 0.0

    def allocate_cores(self, cores) -> int:
        """ Deduct cores from the server after VM allocation.
        :cores: number of cores being allocated
        :return: SCHED_SUCCESS if operation succeeds, fails otherwise. """
        assert(cores > 0)
        assert(self.has_cores_capacity(cores))
        self._cores_remaining -= cores
        return const.SCHED_SUCCESS

    def allocate_ram(self, ram) -> int:
        """ Deduct RAM from the server after VM allocation.
        :ram: amount of RAM being allocated (in MB)
        :return: SCHED_SUCCESS if operation succeeds, fails otherwise. """
        assert(ram > 0)
        assert(self.has_ram_capacity(ram))
        self._ram_remaining -= ram
        return const.SCHED_SUCCESS

    def free_cores(self, cores) -> int:
        """ Add cores back to the server after VM deallocation.
        :cores: number of cores being freed
        :return: SCHED_SUCCESS if operation succeeds, fails otherwise. """
        self._cores_remaining += cores
        assert(self._cores_remaining <= self.cores)
        return const.SCHED_SUCCESS

    def free_ram(self, ram) -> int:
        """ Add RAM back to the server after VM deallocation.
        :ram: amount of RAM being freed (in MB)
        :return: SCHED_SUCCESS if operation succeeds, SCHED_FAIL otherwise. """
        self._ram_remaining += ram
        assert(self._ram_remaining <= self.ram)
        return const.SCHED_SUCCESS
