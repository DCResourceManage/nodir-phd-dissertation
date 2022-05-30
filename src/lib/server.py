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
