from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field

from parsl.utils import RepresentationMixin

@dataclass
class Result:
    start_time: int
    end_time: int
    total_energy: int
    devices: dict[str, Result] = field(default_factory=dict)

    def __add__(self, other):
        start_time = min(self.start_time, other.start_time)
        end_time = max(self.end_time, other.end_time)
        return Result(start_time, end_time, self.total_energy + other.total_energy, dict(self.devices, **other.devices))

    def __sub__(self, other):
        start_time = other.end_time
        devices = dict()
        for name, device in self.deivces.items():
            devices[name] = device - other.devices[name]
        total_energy = self.total_energy - other.total_energy

        return Result(start_time, self.end_time, total_energy, devices)


class NodeEnergyMonitor(RepresentationMixin, metaclass=ABCMeta):
    """NodeEnergyMonitor provides a consistent interface to collect energy
    measurements from a variety of devices/interfaces from the worker node
    """
    def __init__(self, debug: bool = True):
        self.debug = debug

    @abstractmethod
    def report(self) -> dict:
        pass

def JobEnergyMonitor(RepresentationMixin, metaclass=ABCMeta):
    """JobEnergyMonitor provides a interface to access job monitoring information
    usually provided by the cluster scheduler (i.e. Slurm or PBS)"""

    def __init__(self, debug: bool = True):
        self.debug = debug

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def add(self, jobid: str) -> None: