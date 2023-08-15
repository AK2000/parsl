import logging
import os
import pytest
import time

from parsl.monitoring.energy.node_monitors import *

logger = logging.getLogger(__name__)


def test_rapl_monitor():
    monitor = RaplCPUNodeEnergyMonitor(debug=True)
    result = monitor.report()
    assert result

    new_result = monitor.report()
    assert new_result.start_time == result.end_time
    assert new_result.total_energy > 0
    
    

if __name__ == "__main__":
    test_energy_collection()