import logging
import os
import parsl
import pytest
import time

logger = logging.getLogger(__name__)


@parsl.python_app
def this_app():
    # this delay needs to be several times the resource monitoring
    # period configured in the test configuration, so that some
    # messages are actually sent - there is no guarantee that any
    # (non-first) resource message will be sent at all for a short app.
    time.sleep(20)

    return 5


@pytest.mark.local
def test_row_counts():
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from sqlalchemy import text
    from parsl.tests.configs.htex_local_kafka_monitoring import fresh_config

    logger.info("loading parsl")
    parsl.load(fresh_config())

    logger.info("invoking and waiting for result")
    assert this_app().result() == 5

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    # at this point, we should find one row in the monitoring database.

    logger.info("checking kafka content")

    logger.info("all done")
