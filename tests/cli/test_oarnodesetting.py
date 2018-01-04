# coding: utf-8
import pytest

from click.testing import CliRunner

from oar.lib import db
from oar.cli.oarnodesetting import cli

NB_NODES=5
NB_LINES_PER_NODE=3 # network_address: localhost\n resource_id: 1\n state: Alive\n
@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(NB_NODES):
            db['Resource'].create(network_address="localhost")
        yield

def test_oarnodesetting_simple():
    runner = CliRunner()
    result = runner.invoke(cli)
    print(result)
    #nb_lines = len(result.output_bytes.decode().split('\n'))
    #assert nb_lines == NB_LINES_PER_NODE * NB_NODES + 1 # + 1 for last \n
    assert result.exit_code == 1

