# coding: utf-8
import pytest
import os
import re
import random
from click.testing import CliRunner

from ..helpers import (insert_running_jobs, insert_terminated_jobs)

from oar.lib import (db, config, Challenge)
from oar.lib.job_handling import get_job_types
from oar.cli.utils import CommandReturns
from oar.cli.oarsub import cli, connect_job

import oar.lib.tools  # for monkeypatching
import socket

default_res = '/resource_id=1'
nodes_res = 'resource_id'

fake_popen_process_stdout = ''
class FakeProcessStdout(object):
    def __init__(self):
        pass
    def decode(self):
        return fake_popen_process_stdout
 
class FakePopen(object):
    def __init__(self, cmd, stdout):
        pass
    def communicate(self):
        process_sdtout = FakeProcessStdout() 
        return [process_sdtout]

class Fake_getpwnam(object):
    def __init__(self, user):
        self.pw_shell = 'shell'

fake_run_return_code = 0       
class Fake_run(object):
    def __init__(self, cmd, shell):
        self.returncode = fake_run_return_code

class FakeCommandReturns():
    def __init__(self, cli):
        self.buffer = []
        self.exit_values = []
        self.final_exit = 0
        
    def print_(self, objs):
        self.buffer.append((0,objs,0))
        self.exit_values.append(0)
        
    def warning(self, objs, error=0, exit_value=0):
        self.buffer.append((2,objs,error))
        self.exit_values.append(exit_value)

    def error(self, objs, error=0, exit_value=0):
        self.buffer.append((1,objs,error))
        self.exit_values.append(exit_value)
        
    def exit(self, error):
        if error:
            self.exit_values.append(error)
            return error
        elif self.exit_values:
            return self.exit_values[-1]
        else:
            return 0

fake_connection_msg = b''        
class FakeConnection(object):
    def __init__(self):
        pass
    def recv(self, a):
        return fake_connection_msg

class FakeSocket(object):
    def __init__(self, a, b):
        pass
    def bind(self, a):
        pass
    def listen(self, a):
        pass
    def getsockname(self):
        return ('localhost', 101010)    
    def accept(self):
        return(FakeConnection(), 'yop')

@pytest.fixture(scope="module", autouse=True)
def set_env(request):
    os.environ['OARDIR'] = '/tmp'
    os.environ['OARDO_USER'] = 'yop'

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost")

        db['Queue'].create(name='default')
        yield

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'Popen', FakePopen)
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: True)
    monkeypatch.setattr(oar.lib.tools, 'getpwnam', Fake_getpwnam)
    monkeypatch.setattr(oar.lib.tools, 'run', Fake_run)
    monkeypatch.setattr(oar.lib.tools, 'signal_oarexec', lambda r,x,y,w,z: None)
    monkeypatch.setattr(socket, 'socket', FakeSocket)
#    monkeypatch.setattr(socket, 'getfqdn', lambda: True)
    
def test_oarsub_void():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exception.code == \
        (5, 'Command or interactive flag or advance reservation time or connection directive must be provided')

def test_oarsub_version():
    runner = CliRunner()
    result = runner.invoke(cli, ['-V'])
    print(result.output)
    assert re.match(r'.*\d\.\d\.\d.*', result.output)
    
def test_oarsub_sleep_1(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '"sleep 1"'])
    print(result.output)
    # job = db['Job'].query.one()
    mld_job_desc = db['MoldableJobDescription'].query.one()
    job_res_desc = db['JobResourceDescription'].query.one()
    print(mld_job_desc.walltime, job_res_desc.resource_type, job_res_desc.value)
    assert result.exit_code == 0
    assert mld_job_desc.walltime == config['DEFAULT_JOB_WALLTIME']
    assert job_res_desc.resource_type == 'resource_id'
    assert job_res_desc.value == 1


def test_oarsub_sleep_2(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-l resource_id=3', '-q default', '"sleep 1"'])
    print(result.output)
    # job = db['Job'].query.one()
    mld_job_desc = db['MoldableJobDescription'].query.one()
    job_res_desc = db['JobResourceDescription'].query.one()
    print(mld_job_desc.walltime, job_res_desc.resource_type, job_res_desc.value)
    assert result.exit_code == 0
    assert mld_job_desc.walltime == config['DEFAULT_JOB_WALLTIME']
    assert job_res_desc.resource_type == 'resource_id'
    assert job_res_desc.value == 3


def test_oarsub_admission_name_1(monkeypatch):

    db['AdmissionRule'].create(rule="name='yop'")
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    print("name: ", job.name)
    assert result.exit_code == 0
    assert job.name == 'yop'

def test_oarsub_parameters(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '--project', 'batcave',
                                 '--name', 'yop',
                                 '--notify', "mail:name\@domain.com", '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    print("project: ", job.project)
    assert result.exit_code == 0
    assert job.project == 'batcave'
    assert job.name == 'yop'
    assert job.notify == 'mail:name\@domain.com'

def test_oarsub_directory(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-d', '/home/robin/batcave', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    print("directory: ", job.launching_directory)
    assert result.exit_code == 0
    assert job.launching_directory == '/home/robin/batcave'
    
def test_oarsub_stdout_stderr(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-O', 'foo_%jobid%o', '-E', 'foo_%jobid%e', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    assert result.exit_code == 0
    assert job.stdout_file == 'foo_%jobid%o'
    assert job.stderr_file == 'foo_%jobid%e'

def test_oarsub_admission_queue_1(monkeypatch):

    db['AdmissionRule'].create(rule=("if user == 'yop':"
                                     "    queue= 'default'"))

    runner = CliRunner()
    result = runner.invoke(cli, ['-q noexist', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    print("queue-name: ", job.queue_name)
    assert result.exit_code == 0
    assert job.queue_name == 'default'


def test_oarsub_sleep_not_enough_resources_1(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-l resource_id=10', '"sleep 1"'])
    print(result.output)
    assert result.exception.code == (-5, 'There are not enough resources for your request')


def test_oarsub_sleep_property_error(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q default', '-l resource_id=4', '-p yopyop SELECT', '"sleep 1"'])
    print(result.output)
    assert result.exception.code[0] == -5


def test_oarsub_sleep_queue_error(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q queue_doesnot_exist', '"sleep 1"'])
    print(result.output)
    assert result.exception.code == (-8, 'queue queue_doesnot_exist does not exist')

def test_oarsub_interactive_reservation_error():
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', '-r', '2018-02-06 14:48:0'])
    print(result.output)
    assert result.exception.code == (7, 'An advance reservation cannot be interactive.')

def test_oarsub_interactive_desktop_computing_error():
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', '-t desktop_computing'])
    print(result.output)
    assert result.exception.code == (17, 'A desktop computing job cannot be interactive')

def test_oarsub_interactive_noop_error():
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', '-t noop'])
    print(result.output)
    assert result.exception.code == (17, 'a NOOP job cannot be interactive.')

def test_oarsub_connect_noop_error():
    runner = CliRunner()
    result = runner.invoke(cli, ['-C 1234', '-t noop'])
    print(result.output)
    assert result.exception.code == (17, 'A NOOP job does not have a shell to connect to.')  

def test_oarsub_scanscript_1():
    global fake_popen_process_stdout
    fake_popen_process_stdout = ("#Funky job\n"
                                 "#OAR -l resource_id=4,walltime=1\n"
                                 "#OAR -n funky\n"
                                 "#OAR --project batcave\n")

    runner = CliRunner()
    result = runner.invoke(cli, ['-S', 'yop'])
    print(result.output)
    job = db['Job'].query.one()
    print(job.initial_request)
    assert job.name == 'funky'
    assert job.project == 'batcave'

def test_oarsub_multiple_types(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-q', 'default', '-t', 't1', '-t', 't2', '"sleep 1"'])
    print(result.output)
    job = db['Job'].query.one()
    job_types = get_job_types(job.id)
    print(job_types)
    assert job_types == {'t1': True, 't2': True}
    assert result.exit_code == 0
    
def test_oarsub_connect_job_function(monkeypatch):
    config.setdefault_config(oar.lib.tools.DEFAULT_CONFIG)
    os.environ['OARDO_USER'] = 'oar'
    os.environ['DISPLAY'] = ''
    job_id = insert_running_jobs(1)[0]
    cmd_ret = FakeCommandReturns(None)
    exit_value = connect_job(job_id, 0, 'openssh_cmd', cmd_ret)
    print(cmd_ret.buffer)
    assert cmd_ret.exit_values == []

def test_oarsub_connect_job_function_bad_user(monkeypatch):
    config.setdefault_config(oar.lib.tools.DEFAULT_CONFIG)
    os.environ['OARDO_USER'] = 'yop'
    os.environ['DISPLAY'] = ''
    job_id = insert_running_jobs(1)[0]
    cmd_ret = FakeCommandReturns(None)
    exit_value = connect_job(job_id, 0, 'openssh_cmd', cmd_ret)
    print(cmd_ret.buffer)
    print(cmd_ret.exit_values[-1])
    assert cmd_ret.exit_values[-1] == 20
    
def test_oarsub_connect_job_function_noop(monkeypatch):
    config.setdefault_config(oar.lib.tools.DEFAULT_CONFIG)
    os.environ['OARDO_USER'] = 'oar'
    os.environ['DISPLAY'] = ''
    job_id = insert_running_jobs(1, types=['noop'])[0]
    cmd_ret = FakeCommandReturns(None)
    exit_value = connect_job(job_id, 0, 'openssh_cmd', cmd_ret)
    print(cmd_ret.buffer)
    print(cmd_ret.exit_values[-1])
    assert cmd_ret.exit_values[-1] == 17

def test_oarsub_connect_job_function_cosystem(monkeypatch):
    config.setdefault_config(oar.lib.tools.DEFAULT_CONFIG)
    os.environ['OARDO_USER'] = 'oar'
    os.environ['DISPLAY'] = ''
    job_id = insert_running_jobs(1, types=['cosystem'])[0]
    cmd_ret = FakeCommandReturns(None)
    exit_value = connect_job(job_id, 0, 'openssh_cmd', cmd_ret)
    print(cmd_ret.buffer)
    assert cmd_ret.exit_values == []
    
def test_oarsub_connect_job_function_deploy(monkeypatch):
    config.setdefault_config(oar.lib.tools.DEFAULT_CONFIG)
    os.environ['OARDO_USER'] = 'oar'
    os.environ['DISPLAY'] = ''
    job_id = insert_running_jobs(1, types=['deploy'])[0]
    cmd_ret = FakeCommandReturns(None)
    exit_value = connect_job(job_id, 0, 'openssh_cmd', cmd_ret)
    print(cmd_ret.buffer)
    assert cmd_ret.exit_values == []
    
@pytest.mark.parametrize("return_code, exit_values", [ (2, [0,2]), (10, [0,10])])
def test_oarsub_connect_job_function_returncode(return_code, exit_values, monkeypatch):
    global fake_run_return_code
    fake_run_return_code = return_code << 8
    config.setdefault_config(oar.lib.tools.DEFAULT_CONFIG)
    os.environ['OARDO_USER'] = 'oar'
    os.environ['DISPLAY'] = ''
    job_id = insert_running_jobs(1)[0]
    cmd_ret = FakeCommandReturns(None)
    exit_value = connect_job(job_id, 0, 'openssh_cmd', cmd_ret)
    print(cmd_ret.buffer)
    assert cmd_ret.exit_values == exit_values

def test_oarsub_resubmit_bad_user(monkeypatch):
    os.environ['OARDO_USER'] = 'iznogoud'
    job_id = insert_terminated_jobs(False, 1)[0]
    print(job_id)
    runner = CliRunner()
    result = runner.invoke(cli, ['--resubmit', str(job_id)])
    print(result.output)
    assert result.exception.code == (-3, 'Resubmitted job user mismatch.')
    # job = db['Job'].query.one()

def test_oarsub_resubmit(monkeypatch):
    os.environ['OARDO_USER'] = 'zozo'
    job_id = insert_terminated_jobs(False, 1)[0]
    # Insert challenge and ssh_keys

    ins = Challenge.__table__.insert().values(
        {'job_id': job_id, 'challenge': random.randint(1, 100000)})
    db.session.execute(ins)
    print(job_id)
    runner = CliRunner()
    result = runner.invoke(cli, ['--resubmit', str(job_id)])
    print(result.output.split('\n')[2])
    assert re.match(r'.*OAR_JOB_ID=.*', result.output.split('\n')[2])
    
def test_oarsub_parameters_file_error(monkeypatch):
    # TODO not the good error
    runner = CliRunner()
    result = runner.invoke(cli, ['--array-param-file', 'no_file', 'command'])
    print(result.output)
    assert result.exception.code == (6, 'An array of job must have a number of sub-jobs greater than 0.')

def test_oarsub_interactive_bad_job(monkeypatch):
    global fake_connection_msg
    fake_connection_msg = b'BAD JOB_'
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', 'will_be ignored'])
    print(result.output)
    assert result.exit_code == 11

def test_oarsub_interactive_array_param_file(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', '--array-param-file', 'no_file'])
    print(result.output)
    assert result.exit_code == 9
    
def test_oarsub_interactive_array(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['-I', '--array', '2'])
    print(result.output)
    assert result.exit_code == 8


def test_oarsub_reservation_rejected(monkeypatch):
    runner = CliRunner()
    result = runner.invoke(cli, ['--reservation', '1970-01-01 01:20:00'])
    print(result.output)
    print(result.output.split('\n')[4])
    assert re.match(r'.*REJECTED.*', result.output.split('\n')[4])
    assert result.exit_code == 0
    
def test_oarsub_reservation_granted(monkeypatch):
    global fake_connection_msg
    fake_connection_msg = b'GOOD RESERVATION_'
    runner = CliRunner()
    result = runner.invoke(cli, ['--reservation', '1970-01-01 01:20:00'])
    print(result.output)
    print(result.output.split('\n')[3])
    assert re.match(r'.*GRANTED.*', result.output.split('\n')[3])
    assert result.exit_code == 0
