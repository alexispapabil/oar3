# coding: utf-8
import sys
import pwd
import time
import re
import os
import socket
from sqlalchemy import distinct
from oar.lib import (db, config, get_logger, Resource, AssignedResource)

import signal, psutil
from subprocess import (Popen, call, PIPE, check_output, CalledProcessError, TimeoutExpired)


# Constants
DEFAULT_CONFIG = {
    'LEON_SOFT_WALLTIME': 20,
    'LEON_WALLTIME': 300,
    'TIMEOUT_SSH': 120,
    'SERVER_PROLOGUE_EPILOGUE_TIMEOUT': 60,
    'SERVER_PROLOGUE_EXEC_FILE': None,
    
    'BIPBIP_OAREXEC_HASHTABLE_SEND_TIMEOUT': 30,
    'DEAD_SWITCH_TIME': 0,
    'OAREXEC_DIRECTORY': '/tmp/oar_runtime/',
    'OAREXEC_PID_FILE_NAME': 'pid_of_oarexec_for_jobId_',
    'OARSUB_FILE_NAME_PREFIX': 'oarsub_connections_',
    'PROLOGUE_EPILOGUE_TIMEOUT': 60,
    'SUSPEND_RESUME_SCRIPT_TIMEOUT': 60,
    'SSH_RENDEZ_VOUS': 'oarexec is initialized and ready to do the job',
    'OPENSSH_CMD': 'ssh',
    'CPUSET_FILE_MANAGER': '/etc/oar/job_resource_manager.pl',
    'MONITOR_FILE_SENSOR': '/etc/oar/oarmonitor_sensor.pl',
    'SUSPEND_RESUME_FILE_MANAGER': '/etc/oar/suspend_resume_manager.pl',
    'OAR_SSH_CONNECTION_TIMEOUT': 120,
    'OAR_SSH_AUTHORIZED_KEYS_FILE': '.ssh/authorized_keys',
    'NODE_FILE_DB_FIELD': 'network_address',
    'NODE_FILE_DB_FIELD_DISTINCT_VALUES': 'resource_id',
    'NOTIFY_TCP_SOCKET_ENABLED': 1,
    'SUSPECTED_HEALING_TIMEOUT': 10, 
    'SUSPECTED_HEALING_EXEC_FILE': None
    }

logger = get_logger("oar.lib.tools")

almighty_socket = None

notification_user_socket = None


def init_judas_notify_user():  # pragma: no cover

    logger.debug("init judas_notify_user (launch judas_notify_user.pl)")

    global notify_user_socket
    uds_name = "/tmp/judas_notify_user.sock"
    if not os.path.exists(uds_name):
        binary = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "judas_notify_user.pl")
        os.system("%s &" % binary)

        while(not os.path.exists(uds_name)):
            time.sleep(0.1)

        notification_user_socket = socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM)
        notification_user_socket.connect(uds_name)


def notify_user(job, state, msg):  # pragma: no cover
    global notification_user_socket
    # Currently it uses a unix domain sockey to communication to a perl script
    # TODO need to define and develop the next notification system
    # see OAR::Modules::Judas::notify_user

    logger.debug("notify_user uses the perl script: judas_notify_user.pl !!! ("
                 + state + ", " + msg + ")")

    # OAR::Modules::Judas::notify_user($base,notify,$addr,$user,$jid,$name,$state,$msg);
    # OAR::Modules::Judas::notify_user($dbh,$job->{notify},$addr,$job->{job_user},$job->{job_id},$job->{job_name},"SUSPENDED","Job
    # is suspended."
    addr, port = job.info_type.split(':')
    msg_uds = job.notify + "°" + addr + "°" + job.user + "°" + job.id + "°" +\
        job.name + "°" + state + "°" + msg + "\n"
    nb_sent = notification_user_socket.send(msg_uds.encode())

    if nb_sent == 0:
        logger.error("notify_user: socket error")


def create_almighty_socket():  # pragma: no cover
    global almighty_socket
    almighty_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server = config["SERVER_HOSTNAME"]
    port = config["SERVER_PORT"]
    try:
        almighty_socket.connect((server, port))
    except socket.error as exc:
        logger.error("Connection to Almighty" + server + ":" + str(port) +
                     " raised exception socket.error: " + str(exc))
        sys.exit(1)


# TODO: refactor to use zmq
def notify_almighty(message):  # pragma: no cover
    if not almighty_socket:
        create_almighty_socket()
    return almighty_socket.send(message.encode())


# TODO: refactor to use zmq
def notify_tcp_socket(addr, port, message):  # pragma: no cover
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    logger.debug('notify_tcp_socket:' + addr + ":" + port + ', msg:' + message)
    try:
        tcp_socket.connect((addr, int(port)))
    except socket.error as exc:
        logger.error("notify_tcp_socket: Connection to " + addr + ":" + port +
                     " raised exception socket.error: " + str(exc))
        return 0
    nb_sent = tcp_socket.send(message.encode())
    tcp_socket.close()
    return nb_sent


def test_hosts(nodes_to_check):
    raise NotImplementedError("TODO")
    return []

def send_log_by_email(title, message):
    raise NotImplementedError("TODO")


def exec_with_timeout(cmd, TIMEOUT_SSH):
    # Launch admin script
    error_msg = ''
    try:
        check_output(cmd, stderr=STDOUT, timeout=timeout)
    except CalledProcessError as e:
        error_msg = e.output + '. Return code: ' + str(e.return_code)
    except TimeoutExpired as e:
        error_msg = e.output

    return error_msg

def kill_child_processes(parent_pid, sig=signal.SIGTERM):
    """from: https://stackoverflow.com/questions/3332043/obtaining-pid-of-child-process"""
    try:
      parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
      return
    children = parent.children(recursive=True)
    for process in children:
      process.send_signal(sig)

    

def fork_and_feed_stdin(healing_exec_file, timeout, resources_to_heal):
    raise NotImplementedError("TODO")

def get_oar_pid_file_name(job_id):
    raise NotImplementedError("TODO")

def signal_oarexec(host, job_id, signal, wait, ssh_cmd, user_signal):
    raise NotImplementedError('TODO')
    return 0

def set_ssh_timeout(timeout):
    raise NotImplementedError('TODO')

def get_ssh_timeout():
    raise NotImplementedError('TODO')

def signal_oarexec(host, job_id, signal, wait, ssh_cmd, user_signal, timeout):
    raise NotImplementedError('TODO')
    return 0
## Send the given signal to the right oarexec process
## args : host name, job id, signal, wait or not (0 or 1), 
## DB ref (to close it in the child process), ssh cmd, user defined signal 
## for oardel -s (null by default if not used)
## return an array with exit values
#sub signal_oarexec($$$$$$$){
#    my $host = shift;
#    my $job_id = shift;
#    my $signal = shift;
#    my $wait = shift;
#    my $base = shift;
#    my $ssh_cmd = shift;
#    my $user_signal = shift;
#
#    my $file = get_oar_pid_file_name($job_id);
#    #my $cmd = "$ssh_cmd -x -T $host \"test -e $file && cat $file | xargs kill -s $signal\"";
#    #my $cmd = "$ssh_cmd -x -T $host bash -c \"test -e $file && PROC=\\\$(cat $file) && kill -s CONT \\\$PROC && kill -s $signal \\\$PROC\"";
#    my ($cmd_name,@cmd_opts) = split(" ",$ssh_cmd);
#    my @cmd;
#    my $c = 0;
#    $cmd[$c] = $cmd_name;$c++;
#    foreach my $p (@cmd_opts){
#        $cmd[$c] = $p;$c++;
#    }
#    $cmd[$c] = "-x";$c++;
#    $cmd[$c] = "-T";$c++;
#    $cmd[$c] = $host;$c++;
#    if (defined($user_signal) && $user_signal ne ''){
#        my $signal_file = OAR::Tools::get_oar_user_signal_file_name($job_id);
#	    $cmd[$c] = "bash -c 'echo $user_signal > $signal_file && test -e $file && PROC=\$(cat $file) && kill -s CONT \$PROC && kill -s $signal \$PROC'";$c++;
#    }
#    else {
#    	$cmd[$c] = "bash -c 'test -e $file && PROC=\$(cat $file) && kill -s CONT \$PROC && kill -s $signal \$PROC'";$c++;
#    }
#    $SIG{PIPE}  = 'IGNORE';
#    my $pid = fork();
#    if($pid == 0){
#        #CHILD
#        undef($base);
#        my $exit_code;
#        my $ssh_pid;
#        eval{
#            $SIG{PIPE}  = 'IGNORE';
#            $SIG{ALRM} = sub { die "alarm\n" };
#            alarm(get_ssh_timeout());
#            $ssh_pid = fork();
#            if ($ssh_pid == 0){
#                exec({$cmd_name} @cmd);
#                warn("[ERROR] Cannot find @cmd\n");
#                exit(-1);
#            }
#            my $wait_res = -1;
#            # Avaoid to be disrupted by a signal
#            while ((defined($ssh_pid)) and ($wait_res != $ssh_pid)){
#                $wait_res = waitpid($ssh_pid,0);
#            }
#            alarm(0);
#            $exit_code  = $?;
#        };
#        if ($@){
#            if ($@ eq "alarm\n"){
#                if (defined($ssh_pid)){
#                    my ($children,$cmd_name) = get_one_process_children($ssh_pid);
#                    kill(9,@{$children});
#                }
#            }
#        }
#        # Exit from child
#        exit($exit_code);
#    }
#    if ($wait > 0){
#        waitpid($pid,0);
#        my $exit_value  = $? >> 8;
#        my $signal_num  = $? & 127;
#        my $dumped_core = $? & 128;
#
#        return($exit_value,$signal_num,$dumped_core);
#    }else{
#        return(undef);
#    }
#}
#
#

# get_date
# returns the current time in the format used by the sql database

# TODO
def send_to_hulot(cmd, data):
    config.setdefault_config({"FIFO_HULOT": "/tmp/oar_hulot_pipe"})
    fifoname = config["FIFO_HULOT"]
    try:
        with open(fifoname, 'w') as fifo:
            fifo.write('HALT:%s\n' % data)
            fifo.flush()
    except IOError as e:
        e.strerror = 'Unable to communication with Hulot: %s (%s)' % fifoname % e.strerror
        logger.error(e.strerror)
        return 1
    return 0


def get_oar_pid_file_name(job_id):
    logger.error("get_oar_pid_file_name id not YET IMPLEMENTED")


def get_default_suspend_resume_file():
    logger.error("get_default_suspend_resume_file id not YET IMPLEMENTED")


def manage_remote_commands():
    logger.error("manage_remote_commands id not YET IMPLEMENTED")

def get_date():

    if db.engine.dialect.name == 'sqlite':
        req = "SELECT strftime('%s','now')"
    else:   # pragma: no cover
        req = "SELECT EXTRACT(EPOCH FROM current_timestamp)"

    result = db.session.execute(req).scalar()
    return int(result)


# sql_to_local
# converts a date specified in the format used by the sql database to an
# integer local time format
# parameters : date string
# return value : date integer
# side effects : /


def sql_to_local(date):
    # Date "year mon mday hour min sec"
    date = ' '.join(re.findall(r"[\d']+", date))
    t = time.strptime(date, "%Y %m %d %H %m %s")
    return int(time.mktime(t))


# local_to_sql
# converts a date specified in an integer local time format to the format used
# by the sql database
# parameters : date integer
# return value : date string
# side effects : /

def local_to_sql(local):
    return time.strftime("%F %T", time.localtime(local))

# sql_to_hms
# converts a date specified in the format used by the sql database to hours,
# minutes, secondes values
# parameters : date string
# return value : hours, minutes, secondes
# side effects : /


def sql_to_hms(t):
    hms = t.split(':')
    return (hms[0], hms[1], hms[2])

# hms_to_sql
# converts a date specified in hours, minutes, secondes values to the format
# used by the sql database
# parameters : hours, minutes, secondes
# return value : date string
# side effects : /


def hms_to_sql(hour, min, sec):

    return(str(hour) + ":" + str(min) + ":" + str(sec))
# hms_to_duration
# converts a date specified in hours, minutes, secondes values to a duration
# in seconds
# parameters : hours, minutes, secondes
# return value : duration
# side effects : /


def hms_to_duration(hour, min, sec):
    return int(hour) * 3600 + int(min) * 60 + int(sec)


# duration_to_hms
# converts a date specified as a duration in seconds to hours, minutes,
# secondes values
# parameters : duration
# return value : hours, minutes, secondes
# side effects : /


def duration_to_hms(t):

    sec = t % 60
    t /= 60
    min = t % 60
    hour = int(t / 60)

    return (hour, min, sec)

# duration_to_sql
# converts a date specified as a duration in seconds to the format used by the
# sql database
# parameters : duration
# return value : date string
# side effects : /


def duration_to_sql(t):

    hour, min, sec = duration_to_hms(t)

    return hms_to_sql(hour, min, sec)


# sql_to_duration
# converts a date specified in the format used by the sql database to a
# duration in seconds
# parameters : date string
# return value : duration
# side effects : /

def sql_to_duration(t):
    (hour, min, sec) = sql_to_hms(t)
    return hms_to_duration(hour, min, sec)

def send_checkpoint_signal(job):
    raise NotImplementedError("TODO")
    logger.debug("Send checkpoint signal to the job " + str(job.id))
    logger.warning("Send checkpoint signal NOT YET IMPLEMENTED ")
    # Have a look to  check_jobs_to_kill/oar_meta_sched.pl

def get_username(): # NOTUSED
    return pwd.getpwuid( os.getuid() ).pw_name


def format_ssh_pub_key(key, cpuset, user, job_user=None):
    """Add right environment variables to the given public key"""
    if not job_user:
        job_user = user
    if not cpuset:
        cpuset = 'undef'

    formated_key = 'environment="OAR_CPUSET=' + cpuset + '",environment="OAR_JOB_USER='\
                                 + job_user + '" ' + key + "\n"
    return formated_key


def get_private_ssh_key_file_name(cpuset_name):
    """Get the name of the file of the private ssh key for the given cpuset name"""
    return(config['OAREXEC_DIRECTORY'] + '/' + cpuset_name + '.jobkey')
