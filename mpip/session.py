"""Module command.

The Com class is a base class for all m-pip commands
"""

from opts import *
import mylogger
from IPython.parallel import require
import numpy as np
from IPython.zmq.serialize import unpack_apply_message

class Session(object):
    """Primary data container for mpip.


    """
    mylogger.init_logger("mpip.log")
    mylog = mylogger.logging.getLogger("mpip.session")

    opts   = Instance(Opts, doc="User options")
    basedir = String('DUMMY', doc="Base directory for output files")
    _is_interactive_shell = Bool(False, doc="MPIP is being used in the interactive shell")
    _current_cmd = None
    _cluster_status = None
    _cluster_client = None
    _obs_name = None
    _ipshell = None

    def __init__(self, opts):
        self.opts = Opts(opts)
        self.extraparams = {}

    def set_status(self, status, obs_name=None):
        """Set cluster status and possibly obs_name and prompt"""
        self._cluster_status = status
        self._obs_name = obs_name
        # change prompt
        if obs_name == None:
            self._ipshell.prompt_manager.in_template = "MPIP [\#]: "
        else:
            self._ipshell.prompt_manager.in_template = "MPIP (" + obs_name + ") [\#]: "

    def get_status(self):
        """Get cluster status"""
        return self._cluster_status

    def set_client(self):
        """Set current cluster client"""
        from IPython.parallel import Client
        try:
            self._cluster_client = Client(profile='ssh')
            mylogger.userinfo(self.mylog, "Connected to a cluster.")
            return True
        except:
            return False

    def get_client(self):
        """Get current cluster client"""
        return self._cluster_client

    def set_current_cmd(self, current_cmd):
        """Set session current command"""
        self._current_cmd = current_cmd

    def get_current_cmd(self):
        """Get session current command"""
        return self._current_cmd

    def list_pars(self):
        """List parameter values."""
        import interface
        interface.list_pars(self)

    def getSBids(self, SB = ''):
        """Return a list of al msg_id of jobs that are running on
        a given SB
        """
        if SB == '': return []
        SBids = []
        query = self.get_client().db_query({'completed': None},['buffers','msg_id'])
        for q in query:
            # unpack the buffer of the job to obtain the SB number
            null, com, args = unpack_apply_message(q['buffers'])
            if args['SB'] == SB:
                SBids.append(q['msg_id'])

        return SBids


    def run_dist_com_all(self, task, wdir, command, SBs=[], nodes='', group=False):
        """Run a run_dist_com on all nodes and all SBs unless specified a restriction.
        If no restriction are specified, run the command for each node and each SB.
        """
        s = self.get_status()
        if s == None:
            self.mylog.error("Not connected to a cluster.")
            return False
        for node in s:
            # shall we proceed with this node?
            if (node in nodes or nodes == ''):
                if group:
                    nodeSBs = s[node]['group']
                else:
                    nodeSBs = s[node]['sb']
                for SB in nodeSBs:
                    # shall we proceed with this SB?
                    if (int(SB) in SBs or SBs == []):
                        rcommand = command.replace('$SB', SB)
                        rwdir = wdir.replace('$SB', SB)
                        # for groups rename the variable
                        if group: SB = 'g'+SB
                        self.run_dist_com(task, rwdir, rcommand, SB, node)


    def run_dist_com(self, task, wdir, command, SB='', node=''):
        """ Run a distributed command using information on the _cluster_status.
        The node parameter is required, the SB not (used only for job identification
        and to schedule a job after already submitted jobs on that SB are done).
        """
        s = self.get_status()
        if s == None:
            self.mylog.error("Not connected to a cluster.")
            return False
        if node == '':
            self.mylog.error("Node parameter required to run a command.")
            return False
        # define the function called by engines
        @require('subprocess, os')
        def f(c, node='', SB='', task='', wdir=''):
            import os, subprocess
            if wdir != '' and os.path.isdir(wdir): os.chdir(wdir)
            s = subprocess.Popen(c, shell=True,\
               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            s.wait()
            (out, err) = s.communicate()
            return {'node':node, 'SB':SB, 'task':task, 'command':c, \
                        'out':out, 'err':err}

        mylogger.userinfo(self.mylog,"Launching on node: "+node+" (SB: "+SB+", wdir:"+wdir+")\n"+command)
        try:
            scheduler = self.get_client().load_balanced_view(s[node]['e'])
            # Exec a job on a SB only if previous jobs on that
            # SB are finished and finished without errors
            after_ids = self.getSBids(SB)
            with scheduler.temp_flags(after=after_ids, retries=3):
                scheduler.apply_async(f, command, node=node, SB=SB, task=task, wdir=wdir)
        except Exception, e:
            self.mylog.exception("Cannot launch the command: " + command + \
                  "(Task: " + task + " - SB: " + SB + " - Node: " + node + ")")
        return True
