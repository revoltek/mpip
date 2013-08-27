""" Module run_cmd

Define task run_cmd
"""
from ..task import Task
from .. import mylogger

class Run_cmd(Task):

    __name__ = 'run_cmd'
    __doc__ = 'Default working dir is obsdir/obsname/SB$SB/'
    banner = 'RUN_CMD: run a generic command'
    arg_list = ['sb', 'group', 'command', 'obsdir', 'node']

    def __init__(self, session):
        Task.__init__(self, session, self.__name__)
#       self.mylog = mylogger.logging.getLogger('mpip.run_cmd')

    def run(self):
        if self.session._cluster_status == None:
            self.mylog.error("Not connected to a cluster.")
            return False
        if self.session.opts.get_opt('group'):
            wdir = self.session.opts.get_opt('obsdir')+'/'+self.session._obs_name+'/group$SB/'
        else:
            wdir = self.session.opts.get_opt('obsdir')+'/'+self.session._obs_name+'/SB$SB/'
        command = self.session.opts.get_opt('command')
        return self.session.run_dist_com_all('run_cmd', wdir, command, \
              self.session.opts.get_opt('sb'), self.session.opts.get_opt('node'), self.session.opts.get_opt('group'))
