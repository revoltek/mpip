""" Module run_casa

Define task run_casa
"""
from ..task import Task
from .. import mylogger

class Run_casa(Task):

    __name__ = 'run_casa'
    __doc__ = 'Default working dir is obsdir/obsname/SB$SB/'
    banner = 'RUN_CASA: run a CASA script'
    arg_list = ['sb', 'group', 'obsdir', 'node', 'casaargs', 'casascript']

    def __init__(self, session):
        Task.__init__(self, session, self.__name__)

    def run(self):
        if self.session._cluster_status == None:
            self.mylog.error("Not connected to a cluster.")
            return False
        if self.session.opts.get_opt('group'):
            wdir = self.session.opts.get_opt('obsdir')+'/'+self.session._obs_name+'/group$SB/'
        else:
            wdir = self.session.opts.get_opt('obsdir')+'/'+self.session._obs_name+'/SB$SB/'
        # CASA must be run trough the xvfb-casapy.pl script (modified from NRAO pipeline)
        # which solves the problems with X using Xvfb properly
        casacommand = 'xvfb-casapy.pl -f '+self.session.opts.get_opt('casascript')+\
                ' -args \''+self.session.opts.get_opt('casaargs')+'\''
        return self.session.run_dist_com_all('run_casa', wdir, casacommand, \
              self.session.opts.get_opt('sb'), self.session.opts.get_opt('node'), self.session.opts.get_opt('group'))
        
        # CASA script should start with:
        #import sys
        #args = sys.argv[6:] # remove CASA-related args
        #firstarg = args[0]

