""" Module run_ndppp

Define task run_ndppp
"""
from ..task import Task
from .. import mylogger

class Run_ndppp(Task):

    __name__ = 'run_ndppp'
    __doc__ = 'doc for run_ndppp'
    banner = 'RUN_ndppp: run many istances of ndppp'
    arg_list = ['ndppp_parset','sb','group','node','obsdir','msname']

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
        datafile = self.session.opts.get_opt('msname')
        command = 'NDPPP ' + self.session.opts.get_opt('ndppp_parset') + ' >> run_ndppp.log'
        return self.session.run_dist_com_all('run_ndppp', wdir, command, \
              self.session.opts.get_opt('sb'), self.session.opts.get_opt('node'), self.session.opts.get_opt('group'))
