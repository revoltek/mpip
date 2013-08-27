""" Module run_bbs

Define task run_bbs
"""
from ..task import Task
from .. import mylogger

class Run_bbs(Task):

    __name__ = 'run_bbs'
    __doc__ = 'doc for run_bbs'
    banner = 'RUN_BBS: run many istances of BBS'
    arg_list = ['bbs_parset','skymodel','sb','group','node','obsdir','msname','bbsopt']

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
        command = 'calibrate-stand-alone ' + self.session.opts.get_opt('bbsopt') + ' ' + datafile + ' ' + \
              self.session.opts.get_opt('bbs_parset') + ' ' + self.session.opts.get_opt('skymodel') + ' >> run_bbs.log'
        return self.session.run_dist_com_all('run_bbs', wdir, command, \
              self.session.opts.get_opt('sb'), self.session.opts.get_opt('node'), self.session.opts.get_opt('group'))
