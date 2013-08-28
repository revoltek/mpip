""" Module run_awimager

Define task run_awimager
"""
from ..task import Task
from .. import mylogger

class Run_awimager(Task):

    __name__ = 'run_awimager'
    __doc__ = 'doc for run_awimager'
    banner = 'RUN_AWIMAGER: run many istances of awimager'
    arg_list = ['sb','group','obsdir','node','msname','image','wprojplanes','weight','robust','npix','cellsize','datacol','niter','stokes','cyclefactor','gain','applyelement','timewindow','operation']

    def __init__(self, session):
        Task.__init__(self, session, self.__name__)
        #self.mylog = mylogger.logging.getLogger('mpip.run_awimager')

    def run(self):
        if self.session._cluster_status == None:
            self.mylog.error("Not connected to a cluster.")
            return False
        if self.session.opts.get_opt('group'):
            wdir = self.session.opts.get_opt('obsdir')+'/'+self.session._obs_name+'/group$SB/'
        else:
            wdir = self.session.opts.get_opt('obsdir')+'/'+self.session._obs_name+'/SB$SB/'
        datafile = self.session.opts.get_opt('msname')
        image = self.session.opts.get_opt('image')
        wprojplanes = self.session.opts.get_opt('wprojplanes')
        weight = self.session.opts.get_opt('weight')
        robust = self.session.opts.get_opt('robust')
        npix = self.session.opts.get_opt('npix')
        cellsize = self.session.opts.get_opt('cellsize')
        datacol = self.session.opts.get_opt('datacol')
        niter = self.session.opts.get_opt('niter')
        stokes = self.session.opts.get_opt('stokes')
        cyclefactor = self.session.opts.get_opt('cyclefactor')
        gain = self.session.opts.get_opt('gain')
        applyelement = self.session.opts.get_opt('applyelement')
        timewindow = self.session.opts.get_opt('timewindow')
        operation = self.session.opts.get_opt('operation')
        command = 'awimger ms=' + datafile + ' image=' + image + ' wprojplanes=' + wprojplanes +\
           ' weight=' + weight + ' robust=' + robust + ' npix=' + npix + \
           ' cellsize=' + cellsize + ' datacol=' + datacol + ' niter=' + niter + \
           ' stokes=' + stokes + ' cyclefactor=' + cyclefactor + ' gain=' + gain +\
           ' applyelement=' + applyelement + ' timewindow=' + timewindow + \
           ' operation=' + operation
        return self.session.run_dist_com_all('run_awimager', wdir, command, \
              self.session.opts.get_opt('sb'), self.session.opts.get_opt('node'), self.session.opts.get_opt('group'))
