""" Module clcmd

Define task clcmd which is used to fire up the engines and to shut them down.
It also define all the entries of the cluster_status saved in the session and
for retriving the cluster infos in the future.
"""

from ..task import Task
from .. import mylogger
from ..interface import print_cluster

import subprocess, time, socket, os
from IPython.parallel import require, interactive
import pickle

class Clcmd(Task):

    __name__ = 'clcmd'
    __doc__ = 'Define task clcmd which is used to fire up the engines, connect \
    to them and to shut them down.'
    banner = 'CLCOD: starts/stops/updates the engines for distributed tasks'
    arg_list = ['node','engpernode','ccmd','obsdir','obsname']

    def __init__(self, session):
        Task.__init__(self, session, self.__name__)
    #   self.mylog = mylogger.logging.getLogger('mpip.clcmd')

    def _update(self):
        """Update the list of available engines and detect in which node they are
        """
        status = {}
        c = self.session.get_client()
        for i in c.ids:
            engine = c[i]
            # get the hostname of each node
            node = engine.apply_sync(socket.gethostname)
            if node not in status:
                # define the function called by engines
                # to find free space
                @interactive
                def f():
                    import os
                    s = os.statvfs('/data')
                    return 100*(1-float(s.f_bfree)/float(s.f_blocks))
                space = engine.apply_sync(f)
                status[node]={'e':[], 'sb':[], 'group':[], 'df':space}

            status[node]['e'].append(i)

        if status == {}:
            self.session.set_status(None)
            self.mylog.warning("No working engines")
        else:
            self.session.set_status(status, self.session.opts.get_opt('obsname'))


    def _populate(self):
        """Populate the status dict with the SB numbers
        """
        status = self.session.get_status()
        for node in status:
            # define the function called by the engines
            # to find SBs
            @interactive
            def f(obsdir, obsname):
                import glob
                return (glob.glob(obsdir+'/'+obsname+'/SB*'), glob.glob(obsdir+'/'+obsname+'/group*'))

            # use the first engine of each node to retreive the SB info
            SBs, groups = self.session._cluster_client[status[node]['e'][0]].apply_sync(f,self.session.opts.get_opt('obsdir'),self.session.opts.get_opt('obsname'))
            for SB in SBs:
                SBnum = os.path.basename(SB).replace('SB','')
                # check that is a number otherwise ignore it
                try:
                    null = int(SBnum)
                    status[node]['sb'].append(SBnum)
                except:
                    self.mylog.warning("Found a SB sub-dir not in the form SB###. Ignoring " + SB + " on node "+node+".")
            for group in groups:
                groupname = os.path.basename(group).replace('group','')
                status[node]['group'].append(groupname)


    def _no_engine_running(self):
        """Return True if no engines are running.
        """
        query = self.session._cluster_client.db_query({'completed': None},['msg_id'])
        if query == []:
            return True
        else:
            return False


    def run(self): # TODO: add self.session.opts.node self.session.opts.engpernode

        if self.session.opts.get_opt('obsname') == '':
            self.mylog.error("Missing obsname.")
            return False
        if self.session.opts.get_opt('ccmd') == 'start':
            # connect to a client and save in session._cluster_client
            if self.session.set_client():
                # load _cluster_status for this obsname from saved file
                try:
                    home = os.path.expanduser("~")
                    pkl_file = open(home + '/.cluster_status.mpip', 'rb')
                    status = pickle.load(pkl_file)[self.session.opts.get_opt('obsname')]
                    pkl_file.close()
                    self.session.set_status(status, self.session.opts.get_opt('obsname'))
                    mylogger.userinfo(self.mylog, "Retreiving cluster status from '"\
                          + home + "/.cluster_status.mpip'.")
                # cannot load file, try to create and update it
                except:
                    if self._no_engine_running():
                        mylogger.userinfo(self.mylog, "Updating cluster status for unsaved obsname.")
                        self._update()
                        self._populate()
                        self.session.save_status()
                    # TODO: if some engine are running, force a new cluster to start,
                    # collect data and quit (find a better solution! -> use the get_result of the first time we run the populate?)
                    else:
                        self.mylog.error("Cannot create cluster status: some engines are busy.")
                        return False

            # no client, start a new cluster
            else:
                try:
                    # start a new cluster
                    subprocess.Popen(["ipcluster", "start", "--profile=ssh", "--daemon"])
                    # give time to ipcluster to start
                    timeout = 60
                    mylogger.userinfo(self.mylog, "Waiting for ipcontroller to come online...")
                    while True:
                        if not self.session.set_client():
                            pass
                        else:
                            time.sleep(10)
                            break
                        time.sleep(1)
                        timeout -= 1
                        if timeout == 0: raise Exception("Timeout.")
                    self._update()
                    self._populate()
                    self.session.save_status()
                    mylogger.userinfo(self.mylog, "If engines are missing, try ccmd='update'\n \
                       probably the cluster needs some time to start.")
                except Exception, e:
                    self.mylog.exception("Cannot properly start the cluster.")
                    return False

            print_cluster(self.session._cluster_status)
            return True

        elif self.session.opts.get_opt('ccmd') == 'stop':
            if self.session.get_status() == None:
                self.mylog.error("Not attached to any cluster.")
                return False
            try:
                # shut down the cluster
                self.session.get_client().shutdown(hub=True,block=True)
                self.session.set_status(None)
                return True
            except Exception, e:
                self.mylog.exception("Cannot stop the cluster.")
                return False

        elif self.session.opts.get_opt('ccmd') == 'update':
            if self.session.get_status() == None:
                self.mylog.error("Not attached to any cluster, try with ccmd='start'.")
                return False
            if self._no_engine_running():
                mylogger.userinfo(self.mylog, "Updating cluster status.")
                self._update()
                self._populate()
                self.session.save_status()
                print_cluster(self.session._cluster_status)
                return True
            else:
                self.mylog.error("Cannot re-create cluster status: some engines are busy.")
                return False
