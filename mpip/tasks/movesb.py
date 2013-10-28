""" Module movesb

Define task movesb
"""
from ..task import Task
from .. import mylogger

class Movesb(Task):

    __name__ = 'movesb'
    __doc__ = 'doc for movesb'
    banner = 'movesb: move SBs between nodes or spread them in the cluster'
    arg_list = ['mode','fromnode','tonode','sb','msname','delete','maxused','repdir','renum','destdir','untar','http_stagefile']

    def __init__(self, session):
        Task.__init__(self, session, self.__name__)


    def _partition(self, lst, n):
        """Subdive a list "lst" in n sublists
        """
        q, r = divmod(len(lst), n)
        indices = [q*i + min(i, r) for i in xrange(n+1)]
        return [lst[indices[i]:indices[i+1]] for i in xrange(n)]


    def run(self):
        if self.session.get_status() == None:
            self.mylog.error("Not connected to a cluster.")
            return False

        mode = self.session.opts.get_opt('mode')
        s = self.session.get_status()
        wdir = self.session.opts.get_opt('obsdir')+'/'+self.session._obs_name

        if mode == 'move':

            try:
                SBs = s[self.session.opts.get_opt('fromnode')]['sb']
            except:
                self.mylog.error("Wrong \"fromnode\" parameter.")
                return False

            if not self.session.opts.get_opt('tonode') in s:
                self.mylog.error("Wrong \"tonode\" parameter.")
                return False

            # create destination dir
            command = 'mkdir -p ' + wdir
            self.session.run_dist_com('movesb', '', command, node=self.session.opts.get_opt('tonode'))
            # move SBs and optionally delete it
            command = 'scp -r SB$SB ' + self.session.opts.get_opt('tonode') + ':'\
                       + wdir + '/SB$SB'
            if self.session.opts.get_opt('delete'): command += ' && rm -r SB$SB'
            self.session.run_dist_com_all('movesb', wdir, command, SBs=self.session.opts.get_opt('sb'), nodes=self.session.opts.get_opt('fromnode'))

            # update session
            if self.session.opts.get_opt('sb') != []:
                SBs = self.session.opts.get_opt('sb')
            else:
                SBs = s[self.session.opts.get_opt('fromnode')]['sb']

            for SB in SBs:
                s[self.session.find_node_from_SB(SB)]['sb'].remove(SB)
                s[node]['sb'].append(SB)
                self.session.save_status()

        elif mode == 'arrange':
            import numpy as np

            try:
                SBs = s[self.session.opts.get_opt('fromnode')]['sb']
            except:
                self.mylog.error("Wrong \"fromnode\" parameter.")
                return False
            
            # find max SBs per node allawed
            max_SBs_per_node = int(np.ceil(len(self.session.get_avail_SBs()) / float(len(self.session.get_avail_nodes(maxused = self.session.opts.get_opt('maxused'))))))

            # collect SBs to move
            SBs_to_move = []
            for node in sorted(s.iterkeys()):
                # not enough space
                space = int(s[node]['df'])
                if space >= 95:
                    SBs_to_move.extend(s[node]['sb'])
                # too many on this node
                elif len(s[node]['sb']) > max_SBs_per_node:
                    SBs_to_move.extend(s[node]['sb'][:len(s[node]['sb'])-max_SBs_per_node])

            # find free spots
            for SB in SBs_to_move:
                # get the node with less SBs
                minSBnum = np.inf
                for node_to_inspect in self.session.get_avail_nodes(maxused = self.session.opts.get_opt('maxused'), maxSB = max_SBs_per_node):
                    if len(s[node_to_inspect]['sb']) < minSBnum:
                        minSBnum = len(s[node_to_inspect]['sb'])
                        node = node_to_inspect

                # create destination dir
                command = 'mkdir -p ' + wdir
                self.session.run_dist_com('movesb', '', command, node=node)
                command = 'scp -r SB' + SB + ' ' + node + ':'\
                       + wdir + '/SB' + SB + ' && rm -r SB' + SB
                self.session.run_dist_com('movesb', wdir, command, SB=SB, node=self.session.find_node_from_SB(SB))
                # update session
                s[self.session.find_node_from_SB(SB)]['sb'].remove(SB)
                s[node]['sb'].append(SB)
                self.session.save_status()


        elif mode == 'group':

            if not self.session.opts.get_opt('tonode') in s:
                self.mylog.error("Wrong \"tonode\" parameter.")
                return False

            # create destination dir
            command = 'mkdir -p ' + wdir + '/' + self.session.opts.get_opt('destdir')
            self.session.run_dist_com('movesb', '', command, node=self.session.opts.get_opt('tonode'))

            # move data
            command = 'scp -r SB$SB/' + self.session.opts.get_opt('msname') + \
                    ' ' + self.session.opts.get_opt('tonode') + ':' + \
                    wdir + '/' + self.session.opts.get_opt('destdir')
            self.session.run_dist_com_all('movesb', wdir, command, SBs=self.session.opts.get_opt('sb'))

        elif mode == 'spread':
            import itertools, glob, os
            import numpy as np

            # find usable nodes
            usablenodes = [node for node, val in s.iteritems() if val['df'] < self.session.opts.get_opt('maxused')]

            mylogger.userinfo(self.mylog, "Found " + str(len(usablenodes)) + " usable nodes.")
            # prepare list of SB to copy
            tgtsbs = np.array(sorted(glob.glob(self.session.opts.get_opt('repdir')+'/*')))
            # randomize SBs (se if a node fails one does not loose nearby SBs
            p = np.random.permutation(len(tgtsbs))
            tgtsbs = tgtsbs[p]
            blocks = self._partition(tgtsbs, len(usablenodes))

            for i, nodeblock in enumerate(blocks):
                for j, SB in enumerate(nodeblock):
                    SBnum = int(os.path.basename(SB).split('SB')[1][0:3]) # get SB num

                    # restrict on the sb option
                    if not SBnum in self.session.opts.get_opt('sb') and self.session.opts.get_opt('sb') != []: continue
                    SBnum = str(SBnum - self.session.opts.get_opt('renum')).zfill(3)

                    # if this SB is already present, use that node
                    findnode = self.session.find_node_from_SB(SBnum)
                    if findnode == None:
                        node = usablenodes[i]
                        mylogger.userinfo(self.mylog, node + ' (new): ' + SBnum)
                    else:
                        node = findnode
                        mylogger.userinfo(self.mylog, node + ' (existent): ' + SBnum)

                    # create dir
                    command = 'mkdir -p ' + wdir + '/SB' + SBnum
                    self.session.run_dist_com('movesb', '', command, node=node, SB=SBnum)

                    # move SB
                    wdirsb = wdir + '/SB' + SBnum + '/'
                    msname = self.session.opts.get_opt('msname').replace('$SB',SBnum)
                    command = 'cp -r ' + SB + ' ' + wdirsb + os.path.basename(SB)
                    if self.session.opts.get_opt('untar'):
                        command += ' ; tar xf ' + os.path.basename(SB) + ' && mv ' + os.path.basename(SB).replace('.tar', '') + ' ' + msname + ' && ' + 'rm ' + os.path.basename(SB)
                    else: command += ' && mv ' +  os.path.basename(SB) + ' ' + msname
                    self.session.run_dist_com('movesb', wdirsb, command, node=node, SB=SBnum)

        # get data from LTA: 
        elif mode == 'LTAspread':
            """
            Required 1.
            ~/.wgetrc  (private file) contents:
            user=username
            password=*****
            Required 2.
            Have staged the files and received the data ready for retrieval email
            with the html download links http://www.lofar.org/wiki/doku.php?id=public:lta_howto
            """
            import itertools, glob, os
            import numpy as np

            stagefile = self.session.opts.get_opt('http_stagefile')
            
            # find usable nodes
            usablenodes = [node for node, val in s.iteritems() if val['df'] < self.session.opts.get_opt('maxused')]

            mylogger.userinfo(self.mylog, "Found " + str(len(usablenodes)) + " usable nodes.")
            # prepare list of SB to copy from the stagefile (simple text file containing valid links)
            tgtsbpaths = np.loadtxt(stagefile, dtype='str')
            # randomize SBs (se if a node fails one does not loose nearby SBs
            p = np.random.permutation(len(tgtsbpaths))
            tgtsbpaths = tgtsbpaths[p]
            blocks = self._partition(tgtsbpaths, len(usablenodes))

            for i, nodeblock in enumerate(blocks):
                for j, SBpath in enumerate(nodeblock):
		    # SBpath is somethinglike long_link/L131784_SB054_uv.dppp.MS_94b2fac3.tar
		    # which when untared becomes L131784_SB054_uv.dppp.MS
		    
                    SBpath_name = os.path.basename(SBpath)
                    
                    SBnum = int(SBpath_name.split('SB')[1][0:3]) # get SB num

                    # restrict on the sb option
                    if not SBnum in self.session.opts.get_opt('sb') and self.session.opts.get_opt('sb') != []: continue
                    SBnum = str(SBnum - self.session.opts.get_opt('renum')).zfill(3)

                    # if this SB is already present, use that node
                    findnode = s.find_node_from_SB(SBnum)
                    if findnode == None:
                        node = usablenodes[i]
                        mylogger.userinfo(self.mylog, node + ' (new): ' + SBnum)
                    else:
                        node = findnode
                        mylogger.userinfo(self.mylog, node + ' (existent): ' + SBnum)

                    # create dir
                    command = 'mkdir -p ' + wdir + '/SB' + SBnum
                    self.session.run_dist_com('movesb', '', command, node=node)
                    

                    # move SB
                    wdirsb = wdir + '/SB' + SBnum + '/'
                    msname = self.session.opts.get_opt('msname').replace('$SB',SBnum)
                    # run wget to get the file
                    # used a --no-check-certificate  flag : this is not in general a good idea, but it doesn't work otherwise
                    command = 'wget --no-check-certificate  ' + SBpath + ' -O ' + wdirsb + SBpath_name +' >/dev/null 2>&1; stat=$?; echo $stat > '+wdirsb+msname+'_wget.log '
                    if self.session.opts.get_opt('untar'):
                        command += ' && tar xf ' + SBpath_name + ' && mv ' + SBpath_name.replace('_'+SBpath_name.split('_')[-1], '') + ' ' + msname + ' && ' + 'rm ' + SBpath_name
                    else:
                        # if the name does not include .tar and we are not going to untar, force the extension to remain
                        if '.tar' not in msname: msname += '.tar'
                        command += ' && mv ' +  SBpath_name + ' ' + msname
                    self.session.run_dist_com('movesb', wdirsb, command, node=node)
