""" Module jobs

Define task jobs which list, filter and remove the running/finished jobs.
"""
from ..task import Task
from .. import mylogger
from ..interface import print_jobs
import datetime
from IPython.parallel import Client
from IPython.parallel.util import unpack_apply_message

class Jobs(Task):

    __name__ = 'jobs'
    __doc__ = 'List, filter and remove the running/finished jobs.'
    banner = 'JOBS: show the processes status ad outputs.'
    arg_list = ['jcmd','sb','group','node','task','lines','onlyerr','queue','jobid']


    def __init__(self, session):
        Task.__init__(self, session, self.__name__)


    def _check_result(self, result):
        """Refine general query to the hub using local opts
        """
        try:
            SBnum = result['SB']
            if self.session.opts.get_opt('group'):
                if SBnum[0] == 'g':
                    SBnum = SBnum.replace('g','')
                else:
                    return False
            if (int(SBnum) in self.session.opts.get_opt('sb') or SBnum == '' or self.session.opts.get_opt('sb') == []) and \
               (result['node'] in self.session.opts.get_opt('node') or result['node'] == '' or self.session.opts.get_opt('node') == '') and \
               (result['task'] in self.session.opts.get_opt('task') or result['task'] == '' or self.session.opts.get_opt('task') == ''):
                return True
            else:
                return False

        # probably not a task-related result (e.g. one from clcmd)
        except:
            return False


    def run(self):
        if self.session.get_client() == None:
            self.mylog.error("Not connected to a cluster.")
            return False

        # workaround for Ipython bug which makes everything slow,
        # create a new client, use it and delete it
        c = Client(profile='ssh')

        jcmd = self.session.opts.get_opt('jcmd')
        if jcmd == 'purge':
            num = 0
            query = c.db_query({'completed':{'$ne' : None }},['msg_id'])
            for q in query:
                result = c.get_result(q['msg_id']).get()
                # filter on SB, node, task
                if self._check_result(result):
                    num += 1
                    c.purge_results(q['msg_id'])
            mylogger.userinfo(self.mylog, str(num)+" cluster's hub results deleted.")

        elif jcmd == 'list':
            num = 0
            # query the hub DB for all the finished tasks and get IDs
            query = c.db_query({'completed':{'$ne' : None }},['msg_id','completed','started'])
            # search for interesting results and print them
            for q in query:
                #try:
                result = c.get_result(q['msg_id']).get()
                #except:
                #    print "Error!"
                #    continue

                # filter on SB, node, task
                if self._check_result(result):
                    # skip results without error if wanted
                    if self.session.opts.get_opt('onlyerr') and result['err'] == '': continue
                    num += 1
                    header = {'Task' : result['task'], 'Node' : result['node'],\
                          'SB' : result['SB'], \
                          'Completed' : q['completed'].replace(microsecond=0), \
                          'Started' : q['started'].replace(microsecond=0), \
                          'Exec time': q['completed'].replace(microsecond=0)-q['started'].replace(microsecond=0)}
                    data = {'Std Output': result['out'], 'Std Error': result['err'], \
                          'Command':result['command']}
                    print_jobs(header, data, self.session.opts.get_opt('lines'))
            mylogger.userinfo(self.mylog, str(num)+" processes listed.")

        elif jcmd == 'running':
            num_r = 0
            num_q = 0
            # TODO: it should be "Started" not "submitted", unfortunately ipython does not set it
            query = c.db_query({'completed': None},['buffers','engine_uuid','submitted'])
            for q in query:
                # unpack the buffer of the sent jobs to obtain the arguments
                null, com, args = unpack_apply_message(q['buffers'])
                # filter on SB, node, task
                if self._check_result({'node':args['node'],'SB':args['SB'],'task':args['task']}):

                    if q['engine_uuid'] == None:
                        if self.session.opts.get_opt('queue') == False: continue
                        q['msg_id'] = q['msg_id']+" (queue)"
                        num_q += 1
                    else:
                        num_r += 1

                    header = {'Msg_id' : q['msg_id'], 'Task' : args['task'], 'Node' : args['node'], 'SB' : args['SB'], \
                       'Started' : q['submitted'].replace(microsecond=0), \
                       'Extime': datetime.datetime.now().replace(microsecond=0) - q['submitted'].replace(microsecond=0)}

                    data = {'Command': com[0]}
                    print_jobs(header, data, self.session.opts.get_opt('lines'))


            mylogger.userinfo(self.mylog, "Processes running: "+str(num_r)+". In queue: "+str(num_q)+".")
        
        elif jcmd == 'kill':
            print "TBI"

        #TODO: add a resubmit option to resubmit all tasks that failed http://ipython.org/ipython-doc/stable/parallel/parallel_task.html

        del c
