""" Module task

Define the base class for all tasks
"""
import mylogger
import interface

class Task():
    """ General class for all task objects.

    Defines the common functions.
    """
    arg_list = []
    session = None
    mylog = None
    banner = 'No banner set.'
    __name__ = 'Unnamed'
    __doc__ = 'Doc not implemented.'

    def __init__(self, session, name):
        self.session = session
        self.mylog = mylogger.logging.getLogger('mpip.'+name)

    def __call__(self): #TODO: run set_pars_from_prompt otherwise no vars are set
        """Call run() so the user can call the task directly.
        """
        self.run()

    def run(self):
        """Run the task.
        """
        raise NotImplementdError, 'run() not implemented'

    def list_opts(self):
        """USe the interface module to pretty list the options.
        """
        interface.list_pars(self.session, self.arg_list, banner=self.banner)
