#!/home/fdg/.virtualenvs/revenv/bin/ipython
# Software by Francesco de Gasperin (astro@voo.it)
# Interface based on PyBDSM

# TODO:
#  - check the CPU usage on the frontend
#  - runs longer than 1 day hangs
#  - launch ipcluster without subprocess adding #eng and nodes

import mpip
from mpip.task import Task
from mpip.session import Session
import mpip.mylogger as mylogger
import sys, inspect

global _session
_session = Session(None)
_session._is_interactive_shell = True
mylog = _session.mylog

# To be accessible also for users
T = True
F = False
true = True
false = False

def inp(cur_cmd=None):
    """List inputs for current task.

    If a task is given as an argument, inp sets the current task
    to the given task. If no task is given, inp lists the parameters
    of the current task.
    """
    global _session
    success = _set_pars_from_prompt()
    if not success:
        return
    if cur_cmd != None:
        if not isinstance(cur_cmd, Task):
            mylog.error('not a valid task')
            return
        _set_current_cmd(cur_cmd)
    else:
        if _session.get_current_cmd() == None:
            mylog.error('no task is set')
            return
    _session.get_current_cmd().list_opts()

def go(cur_cmd=None):
    """Executes the current task.

    If a task is given as an argument, go executes the given task,
    even if it is not the current task. The current task is not
    changed in this case.
    """
    global _session
    success = _set_pars_from_prompt()
    if not success:
        return
    if cur_cmd != None:
        if not isinstance(cur_cmd, Task):
            mylog.error('not a valid task')
            return
        _set_current_cmd(cur_cmd)
    else:
        if _session.get_current_cmd() == None:
            mylog.error('no task is set')
            return
	else:
	    cur_cmd = _session.get_current_cmd()	

    return cur_cmd.run()


def default(cur_cmd=None):
    """Resets all parameters for a task to their default values.

    If a task name is given (e.g., "default show_fit"), the
    parameters for that task are reset. If no task name is
    given, the parameters of the current task are reset.
    """
    global _session
    if cur_cmd != None:
        if not isinstance(cur_cmd, Task):
            mylog.error('not a valid task')
            return
        _set_current_cmd(cur_cmd)
        opts_list = cur_cmd.arg_list
    else:
        if _session.get_current_cmd() == None:
            mylog.error('no task is set')
            return
        opts_list = _session.get_current_cmd().arg_list
 
    _session.opts.set_default(opts_list)
    _replace_vals_in_namespace(opt_names=opts_list)


def tget(filename=None):
    """Load processing parameters from a parameter save file.

    A file name may be given (e.g., "tget 'savefile.sav'"), in which case the
    parameters are loaded from the file specified. If no file name is given,
    the parameters are loaded from the file 'pybdsm.last' if it exists.

    Normally, the save file is created by the tput command (try "help tput"
    for more info).

    The save file is a "pickled" python dictionary which can be loaded into
    python and edited by hand. See the pickle module for more information.
    Below is an example of how to edit a save file by hand:

      BDSM [1]: import pickle
      BDSM [2]: savefile = open('savefile.sav', 'w')
      BDSM [3]: pars = pickle.load(savefile)
      BDSM [4]: pars['rms_box'] = (80, 20)  --> change rms_box parameter
      BDSM [5]: pickle.dump(pars, savefile) --> save changes

    """
    try:
        import cPickle as pickle
    except ImportError:
        import pickle
    import os

    global _session

    # Check whether user has given a task name as input (as done in casapy).
    # If so, reset filename to None but set the current_cmd.
    if isinstance(filename, Task):
        _set_current_cmd(filename)
        filename = None

    if filename == None or filename == '':
        if os.path.isfile('mpip.last'):
            filename = 'mpip.last'
        else:
            mylog.error('No file name given and '\
                  '"mpip.last" not found.\n Please specify a file to load.')
            return

    if os.path.isfile(filename):
        try:
            pkl_file = open(filename, 'rb')
            pars = pickle.load(pkl_file)
            pkl_file.close()
            _session.opts.set_opts(pars)
            _replace_vals_in_namespace()
            mylogger.userinfo(mylog,"Loaded parameters from file '" + filename + "'.")
        except Exception, e:
	    mylog.exception("Could not read file '" + filename + "'.\n")
    else:
        mylog.error("File '" + filename + "' not found.")


def tput(filename=None, quiet=False):
    """Save processing parameters to a file.

    A file name may be given (e.g., "tput 'savefile.sav'"), in which case the
    parameters are saved to the file specified. If no file name is given, the
    parameters are saved to the file 'pybdsm.last'. The saved parameters can
    be loaded using the tget command (try "help tget" for more info).

    The save file is a "pickled" python dictionary which can be loaded into
    python and edited by hand. See the pickle module for more information.
    Below is an example of how to edit a save file by hand:

      BDSM [1]: import pickle
      BDSM [2]: savefile = open('savefile.sav', 'w')
      BDSM [3]: pars = pickle.load(savefile)
      BDSM [4]: pars['rms_box'] = (80, 20)  --> change rms_box parameter
      BDSM [5]: pickle.dump(pars, savefile) --> save changes

    """
    try:
        import cPickle as pickle
    except ImportError:
        import pickle

    global _session
    success = _set_pars_from_prompt()
    if not success:
        return
    if filename == None or filename == '':
        filename = 'mpip.last'

    # convert opts to dictionary
    pars = _session.opts.to_dict()
    output = open(filename, 'wb')
    pickle.dump(pars, output)
    output.close()
    if not quiet:
        mylogger.userinfo(mylog, "Saved parameters to file '" + filename + "'.")


def _replace_vals_in_namespace(opt_names=None):
    """Replaces opt values in the namespace with the ones in _session.

    opt_names - list of option names to replace (can be string if only one)
    """
    global _session
    f_dict = globals()
    if opt_names == None:
        opt_names = _session.opts.get_names()
    if isinstance(opt_names, str):
        opt_names = [opt_names]
    for opt_name in opt_names:
        if opt_name in f_dict:
            f_dict[opt_name] = _session.opts.__getattribute__(opt_name)


def _set_pars_from_prompt():
    """Gets parameters and value and stores them in _session.

    To do this, we extract all the valid parameter names
    and values from the f_locals directory. Then, use
    set_pars() to set them all.

    Returns True if successful, False if not.
    """
    global _session
    f_dict = globals()

    # Check through all possible options and
    # build options dictionary
    opts = _session.opts.to_dict()
    user_entered_opts = {}
    for k, v in opts.iteritems():
        if k in f_dict:
            if f_dict[k] == '':
                # Set option to default value in _session and namespace
                _session.opts.set_default(k)
                f_dict[k] = _session.opts.__getattribute__(k)
            user_entered_opts.update({k: f_dict[k]})

    # Finally, set the options
    try:
        _session.opts.set_opts(user_entered_opts)
        return True
    except RuntimeError, err:
        # If an opt fails to set, replace its value in the namespace
        # with its current value in _session. Then print error so user knows.
        err_msg = str(err)
        err_msg_trim = err_msg.split('(')[0]
        indx1 = err_msg_trim.find('"') + 1
        indx2 = err_msg_trim.find('"', indx1)
        k = err_msg_trim[indx1:indx2]
        orig_opt_val = opts[k]
        f_dict[k] = orig_opt_val
        mylog.error(err_msg_trim + '\nResetting to previous value.')
        return False


def _set_current_cmd(cmd):
    """Sets information about current command in session.

    This function is used to emulate a casapy interface.

    """
    global _session
    cmd_name = cmd.__name__
    doc = cmd.__doc__
    _session._current_cmd = cmd
    _session._current_cmd_name = cmd_name
    _session._current_cmd_desc = cmd_name.upper() + ': ' + doc.split('\n')[0]
    _session._current_cmd_arg_list = cmd.arg_list


# Define the welcome banner to print on startup.
from mpip._version import __version__, __revision__, changelog

divider1 = '=' * 72 + '\n'
divider2 = '_' * 72 + '\n'
banner = '\nmpip version ' + __version__ + ' (revision ' + \
		         __revision__ + ')\n'\
			 + divider1 + 'mpip commands\n'\
			 '  inp task ............ : Set current task and list parameters\n'\
			 "  par = val ........... : Set a parameter (par = '' sets it to default)\n"\
			 '                          Autocomplete (with TAB) works for par and val\n'\
			 '  go .................. : Run the current task\n'\
			 '  default ............. : Set current task parameters to default values\n'\
			 "  tput ................ : Save parameter values\n"\
			 "  tget ................ : Load parameter values\n"\
			 'mpip tasks\n'\
			 '  run_bbs ............. : Run a distributed BBS\n'\
			 '  run_awimager ........ : Run a distributed AWimager\n'\
			 '  run_cmd ............. : Run a generic command\n'\
			 '  run_casa ............ : Run a CASA command\n'\
			 '  run_ndppp ........... : Run a NDPPP command\n'\
			 '  movesb .............. : Move/spread SBs in the cluster\n'\
			 '  clcmd ............... : Run engines on cluster nodes\n'\
			 '  jobs ................ : Inspect jobs results\n'\
			 'mpip help\n'\
			 '  help command/task ... : Get help on a command or task\n'\
			 '                          (e.g., help run_bbs)\n'\
			 "  help 'par' .......... : Get help on a parameter (e.g., help 'blabla')\n"\
			 '  help changelog ...... : See list of recent changes\n'\
			 + divider2


# IPython must be >= 0.13
try:
    from IPython.frontend.terminal.embed import InteractiveShellEmbed
    from IPython.config.loader import Config
    from IPython import __version__ as ipython_version
    cfg = Config()
    cfg.PromptManager.in_template = "MPIP [\#]: "
    cfg.InteractiveShellEmbed.autocall = 2
    ipshell = InteractiveShellEmbed(config=cfg, banner1=banner,
                                    user_ns=locals())
    _session._ipshell = ipshell
#    ipshell.set_hook('complete_command', _opts_completer, re_key = '.*')
except ImportError:
    if ipython_version.split('.')[1] < 13: 
       mylog.error('Ipython >13 required.')
       sys.exit(1)

# Initialize all the stadard tasks
run_bbs = mpip.Run_bbs(_session)
run_cmd = mpip.Run_cmd(_session)
run_casa = mpip.Run_casa(_session)
run_awimager = mpip.Run_awimager(_session)
run_ndppp = mpip.Run_ndppp(_session)
clcmd = mpip.Clcmd(_session)
jobs = mpip.Jobs(_session)
movesb = mpip.Movesb(_session)

ipshell()

