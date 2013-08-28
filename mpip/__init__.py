"""Initialize mpip namespace.

Import all standard tasks.
"""

try:
    import matplotlib.pyplot as pl
    has_pl = True
except (RuntimeError, ImportError):
    print "\033[31;1mWARNING\033[0m: Matplotlib pyplot could not be imported. Plotting is disabled."
    has_pl = False

# import all the standard tasks
from tasks.run_bbs import Run_bbs
from tasks.run_cmd import Run_cmd
from tasks.run_casa import Run_casa
from tasks.clcmd import Clcmd
from tasks.jobs import Jobs
from tasks.run_awimager import Run_awimager
from tasks.movesb import Movesb
from tasks.run_ndppp import Run_ndppp
