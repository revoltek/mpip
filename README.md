Minimal (LOFAR) PIPeline

REQUIREMENTS:
tested with ipython 0.13.2

INSTALL:
move the mpip dir into a place pointed by your $PYTHONPATH
or just run mpip.py from a dir which contains the mpip directory.

TODO:
 - check the CPU usage on the frontend
 - runs longer than 1 day hangs
 - launch ipcluster without subprocess adding #eng and nodes
 - rewrite the output of the jobs task in a better format
 - catch errors and inform about them
 - efficent task killer
 - have different queues for different obsnames, more generally runs of different obsnames at the same time is not supported
