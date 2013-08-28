"""Version module.

This module simply stores the version
"""

# Version number
__version__ = '1.1'
__revision__ = filter(str.isdigit, "$Revision: 00000 $")

# Change log
def changelog():
    """
    m-pip Changelog.
    -----------------------------------------------
    2013/03/11 - Initial alpha version
    2013/03/19 - Added run_bbs, clcmd and jobs tasks
    2013/03/20 - Added percentage of free space in nodes
    2013/03/22 - Added possibility to change BBS -k option
    2013/03/25 - Added run_awimager task
               - Better cluster shutdown
    2013/03/26 - Added movesb task
               - Cleaned code in clcmd
    2013/03/27 - Reorganized package
    2013/04/24 - sb option now follows the CASA standards
    2013/05/18 - jobs on a SB are executed in the order they are launched
               - jobs are rerun up to 3 times on failure
    2013/07/09 - group support added
               - run_ndppp task added
    """
    pass
