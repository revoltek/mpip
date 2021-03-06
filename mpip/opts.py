"""mpip options

Options are essentially user-controllable parameters passed into mpip
operations, and allow for end-users to control the exact details of how
calculations are done.

The doc string should give a short description of the option, followed by a
line break ('\n') then a long, detailed description. The short description can
then be split off using "str(v.doc()).split('\n')[0]".

The group string can be used to group suboptions under a parent option.  The
group string should be the name of the parent option, which must be Bool
(except for the "hidden" group, which will suppress listing of the option; the
option can still be set as normal).

In general it's better to specify newly added options directly in this file, so
one can oversee them all. But it's also possible to extend it at run-time, and
under some circumstances (e.g. pybdsm installed system-wide, and there is no
way to modify this file) this might be the only option to do so. An example of
such extension follows:

==== file newmodule.py ====
from image import Op

class Op_new_op(Op):
    ## do something useful here
    ## we need to add option my_new_opt
    pass

## this will extend Opts class at runtime and ensure that
## type-checking works properly.
Opts.my_new_opt = Float(33, doc="docstring")
"""
import sys, getpass
import numpy as N
from tc import Int, Float, Bool, String, Tuple, Enum, \
      Option, NArray, Instance, tInstance, List, Any, TCInit, tcError, tcCType

class Opts(object):

    # Observation details (in many tasks)
    obsdir = String('/data/scratch/'+getpass.getuser(), doc = "Observation dir\n")
    group = Bool(False, doc = "True to work on groups and not on SBs\n")
    sb = String('', doc = "On which SBs run the task. Range: xxx..yyy (yyy exscluded) or xxx,yyy,zzz\n")
    msname = String('', doc = "MS name, $SB will be interpreted as the SB number. Default: obsname$SB.MS\n")
    node = String('', doc = "Node where to run the command\n")

    # clcmd
    obsname = String('', doc = "Observation name")
    engpernode = Int(1, doc = "Number of engines per node [TBI]\n", group = ['ccmd',['start']])
    ccmd = Enum('start', 'stop', 'update', doc = "start/stop/update (and populate) the cluster\n")

    # run_bbs
    bbs_parset = String('', doc = "Parset file for a BBS run\n")
    skymodel = String('', doc = "Skymodel (BBS format) file name\n")
    bbsopt = String('-f', doc = "Option for bbs [default: -f]\n")

    # run_ndppp
    ndppp_parset = String('', doc = "Parset file for an NDPPP run (this parset must not contain msin and msout parameters)\n")
    msin = String('', doc = "MS for input to an NDPPP run\n")
    msout = String('', doc = "MS for output from an NDPPP run\n")

    # run_imager
    operation = Enum('csclean', 'empty', 'image', 'predict', 'psf',\
          doc="Operation to perform (empty, image, csclean, predict, psf).\n")
    image = String('', doc = "Image name")
    wprojplanes = Int(0, doc = "if >0 specifies nr of\
          convolution functions to use in W-projection.\n")
    weight = Enum('briggs','uniform', 'superuniform', 'natural', 'briggs (robust)', 'briggsabs', 'radial', doc='Weighting scheme (uniform, superuniform, natural, briggs (robust), briggsabs, or radial).\n')
    robust = Float(0.0, doc='Robust parameter.\n', group=['weight',['briggs']])
    npix = Int(1024, doc='number of image pixels in x and y direction.\n')
    cellsize = String('5arcsec', doc='pixel width in x and y direction.\n')
    datacol = String('CORRECTED_DATA', doc='Name of DATA column to use.\n')
    niter = Int(1000, doc='Number of clean iterations.\n')
    stokes = Enum('I', 'Q', 'U', 'V', 'IQUV', doc='Stokes parameters to image (e.g. IQUV).\n')
    cyclefactor = Float(1.5, doc='Cycle Factor. See Casa definition.\n')
    gain = Float(0.1, doc='Loop gain for cleaning')
    applyelement = Int(1, doc='If turned to >0,\
          apply the element beam every N number of timewindows.\n')
    timewindow = Int(300, doc='Width of time window (in sec) where AW-term is constant.\n')

    # run_cmd
    command = String('', doc = "Generic command to run.\n"\
                               "It understands the $SB convenction.")

    # run_casa
    casascript = String('', doc = "CASA command to run.\n")
    casaargs = String('', doc = "Arguments to pass to the casa script.\n"
                                "It understands the $SB convenction.")

    # movems
    mode = Enum('move','arrange','spread','group','LTAspread', doc = "'Move' SB from a node to another, 'group' them in a destination directory, 'spread' them across the cluster or 'LTAspread' them from the LTA across the cluster. 'Arrange' SB in a smart way.\n")
    fromnode = String('', doc = "Starting node.\n", group=['mode',['move']])
    tonode = String('', doc = "Destination node.\n", group=['mode',['move','group']])
    #gtonode = String('', doc = "Destination node.\n", group=['mode','group'])
    delete = Bool(False, doc = "Delete SB after moving.\n", group=['mode',['move']])
    maxused = Float(80, doc = "Percentage of max used space in a node to use it.\n", group=['mode',['LTAspread','spread','arrange']])
    repdir = String('', doc = "Directory with all the MSs.\n", group=['mode',['spread']])
    http_stagefile = String('', doc = "HTML staging file containing staged paths for SBs.\n", group=['mode',['LTAspread']])
    renum = Int(0, doc = "Renumerate all SBs subtracting this number.\n", group=['mode',['LTAspread','spread']])
    destdir = String('', doc = "Name of the destination dir.\n", group=['mode',['group']])
    untar = Bool(False, doc = "Untar the MS once moved\n", group=['mode',['LTAspread','spread']])

    # jobs
    jcmd = Enum('list', 'purge', 'running', 'kill', doc = "list/purge/running/kill job\n")
    lines = Int('5', doc = "Number of lines of output to print, 0=all")
    onlyerr = Bool(False, doc = "Show only jobs with errors.\n", group=['jcmd',['list']])
    task = Enum('', 'run_bbs', 'run_awimager', 'run_cmd', 'run_casa', 'run_bbs', doc = "Select on given task\n")
    queue = Bool(True, doc = "Show also jobs in the queue.\n", group=['jcmd',['running']])
    jobid = String('', doc = "Job ID to kill.\n", group=['jcmd',['kill']])


    def __init__(self, values = None):
        """Build an instance of Opts and (possibly)
        initialize some variables.

        Parameters:
        values: dictionary of key->value for initialization
                of variables
        """
        TCInit(self)
        if values is not None:
            self.set_opts(values)

    def _parse_string_as_bool(self, bool_string):
        """
        'private' function performing parse of a string containing
        a bool representation as defined in the parameter set/otdb
        implementation
        """
        true_chars = ['t', 'T', 'y', 'Y', '1']
        false_chars = ['f', 'F', 'n', 'N', '0']
        if bool_string[0] in true_chars:
            return True
        if bool_string[0] in false_chars:
            return False

        raise tcError(
            "Supplied string cannot be parsed as a bool: {0}".format(bool_string))


    def set_opts(self, opts):
        """Set multiple variables at once.

        opts should be dictionary of name->value
        """
        opts = dict(opts)
        for k, v in opts.iteritems():
            try:
                # Fix for lofar parameter set integration:
                # If the attribute is a bool, test if it is a string.
                # and then try to parse it
                if hasattr(self, k):
                    if isinstance(self.__getattribute__(k), bool):
                        if isinstance(v, bool) or v == None:
                            # just enter the bool into the parameter
                            pass
                        elif isinstance(v, basestring):
                            # Try parse it as a parameter set bool string
                            v = self._parse_string_as_bool(v)
                        else:
                            # raise error
                            raise tcError("unknown type for bool variable")
                if k == 'sb':
                    # quick check on the sb sintax
                    import re
                    def sb_re(strg):
                        return bool(re.match("^[0-9,.]*$", strg))
                    if not sb_re(v): raise RuntimeError('Parameter "{0}" is not defined properly.'.format(k))

                if v == "none":
                    v = None
                self.__setattr__(k, v)
            except tcError, e:
                # Catch and re-raise as a RuntimeError
                raise RuntimeError('Parameter "{0}" is not defined properly.\
                         \n {1}'.format(k, str(e)))


    def get_opt(self, opt):
        """Retrieve an option
        just send the option back, the only difference are:
        - the sb obtion which is returned as a list once converted
        """
        if opt == 'sb':
            v = self.__getattribute__(opt)
            if v == '': return []
            sb_list = []
            for num in v.split(','):
                try:
                    sb_list.append(int(num))
                except:
                    ini, end = num.split('..')
                    sb_list.append(range(int(ini),int(end)))
            # remove the list of list generated by the range()
            def flatten(S):
                if S == []:
                    return S
                if isinstance(S[0], list):
                    return flatten(S[0]) + flatten(S[1:])
                return S[:1] + flatten(S[1:])
            return flatten(sb_list)
        else:
            return self.__getattribute__(opt)


    def set_default(self, opt_names = None):
        """Set one or more opts to default value.

        opt_names should be a list of opt names as strings, but can be
        a string of a single opt name.

        If None, set all opts to default values."""
        if opt_names == None:
            TCInit(self)
        else:
            if isinstance(opt_names, str):
                opt_names = [opt_names]
            for k in opt_names:
                if isinstance(k, str):
                    self.__delattr__(k)

    def info(self):
        """Pretty-print current values of options"""
        import tc
        ## enumerate all options
        opts = self.to_list()
        res = ""
        fmt = "%20s = %5s  ## %s\n"

        for k, v in opts:
            res += fmt % (k, str(self.__getattribute__(k)),
                          str(v.doc()).split('\n')[0])

        return res

    def to_list(self):
        """Returns a sorted list of (name, TC object) tuples for all opts."""
        import tc
        opts_list = []
        for k, v in self.__class__.__dict__.iteritems():
            if isinstance(v, tc.TC):
                opts_list.append((k, v))
        opts_list = sorted(opts_list)
        return opts_list

    def to_dict(self):
        """Returns a dictionary of names and values for all opts."""
        import tc
        opts_dict = {}
        for k, v in self.__class__.__dict__.iteritems():
            if isinstance(v, tc.TC):
                opts_dict.update({k: self.__getattribute__(k)})
        return opts_dict

    def get_names(self):
        """Returns a sorted list of names of all opts."""
        import tc
        opts_list = []
        for k, v in self.__class__.__dict__.iteritems():
            if isinstance(v, tc.TC):
                opts_list.append(k)
        opts_list = sorted(opts_list)
        return opts_list

    def __setstate__(self, state):
        self.set_opts(state)

    def __getstate__(self):
        import tc
        state = {}
        for k, v in self.__class__.__dict__.iteritems():
            if isinstance(v, tc.TC):
                state.update({k: self.__getattribute__(k)})
        return state
