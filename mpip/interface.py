"""Interface module.

The interface module handles all functions typically needed by the user in an
interactive environment such as IPython. Many are also used by the
custom IPython shell defined in pybdsm.py.

"""

#def load_pars(filename):
#    """Load parameters from a save file or dictionary.
#
#    The file must be a pickled opts dictionary.
#
#    filename - name of options file to load.
#    Returns None (and original error) if no file can be loaded successfully.
#    """
#    from session import Session
#    import mylogger
#    try:
#        import cPickle as pickle
#    except ImportError:
#        import pickle

    # First, check if input is a dictionary
 #   if isinstance(filename, dict):
 #       timg = Session(filename)
 #       return timg, None
 #   else:
 #       try:
 #           pkl_file = open(filename, 'rb')
 #           pars = pickle.load(pkl_file)
 #           pkl_file.close()
 #           timg = Session(pars)
 #           print "--> Loaded parameters from file '" + filename + "'."
 #           return timg, None
 #       except Exception, err:
 #           return None, err

#def save_pars(session, savefile=None, quiet=False):
#    """Save parameters to a file.

#    The save file is a "pickled" opts dictionary.
#    """
#    try:
#        import cPickle as pickle
#    except ImportError:
#        import pickle
#    import tc
#    import sys

#    if savefile == None or savefile == '':
#        savefile = img.opts.filename + '.pybdsm.sav'

    # convert opts to dictionary
#    pars = session.opts.to_dict()
#    output = open(savefile, 'wb')
#    pickle.dump(pars, output)
#    output.close()
#    if not quiet:
#        print "--> Saved parameters to file '" + savefile + "'."

def list_pars(session, opts_list=None, banner=None, use_groups=True):
    """Lists all parameters for the Session object.

    opts_list - a list of the parameter names to list;
                if None, all parameters are used.
    banner - banner text to place at top of listing.
    use_groups - whether to use the group information for each
                 parameter.
    """
    import tc
    import sys

    # Get all options as a list sorted by name
    opts = session.opts.to_list()

    # Filter list
    if opts_list != None:
        opts_temp = []
        for o in opts:
            if o[0] in opts_list:
                opts_temp.append(o)
        opts = opts_temp

    # Move filename, infile, outfile to front of list
    for o in opts:
        if o[0] == 'filename' or o[0] == 'infile' or o[0] == 'outfile':
            opts.remove(o)
            opts.insert(0, o)

    # Now group options with the same "group" together.
    if use_groups:
        opts = group_opts(opts)

    # Finally, print options, values, and doc strings to screen
    print_opts(opts, session, banner=banner)


def set_pars(session, **kwargs):
    """Set parameters using arguments instead of using a dictionary.

    Allows partial names for parameters as long as they are unique. Parameters
    are set to default values if par = ''.
    """
    import re
    import sys
    from session import Session

    # Enumerate all options
    opts = session.opts.get_names()

    # Check that parameters are valid options and are unique
    full_key = []
    for i, key in enumerate(kwargs):
        chk_key = checkpars(opts, key)
        if chk_key == []:
            raise RuntimeError("Input parameter '" + key + "' not recognized.")
        if len(chk_key) > 1 and key not in opts:
            raise RuntimeError("Input parameter '" + key + "' matches to more than one "\
                         "possible parameter:\n " + "\n ".join(chk_key))
        if key in opts:
            full_key.append(key)
        else:
            full_key.append(chk_key[0])

    # Build options dictionary
    pars = {}
    for i, key in enumerate(kwargs):
        if kwargs[key] == '':
            temp_session = Session({'filename':''})
            opt_names = temp_session.opts.get_names
            for k in opt_names:
                if key == k:
                    kwargs[key] = temp_session.opts.__getattribute__(k)
        pars.update({full_key[i]: kwargs[key]})

    # Finally, set the options
    session.opts.set_opts(pars)


def group_opts(opts):
    """Sorts options by group (as defined in opts.py).

    Returns a list of options, with suboptions arranged in a dict inside the
    main list and directly following the main options. The dict has as index the
    value of the parent option which activate the child option. Options belonging to the
    "hidden" group are excluded from the returned list (as defined in opts.py).
    """
    # mskr a list of all groups
    groups = []
    for opt in opts:
        grp = opt[1].group()
        if grp != None:
            if grp[0] not in groups:
                groups.append(grp[0])

    groups.sort()

    # Now, make a list for each group with its options. Don't include
    # "hidden" options, as they should never by seen by the user.
    for g in groups:
        g_list = {}
        for opt in opts:
            if isinstance(opt, tuple):
                grp = opt[1].group()
                # if this opt depends on the current group
                if grp != None and g == str(grp[0]):
                    # active it only for these activators
                    activators = grp[1]
                    for activator in activators:
                        if activator not in g_list: g_list[activator] = []
                        g_list[activator].append(opt)
        # remove entries listd under an activator form opt (prevent twice printing)
        for activator in g_list:
            for gs in g_list[activator]:
                if gs in opts:
                    opts.remove(gs)
                
        for i in range(len(opts)):
            if g == str(opts[i][0]) and g != 'hidden':
                opts.insert(i+1, g_list)
                break
    return opts


def getTerminalSize():
    """
    returns (lines:int, cols:int)
    """
    import os, struct
    def ioctl_GWINSZ(fd):
        import fcntl, termios
        return struct.unpack("hh", fcntl.ioctl(fd, termios.TIOCGWINSZ, "1234"))
    # try stdin, stdout, stderr
    for fd in (0, 1, 2):
        try:
            return ioctl_GWINSZ(fd)
        except:
            pass
    # try os.ctermid()
    try:
        fd = os.open(os.ctermid(), os.O_RDONLY)
        try:
            return ioctl_GWINSZ(fd)
        finally:
            os.close(fd)
    except:
        pass
    # try `stty size`
    try:
        return tuple(int(x) for x in os.popen("stty size", "r").read().split())
    except:
        pass
    # try environment variables
    try:
        return tuple(int(os.getenv(var)) for var in ("LINES", "COLUMNS"))
    except:
        pass
    # Give up. return 0.
    return (0, 0)


def print_jobs(header='', out='', lines=3):
    """Print the jobs list.

    Out is a dict with {title: output, title: output}.
    """
    nc = '\033[0m'    # normal text color
    ncb = '\033[1m'    # normal text color bold
    dc = '\033[1;34m' # Blue: non-default option text color
    termy, termx = getTerminalSize() # note: returns row, col -> y, x
    if 'Msg_id' in header:
        print ncb + 'ID' + nc + ": " + header['Msg_id']
    if 'Task' in header:
        print ncb + 'Task' + nc + ": " + header['Task'] + ' - ',
    if 'Node' in header:
        print ncb + 'Node' + nc + ": " + header['Node'] + ' - ',
    if 'SB' in header:
        print ncb + 'SB' + nc + ": " + header['SB'],
    print "\n",

    if 'Started' in header:
        print ncb + 'Start' + nc + ": " + str(header['Started']),
    if 'Completed' in header:
        print ' - ' + ncb + 'End' + nc + ": " + str(header['Completed']),
    if 'Extime' in header:
        print '(' + ncb + 'Exec time' + nc + ": " + str(header['Extime']) +')'
    print "\n",

    for o in out:
        print dc + o + ":" + nc
        for line in out[o].split("\n")[-lines:]:
            print line
    print "=" * termx # division string between jobs


def print_opts(grouped_opts_list, session, banner=None):
    """Print options to screen.

    Options can be sorted by group (defined in opts.py) previously defined by
    group_opts. Output of grouped items is suppressed if parent option is
    False. The layout is as follows:

      [20 spaces par name with ...] = [at least 49 spaces for value]
                                      [at least 49 spaces for doc]

    When more than one line is required for the doc, the next line is:

      [25 blank spaces][at least 47 spaces for doc]

    As in casapy, print non-defaults in blue, options with suboptions in
    47m and suboptions in green. Option Values are printed in bold, to help
    to distinguish them from the descriptions. NOTE: in iTerm, one needs
    to set the bold color in the profiles to white, as it defaults to red,
    which is a bit hard on the eyes in this case.
    """
    import os

    termy, termx = getTerminalSize() # note: returns row, col -> y, x
    minwidth = 28 # minimum width for parameter names and values

    # Define colors for output
    dc = '\033[1;34m' # Blue: non-default option text color
    ec = '\033[0;47m' # expandable option text color
    sc = '\033[0;32m' # Green: suboption text color
    nc = '\033[0m'    # normal text color
    ncb = '\033[1m'   # normal text color bold

    if banner != None:
        print banner
    spcstr = ' ' * minwidth # spaces string for second or later lines
    infix = nc + ': ' + nc # infix character used to separate values from comments
    print '=' * termx # division string for top of parameter listing
    for indx, o in enumerate(grouped_opts_list):
        if isinstance(o, tuple):
            # Print main options, which are always tuples, before printing
            # suboptions (if any).
            k = o[0]
            v = o[1]
            val = session.opts.__getattribute__(k)
            v1 = v2 = ''
            if val == v._default:
                # value is default
                v1 = ncb
                v2 = nc
            else:
                # value is non-default
                v1 = dc
                v2 = nc
            if isinstance(val, str):
                valstr = v1 + repr(val) + v2
                if k == 'filename':
                    # Since we can check whether filename is valid,
                    # do so here and print in red if not.
                    if not os.path.exists(val):
                        valstr = '\033[31;1m' + repr(val) + nc
                width_par_val = max(minwidth, len(k) + len(str(val)) + 5)
            else:
                if isinstance(val, float):
                    val = round_float(val)
                if isinstance(val, tuple):
                    val = round_tuple(val)
                valstr = v1 + str(val) + v2
                width_par_val = max(minwidth, len(k) + len(str(val)) + 4)
            width_desc = max(termx - width_par_val - 3, 44)
            # Get the option description text from the doc string, which
            # is defined in opts.py. By convention, print_opts will only
            # show the short description; help('option_name') will
            # print both the short and long description. The versions
            # are separated in the doc string by '\n', which is split
            # on here:
            desc_text = wrap(str(v.doc()).split('\n')[0], width_desc)
            fmt = '%' + str(minwidth) + 's' + infix + '%44s'

            # Now loop over lines of description
            if indx < len(grouped_opts_list)-1:
                # Here we check if next entry in options list is a tuple or a
                # list.  If it is a list, then the current option has
                # suboptions and should be in the ec color. Since we check the
                # next option, we can't do this if we let indx go to the end.
                if isinstance(grouped_opts_list[indx+1], tuple):
                    parvalstr = nc + k + nc + ' ..'
                else:
                    parvalstr = ec + k + nc + ' ..'
            else:
                # Since this is the last entry in the options list and is a
                # tuple, it cannot be an expandable option, so make it nc color
                parvalstr = nc + k + nc + ' ..'
            if "'" in valstr:
                len_without_formatting = len(k) + len(str(val)) + 5
            else:
                len_without_formatting = len(k) + len(str(val)) + 4
            for i in range(len_without_formatting, minwidth):
                parvalstr += '.'
            parvalstr += ' ' + valstr
            if "'" not in valstr:
                parvalstr += ' '
            for dt_indx, dt in enumerate(desc_text):
                if dt_indx == 0:
                    print fmt % (parvalstr.ljust(minwidth), dt.ljust(44))
                else:
                    print nc + spcstr + '   %44s' % dt.ljust(44)
        else:
            # Print suboptions, indented 2 spaces from main options in sc color
            parent_opt_val = session.opts.__getattribute__(grouped_opts_list[indx-1][0])
            for act in o:
                # Print the option only if it is activated by the parent
                if act != parent_opt_val: continue
                for og in o[act]:
                    k = og[0]
                    v = og[1]
                    val = session.opts.__getattribute__(k)
                    v1 = v2 = ''
                    if val == v._default:
                        # value is default
                        v1 = ncb
                        v2 = nc
                    else:
                        # value is non-default
                        v1 = dc
                        v2 = nc
                    if isinstance(val, str):
                        valstr = v1 + repr(val) + v2
                        width_par_val = max(minwidth, len(k) + len(str(val)) + 7)
                    else:
                        if isinstance(val, float):
                            val = round_float(val)
                        valstr = v1 + str(val) + v2
                        width_par_val = max(minwidth, len(k) + len(str(val)) + 6)
                    width_desc = max(termx - width_par_val - 3, 44)
                    desc_text = wrap(str(v.doc()).split('\n')[0], width_desc)
                    fmt = '  ' + '%' + str(minwidth) + 's' + infix + '%44s'
                    parvalstr = sc + k + nc + ' ..'
                    if "'" in valstr:
                        len_without_formatting = len(k) + len(str(val)) + 7
                    else:
                            len_without_formatting = len(k) + len(str(val)) + 6
                    for i in range(len_without_formatting, minwidth):
                        parvalstr += '.'
                    parvalstr += ' ' + valstr
                    if "'" not in valstr:
                        parvalstr += ' '
                    for dt_indx, dt in enumerate(desc_text):
                        if dt_indx == 0:
                            print fmt % (parvalstr.ljust(minwidth-2), dt.ljust(44))
                        else:
                            print nc + spcstr + '   %44s' % dt.ljust(44)


def print_cluster(status):
    """Print the cluster status.
    """
    num_e = num_e_red = num_e_yellow = num_e_green = 0
    num_SB = num_SB_red = num_SB_yellow = num_SB_green = 0
    num_group = num_group_red = num_group_yellow = num_group_green = 0
    avail_SBs = []
    avail_nodes = []
    if status is not None:
        for node in sorted(status.iterkeys()):
            print node + ": " + str(len(status[node]['e'])),
            space = int(status[node]['df'])
            avail_SBs.extend(status[node]['sb'])
            avail_nodes.append(node)

            if (space >= 95):
                print " \033[0;31m(" + str(space) + "%)\033[0m",
                num_e_red += len(status[node]['e'])
                for SB in status[node]['sb']:
                    num_SB_red += 1
                    print SB,
                for group in status[node]['group']:
                    num_group_red += 1
                    print '\033[1;36m'+group+'\033[0m',

            elif (80 <= space and space <= 95):
                print " \033[0;33m(" + str(space) + "%)\033[0m",
                num_e_yellow += len(status[node]['e'])
                for SB in status[node]['sb']:
                    num_SB_yellow += 1
                    print SB,
                for group in status[node]['group']:
                    num_group_yellow += 1
                    print '\033[1;36m'+group+'\033[0m',

            else:
                print " \033[0;32m(" + str(space) + "%)\033[0m",
                num_e_green += len(status[node]['e'])
                for SB in status[node]['sb']:
                    num_SB_green += 1
                    print SB,
                for group in status[node]['group']:
                    num_group_green += 1
                    print '\033[1;36m'+group+'\033[0m',

            num_e += len(status[node]['e'])
            num_SB += len(status[node]['sb'])
            num_group += len(status[node]['group'])
            print "\n",

        avail_SBs = [int(SB) for SB in avail_SBs]
        if len(avail_SBs) != (max(avail_SBs)+1):
            print "--> Missing SBs: ",
            for SB in xrange(max(avail_SBs)+1):
                if not SB in avail_SBs:
                    print SB,
            print ""
    
        if len(avail_nodes) != 60:
            print "--> Missing Nodes: ",
            for node in xrange(1,61):
                if not 'lce%03d' % node in avail_nodes:
                    print node,
            print ""

        print "\n\033[1;34m--> Total engines: " + str(num_e) + \
              " (\033[1;32m" + str(num_e_green) + \
              "\033[1;34m, \033[1;33m" + str(num_e_yellow) + \
              "\033[1;34m, \033[1;31m" + str(num_e_red) + \
              "\033[1;34m) - Total SB: " + str(num_SB) + \
              " (\033[1;32m" + str(num_SB_green) + \
              "\033[1;34m, \033[1;33m" + str(num_SB_yellow) + \
              "\033[1;34m, \033[1;31m" + str(num_SB_red) + \
              "\033[1;34m) - Total groups: " + str(num_group) + \
              " (\033[1;32m" + str(num_group_green) + \
              "\033[1;34m, \033[1;33m" + str(num_group_yellow) + \
              "\033[1;34m, \033[1;31m" + str(num_group_red) + "\033[1;34m)"

    else:
        print "Engines are off."


def wrap(text, width=80):
    """Wraps text to given width and returns list of lines."""
    lines = []
    for paragraph in text.split('\n'):
        line = []
        len_line = 0
        for word in paragraph.split(' '):
            word.strip()
            len_word = len(word)
            if len_line + len_word <= width:
                line.append(word)
                len_line += len_word + 1
            else:
                lines.append(' '.join(line))
                line = [word]
                len_line = len_word + 1
        lines.append(' '.join(line))
    return lines


def checkpars(lines, regex):
    """Checks that parameters are unique"""
    import re
    result = []
    for l in lines:
        match = re.match(regex,l)
        if match:
            result += [l]
    return result


def in_ipython():
    """Checks if interpreter is IPython."""
    try:
        __IPYTHON__
    except NameError:
        return False
    else:
        return True


def raw_input_no_history(prompt):
    """Removes user input from readline history."""
    import readline
    input = raw_input(prompt)
    if input != '':
        readline.remove_history_item(readline.get_current_history_length()-1)
    return input


# The following functions just make the printing of
# parameters look better
def round_tuple(val):
    valstr_list = []
    for v in val:
        vstr = '%s' % (round(v, 5))
        if len(vstr) > 7:
            vstr = '%.5f' % (v,)
        valstr_list.append(vstr)
    valstr = '(' + ','.join(valstr_list) + ')'
    return valstr

def round_float(val):
    vstr = '%s' % (round(val, 5))
    if len(vstr) > 7 and val < 1e3:
        vstr = '%.5f' % (val,)
    elif len(vstr) > 7 and val >= 1e3:
        vstr = '%.2e' % (val,)
    return vstr

def round_list(val):
    valstr_list = []
    for v in val:
        valstr_list.append('%.2e' % (v,))
    valstr = '[' + ','.join(valstr_list) + ']'
    return valstr

def round_list_of_tuples(val):
    valstr_list = []
    valstr_list_tot = []
    for l in val:
        for v in l:
            vstr = '%s' % (round(v, 5))
            if len(vstr) > 7:
                vstr = '%.5f' % (v,)
            valstr_list.append(vstr)
        valstr = '(' + ','.join(valstr_list) + ')'
        valstr_list_tot.append(valstr)
    valstr = '[' + ','.join(valstr_list_tot) + ']'
    return valstr

def add_break_to_logfile(logfile):
    f = open(logfile, 'a')
    f.write('\n' + '='*72 + '\n')
    f.close()
