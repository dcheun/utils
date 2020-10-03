#!/usr/bin/env python

"""
Converts l's to I's
"""

import codecs
import getopt
import re
import sys
from textwrap import dedent

__author__ = "Danny Cheun"
__copyright__ = "Copyright 2019, DPK Production."
__credits__ = ["Danny Cheun"]
__license__ = "Proprietary software."
__version__ = "1.0.0"
__maintainer__ = "DPK Development Team"
__email__ = "dcheun@gmail.com"
__status__ = "Production"


__all__ = []

script_args = {}


def process(input, output):
    contents = []
    contents_clean = []
    with codecs.open(input, encoding='utf-8') as f:
        contents = f.readlines()
    # Clean.
    for line in contents:
        line = re.sub(r'^(l)\b', 'I', line)
        line = re.sub(r' l ', ' I ', line)
        line = re.sub(r'^(lf)\b', 'If', line)
        line = re.sub(r'\b(l\'m)\b', 'I\'m', line)
        line = re.sub(r'^-(lt\'s)\b', '-It\'s', line)
        line = re.sub(r'^(lt\'s)\b', 'It\'s', line)
        line = re.sub(r'\b(lt\'s)\b', 'it\'s', line)
        line = re.sub(r'\b(l\'ve)\b', 'I\'ve', line)
        contents_clean.append(line)
    # Write output.
    with open(output, 'wb') as f:
        for line in contents_clean:
            f.write(line.encode('utf-8'))


def usage():
    print(dedent("""
    Required arguments(s):
        -i <FILENAME> --input=<FILENAME>
            Input file.
        -o <FILENAME> --output=<FILENAME>
            Output file.
    Optional argument(s):
        -e <ENCODING> --encoding=<ENCODING>
            The encoding to read the file
        -h --help
    """))


def handle_args():
    global script_args
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'i:o:e:h',
                                   ['input=', 'outpu=',
                                    'encoding=', 'help'])
    except getopt.GetoptError as e:
        print(str(e))
        usage()
        sys.exit(2)
    for o, a in opts:
        if o == '-i' or o == '--input':
            script_args['input'] = a
        elif o == '-o' or o == '--output':
            script_args['output'] = a
        elif o == '-e' or o == '--encoding':
            script_args['encoding'] = a
        elif o == '-h' or o == '--help':
            script_args['help'] = a
        else:
            assert False, 'Unhandled option %s' % o
    
    if 'help' in script_args:
        usage()
        sys.exit(0)
    
    if 'input' not in script_args or 'output' not in script_args:
        print(' ERROR: No input/output files specified.')
        usage()
        sys.exit(2)


def main():
    global script_args
    handle_args()
    process(input=script_args['input'], output=script_args['output'])


if __name__ == '__main__':
    main()
