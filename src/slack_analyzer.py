#!/usr/bin/env python

"""Slack Analyzer.

slack_analyzer analyzes CSV files that have been pre-processed from Slack json
data and produces an HTML view of the conversation.

@attention: Dependent on pre-processor program to produce data in correct format/fields.

Results are saved to HTML files with the same base name in the same directory.

"""

import chardet
import codecs
import csv
from itertools import islice, cycle
import getopt
import os
import random
import re
import sys
import time
from textwrap import dedent


__author__ = "Danny Cheun"
__credits__ = ["Danny Cheun"]
__version__ = "1.0.0"
__maintainer__ = "Danny Cheun"
__email__ = "dcheun@gmail.com"


# Export on *
# __all__ = []

# Globals
# Store script_args passed to script.
script_args = {}


class Analyzer(object):
    
    """Analyzes resources."""
    
    _header = None
    _users = {}
    _color_gen = None
    _directory = None
    _process_file_cnt = 0
    _debug = False
    
    def __init__(self, _file=None, directory=None, encoding=None):
        """Constructs a new Analyzer object.
        
        @param _file: The absolute file path to analyze.
        @keyword directory: The absolute directory path to analyze.
        @keyword encoding: The encoding of the file.
                If None, tries to guess encoding type.
                Eg: utf-8
        
        """
        self._file = _file
        self._directory = directory
        self.encoding = encoding
        self._color_gen = self.color_pool_gen()
        self._users = {}
    
    def process(self):
        """Top level process."""
        # Process directory, otherwise process file.
        if not any([self._directory,self._file]):
            print('Nothing to process.')
            return
        if self._directory:
            self.process_directory()
        else:
            self.process_file()
    
    def process_directory(self):
        """Processes all csv files recursively inside a directory. Assumes
        proper formatting of csv.
        
        """
        if not self._directory:
            return
        for dirpath, dirnames, filenames in os.walk(self._directory):
            for f in filenames:
                fp = os.path.join(dirpath,f)
                if os.path.isfile(fp) and re.search(r'\.csv$',fp,re.IGNORECASE):
                    self._file = fp
                    self.process_file()
    
    def process_file(self):
        """Analyzes the file (self._file)."""
        # Detect encoding.
        if self.encoding is None:
            self.detect_encoding()
        
        curdir = os.path.sep.join(self._file.split(os.path.sep)[:-1])
        # output file.
        output_basename = '.'.join([os.path.splitext(os.path.basename(self._file))[0],'htm'])
        output_file = os.path.sep.join([curdir,output_basename])
        # Get CSV file generator.
        file_gen = self.file_generator_csv()
        # Assume first line is the header. Ignore line.
        file_gen.next()
        
        # Write out HTML file.
        with codecs.open(output_file, 'wb', encoding=self.encoding) as f:
            f.write('<html>\n')
            f.write('''<head>
            <style>
            h1 { font-family: Lato; font-size: 24px; font-style: normal; font-variant: normal; font-weight: 700; line-height: 26.4px; }
            h3 { font-family: Lato; font-size: 14px; font-style: normal; font-variant: normal; font-weight: 700; line-height: 15.4px; }
            p2 { font-family: Lato; font-size: 14px; font-style: normal; font-variant: normal; font-weight: 400; line-height: 20px; }
            p { font-family: Lato; font-size: 15px; font-style: normal; font-variant: normal; font-weight: 400; line-height: 20px; }
            ts { font-family: Lato; font-size: 12px; font-style: normal; font-variant: normal; font-weight: 400; line-height: 20px; }
            blockquote { font-family: Lato; font-size: 21px; font-style: normal; font-variant: normal; font-weight: 400; line-height: 30px; }
            pre { font-family: Lato; font-size: 13px; font-style: normal; font-variant: normal; font-weight: 400; line-height: 18.5667px; }
            atchan { font-family: Lato; font-size: 15px; font-style: normal; font-variant: normal; font-weight: 700; line-height: 20px; background: rgba(242,199,68,.2); }
            atuser { font-family: Lato; font-size: 15px; font-style: normal; font-variant: normal; font-weight: 400; line-height: 20px; background: rgba(29,155,209,.1); color: rgba(18,100,163,1); }
            </style>
            </head>
            ''')
            f.write('<body>\n')
            f.write('<table style="width:100%">\n')
            for line in file_gen:
                line = self.clean_line(line)
                f.write('<tr>\n')
                f.write('<td width="125px">\n')
                ts = ('<ts>%s</ts>' % line[0])
                f.write(ts)
                f.write('</td>\n')
                f.write('<td align="left" style="padding: 0px 0px 0px 5px">\n')
                msg = ('<p><b>%s</b>&nbsp;%s</p>' % (line[2],line[4]))
                f.write(msg)
                f.write('</td>\n')
                f.write('</tr>\n')
            f.write('</table>\n')
            f.write('</body>\n')
            f.write('</html>\n')
        
        self._process_file_cnt += 1
    
    @staticmethod
    def color_pool_gen():
        """Randomizes a list of HTML hex color codes and cycles through them.
        
        Each call to next() on the generator yields the next color with no end.
        
        """
        colors = ['#3AAF85','#AF3A8C','#3A3EAF','#22674F','#8CAF3A','#536722','#843AAF','#AF843A','#674E22','#AF3A5B']
        random.shuffle(colors)
        pool = cycle(colors)
        for color in pool:
            yield color
    
    def get_fmt_user_color(self, user):
        """Returns the user text decorated with span inline color.
        
        @param user: The text of the username.
        
        """
        try:
            color = self._users[user]['color_code']
        except KeyError:
            color = self._color_gen.next()
            self._users.update({user:{'color_code':color}})
        colored_user = '<span style="color:%s;">%s</span>' % (color,user)
        return colored_user
    
    def clean_line(self, line):
        """Applies some cleaning and formatting.
        """
        cleaned_line = line
        # Format username with colors.
        cleaned_line[2] = self.get_fmt_user_color(cleaned_line[2])
        # Other updates.
        cleaned_line = [re.sub(r'<!channel>','<atchan>@channel</atchan>',x) for x in cleaned_line]
        cleaned_line = [re.sub(r'<(@[^>]{5,})>','<atuser>\\1</atuser>',x) for x in cleaned_line]
        
        return cleaned_line
    
    def file_generator(self):
        """Generator object for file.
        
        Each call to next() yields one line of content from the file.
        
        """
        with codecs.open(self._file, encoding=self.encoding) as f:
            for line in f:
                yield line
    
    def file_generator_csv(self):
        """Generator object for csv.
        """
        with open(self._file,'rb') as f:
            csv_reader = csv.reader(f,dialect=csv.excel)
            for row in csv_reader:
                yield [unicode(cell, self.encoding) for cell in row]
    
    def detect_encoding(self, N=20):
        """Tries to detect the file's encoding.
        
        @attention: Defaults to UTF-8 if detection confidence is < 50%.
        
        @keyword N: The number of lines to read from a file to guess
                the encoding type.
        
        """
        try:
            N = int(N)
        except ValueError:
            N = 20
        _file = self._file
        
        unicode_regex = ('UTF|Big5|GB2312|EUC-TW|HZ-GB-2312|ISO-2022'
                         'EUC-JP|SHIFT_JIS|EUC-KR|TIS-620')
        
        with open(_file) as f:
            # Read N lines from file, and join them into a single
            # string buffer that chardet will accept.
            detect = chardet.detect(''.join(islice(f,N)))
            self.detect = detect
        
        encoding = detect['encoding'] if detect['encoding'] else ''
        m = re.search(unicode_regex, encoding, re.IGNORECASE)
        
        # If the confidence is better than or equal to 50%
        if (m and detect['confidence'] >= 0.5):
            self.encoding = encoding
        else:
            # Default to UTF-8
            self.encoding = 'utf-8'


###############################################################################
# Main.
###############################################################################
def usage():
    """Print usage info."""
    program_name = os.path.basename(sys.argv[0])
    message = ['Usage: %s <options>...' % program_name]
    message.append(dedent('''
    Required argument(s):
      NOTE: Specify FILE_PATH or DIR_PATH
      -f <FILE_PATH>, --file=<FILE_PATH>
            The absolute path of the file to analyze.
      -d <DIR_PATH>, --dir=<DIR_PATH>
            The absolute path of the directory to analyze.
    
    Optional argument(s):
      -e <ENCODING>, --encoding=<ENCODING>
            The encoding to use. If not specified, then
            tries to detect first, otherwise defaults to utf-8.
            Eg: -e utf-8
      -h, --help
            Displays this help screen.
    '''))
    print '\n'.join(message)


def handle_args():
    """Handle script's command line script_args."""
    global script_args
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f:e:d:h',
                                   ['file=','encoding=','dir=',
                                    'help','debug'])
    except getopt.GetoptError as e:
        # Print usage info and exit.
        print str(e)
        usage()
        sys.exit(2)
    
    for o, a in opts:
        if o == '-f' or o == '--file':
            script_args['file'] = a
        elif o == '-e' or o == '--encoding':
            script_args['encoding'] = a
        elif o == '-d' or o == '--dir':
            script_args['dir'] = a
        elif o == '-h' or o == '--help':
            script_args['help'] = a
        elif o == '--debug':
            script_args['debug'] = a
        else:
            assert False, 'Unhandled option %s' % o
    
    # Check for help.
    if 'help' in script_args:
        usage()
        sys.exit(0)
    # Check if required arguments are set.
    if not any([script_args.get('file'),script_args.get('dir')]):
        print >>sys.stderr, 'ERROR: Missing argument(s).'
        usage()
        sys.exit(2)


def main():
    global script_args
    handle_args()
    analyzer = Analyzer(_file=script_args.get('file'),
                        directory=script_args.get('dir'),
                        encoding=script_args.get('encoding')
                        )
    analyzer.process()
    print 'Done processing %s files.' % analyzer._process_file_cnt

if __name__ == '__main__':
    main()

