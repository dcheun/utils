#!/usr/bin/env python

"""Path Name Analyzer.

path_analyzer reads in a file containing information about pathnames,
and analyzes each folder level with statistics given the parameters.

The main analysis comes in the form of detecting long pathnames.
The algorithm builds a tree to navigate the folder structure.
The results are printed to a csv file.

Input: Raw text file with FULL listing of all "Long Path Files"

Long Path Files: Files identified by EnCase enscript as having an "item path"(including filename and directory tree) that is > 190 characters(this limit can be adjusted as needed)

Output Requested:

1) Outliers List #1: listing of files with a filename > 190 characters + cross-reference to appropriate batch
2) Outliers List #2: listing of files who's filename and parent directory together are > 250 characters ex) \FOLDER\filename... + cross-reference to appropriate batch
2) Batch List: listing of parent directories to target for extraction
    Batch Criteria: addressable path of all contents < 190 characters; flag and ignore outliers from list #1 and list #2
        addressable path = (targeted directory path + filename) < 250 characters

"""

from abc import ABCMeta, abstractmethod
import chardet
import codecs
from collections import defaultdict
import csv
import _ctypes
from datetime import datetime
from itertools import islice
import getopt
import os
import re
import sys
import time
from textwrap import dedent

__author__ = "Danny Cheun"
__credits__ = ["Danny Cheun"]
__version__ = "2.2.0"
__maintainer__ = "Danny Cheun"
__email__ = "dcheun@gmail.com"


# Export on *
__all__ = []

# Globals
# Store script_args passed to script.
script_args = {}

class Item(object):
    
    """Structure to hold relevant file data."""
    
    def __init__(self, data=None):
        if data is None:
            self._data = {}
        else:
            self._data = data
        self.dict_to_attrs(data)
    
    def dict_to_attrs(self, d):
        """Helper procedure to unpack dictionary items and convert them into
        class instance attributes.
        
        @param d: Dictionary containing attributes to unpack.
        
        """
        for k,v in d.iteritems():
            setattr(self, k, v)
    
    def update(self, data):
        """Updates instance with the data provided.
        
        @param data: The update data. Must be a dictionary.
        
        """
        self._data.update(data)
        self.dict_to_attrs(data)
    
    def print_attributes(self):
        message = '\n'.join("%s: %s" % item for item in self._data.items())
        log('INFO', logfile, message, print_stdout=True)
    
    def get(self, attribute):
        try:
            return getattr(self, attribute)
        except AttributeError:
            return None
    
    def __repr__(self):
        _id = self.id if 'id' in self._data else None
        return '%s(%s)' % (self.__class__.__name__,_id)
    
    # alias
    print_attr = print_attributes


class Node(object):
    
    """Structure to hold tree nodes attributes.
    
    These nodes will NOT be on the tree implementation itself, but serve
    as a lookup for its attributes.
    
    """
    
    # Global counter of nodes created.
    _COUNT = 0
    
    def __init__(self, data=None):
        """Constructs a new Node and increments it attributes."""
        self.init_attributes()
        Node._COUNT += 1
        self._data.update({'id':Node._COUNT})
        if data:
            self._data.update(data)
        self.dict_to_attrs(self._data)
    
    def init_attributes(self):
        self._data = {# local_cnt = num local files, excluding folders.
                      'local_cnt':0,
                      # subdir_cnt = total num files in sub-folders, excluding folders.
                      'subdir_cnt':0,
                      # total_cnt = local_cnt + subdir_cnt (excludes folders).
                      'total_cnt':0,
                      #############################
                      # The following are for counts with folders included.
                      'child_node_cnt':None,
                      'local_plus_child_cnt':None,
                      'subdir_plus_child_cnt':None,
                      'total_plus_child_cnt':None,
                      #############################
                      # Length of the local path.
                      'local_path_length':0,
                      # longest file length = longest filename found including sub-folders.
                      # This will recurse up the tree.
                      'longest_fn_length':0,
                      # longest file length = longest filepath found including sub-folders.
                      # This will recurse up the tree.
                      'longest_fp_length':0,
                      # Number of outliers at the current level.
                      'num_local_outliers1':0,
                      'num_local_outliers2':0,
                      'num_local_outliers3':0,
                      # If current or sub-folders have outliers.
                      # TODO: Phase these out.
                      'has_outliers1':False,
                      'has_outliers2':False,
                      'has_outliers3':False,
                      # Total number of in subdirs.
                      'num_subdir_outliers1':0,
                      'num_subdir_outliers2':0,
                      'num_subdir_outliers3':0,
                      'num_unable_to_shorten':0,
                      # For batching purposes.
                      'batchable':None,
                      # If Node path has been shortened.
                      'shortened':False,
                      'unable_to_shorten':False,
                      # For trimming purposes if path is over file limit.
                      'can_shorten':False,
                      'trimmable':False,
                      'trimmed':False,
                      # If warning was already written to csv.
                      'wrote_over_limit':False,
                      'depth':None,
                      'batch':0,
                      }
    
    def dict_to_attrs(self, d):
        """Helper procedure to unpack dictionary items and convert them into
        class instance attributes.
        
        @param d: Dictionary containing attributes to unpack.
        
        """
        for k,v in d.iteritems():
            setattr(self, k, v)
    
    def update(self, data):
        """Updates instance with the data provided.
        
        @param data: The update data. Must be a dictionary.
        
        """
        self._data.update(data)
        self.dict_to_attrs(data)
    
    def print_attributes(self):
        message = '\n'.join("%s: %s" % item for item in self._data.items())
        log('INFO', logfile, message, print_stdout=True)
    
    def __repr__(self):
        return '%s(id=%s,depth=%s)' % (self.__class__.__name__,self.id,self.depth)
    
    # alias
    print_attr = print_attributes


class Outlier(object):
    
    """Abstract Base Class to hold outlier attributes and functions."""
    
    __metaclass__ = ABCMeta
    # Global counter of objects created.
    _COUNT = 0
    
    def __init__(self, data=None):
        """Constructs a new Node and increments it attributes."""
        # Update global counter.
        Outlier._COUNT += 1
        # Update Class counter.
        self.__class__._COUNT += 1
        self.id = self.__class__._COUNT
        self._data = {'id':self.id,
                      'node_id':None
                      }
        self._data.update(self.get_init_attributes())
        if data:
            self._data.update(data)
        self.dict_to_attrs(self._data)
    
    @abstractmethod
    def get_init_attributes(self):
        """Return a dictionary of specific attributes."""
        return {}
    
    def dict_to_attrs(self, d):
        """Helper procedure to unpack dictionary items and convert them into
        class instance attributes.
        
        @param d: Dictionary containing attributes to unpack.
        
        """
        for k,v in d.iteritems():
            setattr(self, k, v)
    
    def update(self, data):
        """Updates instance with the data provided.
        
        @param data: The update data. Must be a dictionary.
        
        """
        self._data.update(data)
        self.dict_to_attrs(data)
    
    def print_attributes(self):
        message = '\n'.join("%s: %s" % item for item in self._data.items())
        log('INFO', logfile, message, print_stdout=True)
    
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,self.id)
    
    # alias
    print_attr = print_attributes


class Outlier1(Outlier):
    
    """Filename over limit."""
    
    _COUNT = 0
    
    def get_init_attributes(self):
        return {'filename':None}


class Outlier2(Outlier):
    
    """Parent + filename over limit."""
    
    _COUNT = 0
    
    def get_init_attributes(self):
        return {'parent_file':None}


class Outlier3(Outlier):
    
    """Paths over limit, shortened."""
    
    _COUNT = 0
    
    def get_init_attributes(self):
        return {'shortened':None,
                '_file':None}


class Analyzer(object):
    
    """Analyzes a file listing structure."""
    
    _header = None
    _file_line_cnt = 0
    _dirs_within_limit = 0
    _dirs_over_limit = 0
    _pathnames_over_max = 0
    _trimmed = 0
    _unable_to_shorten = 0
    _debug = False
    
    def __init__(self, _file, encoding=None, delimiter=None, path_sep=None,
                 file_limit=None, max_path_length=None,
                 max_parent_file_length=None, max_file_length=None,
                 search_local=False):
        """Constructs a new Analyzer object.
        
        @param _file: The file path to analyze.
        @keyword encoding: The encoding of the file.
                If None, tries to guess encoding type.
                Eg: utf-8
        @keyword delimiter: The field delimiter that appears in the file.
                Defaults to '\t'.
        @keyword path_sep: The path separator that appears in the file.
                Defaults to '\\'.
        @keyword file_limit: The search limit for highest paths that
                satisfies this file limit.
                Defaults to 30,000.
        @keyword max_path_length: The max character length of the absolute
                path name.
                Defaults to 250.
        @keyword max_parent_file_length: The max character length of the file
                and it's parent folder.
                Defaults to 250.
        @keyword max_file_length: The max character length of the file name.
                Defaults to 190.
        @keyword search_local: Searches the local count instead of total for
                file_limit.
        
        """
        self._file = _file
        # For encoding detection.
        if encoding is None:
            self.detect_encoding()
        else:
            self.encoding = encoding
        self.delimiter = delimiter if delimiter else '\t'
        self.path_sep = path_sep if path_sep else '\\'
        try:
            self.file_limit = int(file_limit)
        except (ValueError,TypeError):
            self.file_limit = 30000
        try:
            self.max_path_length = int(max_path_length)
        except (ValueError,TypeError):
            self.max_path_length = 250
        try:
            self.max_parent_file_length = int(max_parent_file_length)
        except (ValueError,TypeError):
            self.max_parent_file_length = 250
        try:
            self.max_file_length = int(max_file_length)
        except (ValueError,TypeError):
            self.max_file_length = 190
        self.search_local = search_local
        # For holding file counts of each directory.
        # {dir_path: {'local_cnt': 0,
        #             'subdir_cnt': 0,
        #             'total_cnt': 0,
        #             'long_file_cnt': 0,
        #             'long_file_parent_cnt':0,
        #             'longest_file_length': 0,
        #             'batch':0
        #             }, ...
        #  '_total':0
        #  }
        self.dir_file_cnts = {'_total':0}
        # For constructing file directory structure.
        self.dir_tree = Tree()
        # Lookup table for tree nodes, one via path, one via id.
        self.nodes_path = {}
        self.nodes_id = {}
        # Lookup table for id -> path
        self.nodes_id_path_ptr = {}
        # nodes_depth = {depth: [node_id, ...], ...}
        self.nodes_depth = {}
        # Outliers.
        # {id: Outlier, ...}
        self.outliers1 = {}
        self.outliers2 = {}
        self.outliers3 = {}
        # For keeping track of path names greater than max_path_length.
        self.max_path_length_cnts = {}
        # Make top level directory
        curdir = os.path.sep.join(self._file.split(os.path.sep)[:-1])
        self.top_dir = os.path.join(curdir,str(int(time.time())))
        if not os.path.exists(self.top_dir):
            os.makedirs(self.top_dir)
        self.timestamp = datetime.now().strftime('ts%Y%m%dT%H%M%S')
        
     
    def process(self):
        """Analyzes the file.
        
        Parsing line item (parse_item_path) will:
        1. Extract file and path.
        2. Create nodes and build directory tree.
        3. Find outliers.
        4. Update node attributes, including counters.
        
        """
        curdir = os.path.sep.join(self._file.split(os.path.sep)[:-1])
        top_dir = os.path.join(curdir,str(int(time.time())))
        if not os.path.exists(top_dir):
            os.makedirs(top_dir)
        message = 'Starting Process'
        log('INFO', logfile, message, print_stdout=True)
        file_gen = self.file_generator()
        # Assume first line is the header.
        header = file_gen.next()
        self._file_line_cnt += 1
        
        for line in file_gen:
            item = self.get_line_item(line, header)
            self.parse_item_path(item)
            self._file_line_cnt += 1
            if self._file_line_cnt % 100000 == 0:
                message = 'Read lines: %s' % (self._file_line_cnt)
                log('INFO', logfile, message, print_stdout=True)
        message = 'Done reading %s lines.' % (self._file_line_cnt)
        log('INFO', logfile, message, print_stdout=True)
        # Update tree node attributes.
        message = ['Number of nodes: %s' % (len(self.nodes_id))]
        message.append('Updating parent node attributes lowest depth up...')
        log('INFO', logfile, '\n'.join(message), print_stdout=True)
        self.depth_first_reverse_update()
        
        # Update child (folder) counters.
        message = 'Updating child (folder) counters...'
        log('INFO', logfile, message, print_stdout=True)
        self.update_node_child_cnts()
        message = 'Preparing results...'
        log('INFO', logfile, message, print_stdout=True)
        self.prepare_batch_results()
        message = 'Finished processing.'
        log('INFO', logfile, message, print_stdout=True)
    
    def get_line_item(self, line, header, use_cache_header=True):
        """Parses a line in a file and returns mapped data
        wrapped into Item object.
        
        @param line: The line from the file.
        @param header: The header line from the file.
        @keyword use_cache_header: Caches the header and reuses cached version.
        
        """
        if not self._header or use_cache_header is False:
            self._header = header.split(self.delimiter)
            # Replace first field of header with 'id' if there is None.
            if 'id' not in self._header and not self._header[0]:
                self._header[0] = u'id'
            # Clean header.
            self._header = [re.sub(r'\s','_',x.strip()) for x in self._header]
        d_line = line.split(self.delimiter)
        # Clean d_line:
        d_line = [x.strip() for x in d_line]
        # Create Item object.
        data = dict(zip(self._header,d_line))
        return Item(data=data)
    
    def parse_item_path(self, item):
        """Parses the Item_Path field.
        
        Increments dir_file_cnt counters as well as adds to the dir_tree
        to construct the file structure.
        
        @attention: Pass in either item or item_path.
        
        @param item: The Item object.
        
        """
        folder = False
        if item.get('Category') == 'Folder':
            folder = True
        item_path = item.Item_Path
        path = None
        # Find Path.  It should always start after the first backslash.
        # The prefix "root folder" in the raw text is not really a
        # folder, but appears to be something prepended by Forensics software,
        # so we strip it in the regex below.
        m = re.search(r'[^\\]+(.*)',item_path.strip())
        if m:
            # Strip leading backslash.
            path = m.group(1).lstrip(self.path_sep)
        if not path:
            message = 'WARNING: Unable to find file path. Item_Path=\'%s\'' % item_path
            log('INFO', logfile, message, print_stdout=True)
            return
        ########### Process folder specifics. ##############
        if folder:
            dir_path = path
            dir_path_list = path.split(self.path_sep)
            if dir_path not in self.nodes_path:
                # Create new node.
                node = Node(data={'depth':len(dir_path_list),
                                  'local_path_length':len(dir_path)})
                self.nodes_path[dir_path] = node
                self.nodes_id[node.id] = node
                self.nodes_id_path_ptr[node.id] = id(dir_path)
                self.insert_nodes_depth(node,dir_path)
                self.insert_nodes(dir_path)
            return
        ####################################################
        # Extract filename.
        _file = path.split(self.path_sep)[-1]
        # Extract parent directory.
        dir_path_list = path.split(self.path_sep)[:-1]
        # Add directory path to the tree.
        self.add(self.dir_tree, dir_path_list)
        # Construct node lookup tables.
        # Check if a new node needs to be created.
        dir_path = self.path_sep.join(dir_path_list)
        # Calculate lengths.
        _file_length = len(_file)
        path_length = len(path)
        try:
            node = self.nodes_path[dir_path]
            # Update lengths.
            if node.longest_fn_length < _file_length:
                node.update({'longest_fn_length':_file_length})
            if node.longest_fp_length < path_length:
                node.update({'longest_fp_length':path_length})
        except KeyError:
            # Create new node and initialize some attributes.
            node = Node()
            depth = len(dir_path_list)
            node.update({'depth':depth,
                         'local_path_length':len(dir_path),
                         'longest_fn_length':_file_length,
                         'longest_fp_length':path_length})
            self.nodes_path[dir_path] = node
            self.nodes_id[node.id] = node
            self.nodes_id_path_ptr[node.id] = id(dir_path)
            self.insert_nodes_depth(node,dir_path)
            # Insert the rest of the parent nodes.
            self.insert_nodes(dir_path)
        # Find outliers.
        self.find_outliers(node, path)
    
    def find_outliers(self, node, path):
        """Find outliers.
        
        @attention: The path should contain the file.
        
        @param node: A Node object.
        @param path: A string of the absolute file path.
        
        """
        found_outlier1 = False
        found_outlier2 = False
        found_outlier3 = False
        _file = path.split(self.path_sep)[-1]
        parent_file = self.path_sep.join(path.split(self.path_sep)[-2:])
        # Outlier 1 - filename over max_file_length.
        if len(_file) > self.max_file_length:
            found_outlier1 = True
            if self._debug:
                message = 'DEBUG: Found Outlier1: file=%s, max_file_length=%s' % (_file,self.max_file_length)
                log('INFO', logfile, message, print_stdout=True)
            if Outlier1._COUNT != 0 and Outlier1._COUNT % 2000 == 0:
                message = 'Outlier1 count: %s' % (Outlier1._COUNT)
                log('INFO', logfile, message, print_stdout=True)
            data = {'node_id':node.id,'filename':_file}
            outlier = Outlier1(data=data)
            self.outliers1.update({outlier.id:outlier})
            node.update({'num_local_outliers1':node.num_local_outliers1 + 1,
                         'has_outliers1':True})
        # Outlier 2 - parent folder + filename over max_parent_file_length
        elif len(parent_file) > self.max_parent_file_length:
            found_outlier2 = True
            if self._debug:
                message = 'DEBUG: Found Outlier2: parent_file=%s, max_parent_file_length=%s' % (parent_file,self.max_parent_file_length)
                log('INFO', logfile, message, print_stdout=True)
            if Outlier2._COUNT != 0 and Outlier2._COUNT % 1000 == 0:
                message = 'Outlier2 count: %s' % (Outlier2._COUNT)
                log('INFO', logfile, message, print_stdout=True)
            data = {'node_id':node.id,'parent_file':parent_file}
            outlier = Outlier2(data=data)
            self.outliers2.update({outlier.id:outlier})
            node.update({'num_local_outliers2':node.num_local_outliers2 + 1,
                         'has_outliers2':True})
        # Outlier 3 - Absolute filepath over max_path_length
        elif len(path) > self.max_path_length:
            found_outlier3 = True
            if self._debug:
                message = 'DEBUG: Found Outlier3: path=%s, max_path_length=%s' % (path,self.max_path_length)
                log('INFO', logfile, message, print_stdout=True)
            shortened = self.shorten_path(path, self.max_path_length)
            unable_to_shorten = False
            if shortened == 'UNABLE_TO_SHORTEN':
                unable_to_shorten = True
            # Check if we need to update data in outliers3.
            if self.need_add_outlier3(node, shortened):
                data = {'node_id':node.id,'shortened':shortened,'_file':_file}
                outlier = Outlier3(data=data)
                self.outliers3.update({node.id:outlier})
            if unable_to_shorten:
                node.update({'unable_to_shorten':unable_to_shorten,
                             'num_unable_to_shorten':1})
            else:
                # Set can_shorten to all nodes in shortened path.
                # This is to help out with trimming the shortened path from
                # the tree later.
                dir_path = self.path_sep.join(path.split(self.path_sep)[:-1])
                self.set_can_shorten(dir_path, shortened)
            node.update({'num_local_outliers3':node.num_local_outliers3 + 1,
                         'has_outliers3':True,
                         'shortened':True})
        
        # Update node local count - only if no outliers were found.
        if not any([found_outlier1,found_outlier2,found_outlier3]):
            node.update({'local_cnt':node.local_cnt + 1,
                         'total_cnt':node.total_cnt + 1})
    
    def update_node_attributes(self, node, has_outliers1=False,
                               has_outliers2=False, has_outliers3=False,
                               longest_fn_length=0, longest_fp_length=0,
                               _update_subdir_cnt=False, _subdir_cnt=0,
                               path=None):
        """Recursively updates nodes and it's parent node attributes.
        
        @param node: The node to update.
        @keyword update_subdir_cnt: Updates the sub-directory count.
                This should be set to true when parents are updated.
                Internal use only.
        @keyword path: The path of the node. This is for efficiency purposes.
        
        """
        # TODO: Needs testing.
        if not path:
            path = self.get_node_path(node)
        # Update attributes.
        # Only update has_outliers if False.
        data = {'has_outliers1':node.has_outliers1 or has_outliers1,
                'has_outliers2':node.has_outliers2 or has_outliers2,
                'has_outliers3':node.has_outliers3 or has_outliers3}
        if node.longest_fn_length < longest_fn_length:
            data.update({'longest_fn_length':longest_fn_length})
        if node.longest_fp_length < longest_fp_length:
            data.update({'longest_fp_length':longest_fp_length})
        if _update_subdir_cnt:
            data.update({'subdir_cnt':node.subdir_cnt + _subdir_cnt,
                         'total_cnt':node.total_cnt + _subdir_cnt})
        node.update(data)
        # Get parent.
        parent_path_list = path.split(self.path_sep)[:-1]
        if not parent_path_list:
            return
        parent_path = self.path_sep.join(parent_path_list)
        parent_node = self.nodes_path[parent_path]
        # Recurse.
        self.update_node_attributes(parent_node,
                                    has_outliers1=has_outliers1,
                                    has_outliers2=has_outliers2,
                                    has_outliers3=has_outliers3,
                                    longest_fn_length=longest_fn_length,
                                    longest_fp_length=longest_fp_length,
                                    _update_subdir_cnt=True,
                                    _subdir_cnt=_subdir_cnt,
                                    path=parent_path)
    
    def update_leaf_node_attributes(self, node, has_outliers1=False,
                                    has_outliers2=False, has_outliers3=False,
                                    longest_fn_length=0, longest_fp_length=0,
                                    _update_subdir_cnt=False, _subdir_cnt=0,
                                    path=None):
        """Recursively updates nodes and it's parent node attributes.
        
        @attention: Should pass in leaf nodes only.
        
        @param node: The node to update.
        @keyword update_subdir_cnt: Updates the sub-directory count.
                This should be set to true when parents are updated.
                Internal use only.
        @keyword path: The path of the node. This is for efficiency purposes.
        
        """
        if not path:
            path = self.get_node_path(node)
        # Update attributes.
        # Only update has_outliers if False.
        data = {'has_outliers1':node.has_outliers1 or has_outliers1,
                'has_outliers2':node.has_outliers2 or has_outliers2,
                'has_outliers3':node.has_outliers3 or has_outliers3}
        if node.longest_fn_length < longest_fn_length:
            data.update({'longest_fn_length':longest_fn_length})
        if node.longest_fp_length < longest_fp_length:
            data.update({'longest_fp_length':longest_fp_length})
        if _update_subdir_cnt:
            data.update({'subdir_cnt':node.subdir_cnt + _subdir_cnt,
                         'total_cnt':node.total_cnt + _subdir_cnt})
        node.update(data)
        # Get parent.
        parent_path_list = path.split(self.path_sep)[:-1]
        if not parent_path_list:
            return
        parent_path = self.path_sep.join(parent_path_list)
        parent_node = self.nodes_path[parent_path]
        # Recurse.
        self.update_node_attributes(parent_node,
                                    has_outliers1=node.has_outliers1,
                                    has_outliers2=node.has_outliers2,
                                    has_outliers3=node.has_outliers3,
                                    longest_fn_length=node.longest_fn_length,
                                    longest_fp_length=node.longest_fp_length,
                                    _update_subdir_cnt=True,
                                    _subdir_cnt=node.total_cnt,
                                    path=parent_path)
    
    def update_parent_attributes(self, node, path=None):
        """Updates only the parent node."""
        if not path:
            path = self.get_node_path(node)
        # Get parent.
        parent_path_list = path.split(self.path_sep)[:-1]
        if not parent_path_list:
            return
        parent_path = self.path_sep.join(parent_path_list)
        parent_node = self.nodes_path[parent_path]
        # Update parent.
        # Only update has_outliers if False.
        data = {'has_outliers1':parent_node.has_outliers1 or node.has_outliers1,
                'has_outliers2':parent_node.has_outliers2 or node.has_outliers2,
                'has_outliers3':parent_node.has_outliers3 or node.has_outliers3}
        if parent_node.longest_fn_length < node.longest_fn_length:
            data.update({'longest_fn_length':node.longest_fn_length})
        if parent_node.longest_fp_length < node.longest_fp_length:
            data.update({'longest_fp_length':node.longest_fp_length})
        data.update({'subdir_cnt':parent_node.subdir_cnt + node.total_cnt,
                     'total_cnt':parent_node.total_cnt + node.total_cnt,
                     'num_unable_to_shorten':(parent_node.num_unable_to_shorten +
                                              node.num_unable_to_shorten)})
        parent_node.update(data)
    
    def prepare_batch_results(self):
        """Writes the batch results to output files."""
        # Prepare csv file and put results in input file's directory.
        batch_file = os.path.join(self.top_dir,'%s_%s.csv' %
                                  ('batch',self.timestamp))
        outliers1_file = os.path.join(self.top_dir,'%s_%s.csv' %
                                      ('outliers1',self.timestamp))
        outliers2_file = os.path.join(self.top_dir,'%s_%s.csv' %
                                      ('outliers2',self.timestamp))
        outliers3_file = os.path.join(self.top_dir,'%s_%s.csv' %
                                      ('shortened',self.timestamp))
        warnings_file = os.path.join(self.top_dir,'%s_%s.csv' %
                                      ('warnings',self.timestamp))
        # Get the file pointer for destination csv files.
        batch_fp = open(batch_file,'w')
        outliers1_fp = open(outliers1_file,'w')
        outliers2_fp = open(outliers2_file,'w')
        outliers3_fp = open(outliers3_file,'w')
        warnings_fp = open(warnings_file,'w')
        # Get the csv writer object.
        batch_writer = csv.writer(batch_fp, quoting=csv.QUOTE_ALL, lineterminator='\n')
        outliers1_writer = csv.writer(outliers1_fp, quoting=csv.QUOTE_ALL, lineterminator='\n')
        outliers2_writer = csv.writer(outliers2_fp, quoting=csv.QUOTE_ALL, lineterminator='\n')
        outliers3_writer = csv.writer(outliers3_fp, quoting=csv.QUOTE_ALL, lineterminator='\n')
        warnings_writer = csv.writer(warnings_fp, quoting=csv.QUOTE_ALL, lineterminator='\n')
        ############## Write to files. #################
        try:
            # Prepare warnings file.
            message = 'Writing warnings file...'
            log('INFO', logfile, message, print_stdout=True)
            warnings_header = ['TAG','Message','File Limit','Path',
                               'Num Local Files','Num Sub-directory Files',
                               'Total Files']
            self.writerow(warnings_writer, warnings_header)
            # Write unable to shorten results.
            for (path, node) in self.search_unable_shorten(self.dir_tree, self.path_sep,
                                                           csv_writer=warnings_writer):
                outlier = self.outliers3[node.id]
                row = ['WARNING','Path cannot be shortened',
                       self.file_limit,
                       self.path_sep.join([path,outlier._file]),
                       node.local_plus_child_cnt,
                       node.subdir_plus_child_cnt,
                       node.total_plus_child_cnt]
                self.writerow(warnings_writer, row)
                self._unable_to_shorten += 1
            ################## Trimmed CSV File ####################
            # Run analysis function first.
            # This will set the trimmable attribute.
            message = 'Writing trimmed file...'
            log('INFO', logfile, message, print_stdout=True)
            self.analyze_trimmable(csv_writer=warnings_writer)
            header = ['Depth',
                      'File Limit',
                      'Directory Path',
                      'Trimmed Folder',
                      'Num Warnings (Cannot Shorten)',
                      'Num Local Files',
                      'Num Sub-directory Files',
                      'Total Files',
                      'Local Path Length',
                      'Longest Filename',
                      'Longest Filepath',
                      'Has Outliers 1',
                      'Has Outliers 2',
                      'Num Local Outliers 1',
                      'Num Local Outliers 2'
                      ]
            self.writerow(outliers3_writer, header)
            # Walk tree and search for highest trimmable.
            for (path, node) in self.search_trimmable(self.dir_tree, self.path_sep,
                                                      csv_writer=warnings_writer):
                row = [node.depth,
                       self.file_limit,
                       path,
                       path.split(self.path_sep)[-1],
                       node.num_unable_to_shorten,
                       node.local_plus_child_cnt,
                       node.subdir_plus_child_cnt,
                       node.total_plus_child_cnt,
                       node.local_path_length,
                       node.longest_fn_length,
                       node.longest_fp_length,
                       node.has_outliers1,
                       node.has_outliers2,
                       node.num_local_outliers1,
                       node.num_local_outliers2
                       ]
                self.writerow(outliers3_writer, row)
                # Mark node trimmed so the search batchable will not go
                # beyond it.
                node.update({'trimmed':True})
                self._trimmed += 1
            ################## Main Batch File ####################
            message = 'Writing main batch file...'
            log('INFO', logfile, message, print_stdout=True)
            # Search for batches.
            # Write the header for results file.
            header = ['Depth',
                      'File Limit',
                      'Directory Path',
                      'Num Local Files',
                      'Num Sub-directory Files',
                      'Total Files',
                      'Local Path Length',
                      'Longest Filename',
                      'Longest Filepath',
                      'Has Outliers 1',
                      'Has Outliers 2',
                      'Has Shortened Paths',
                      'Num Local Outliers 1',
                      'Num Local Outliers 2'
                      ]
            self.writerow(batch_writer, header)
            # Set search function.
            search_fn = self.batch_search
            if self.search_local:
                search_fn = self.search_batchable
                # Run the analysis function first.
                self.analyze_batchable()
            for (path, node) in search_fn(self.dir_tree, self.path_sep,
                                          csv_writer=warnings_writer):
                row = [node.depth,
                       self.file_limit,
                       path,
                       node.local_plus_child_cnt,
                       node.subdir_plus_child_cnt,
                       node.total_plus_child_cnt,
                       node.local_path_length,
                       node.longest_fn_length,
                       node.longest_fp_length,
                       node.has_outliers1,
                       node.has_outliers2,
                       node.has_outliers3,
                       node.num_local_outliers1,
                       node.num_local_outliers2
                       ]
                self.writerow(batch_writer, row)
                self._dirs_within_limit += 1
            ################## Outliers File ####################
            message = 'Writing outlier files...'
            log('INFO', logfile, message, print_stdout=True)
            # Write Outliers 1.
            header = ['Depth','Filename Length','Filename','Directory Path']
            self.writerow(outliers1_writer, header)
            for v in self.outliers1.values():
                node = self.nodes_id[v.node_id]
                path = self.get_node_path(node)
                row = [node.depth,len(v.filename),v.filename,path]
                self.writerow(outliers1_writer, row)
            # Write Outliers 2.
            header = ['Depth','Parent File Path Length','Parent File Path',
                      'Directory Path']
            self.writerow(outliers2_writer, header)
            for v in self.outliers2.values():
                node = self.nodes_id[v.node_id]
                path = self.get_node_path(node)
                row = [node.depth,len(v.parent_file),v.parent_file,path]
                self.writerow(outliers2_writer, row)
        except Exception:
            raise
        finally:
            batch_fp.flush()
            batch_fp.close()
            outliers1_fp.flush()
            outliers1_fp.close()
            outliers2_fp.flush()
            outliers2_fp.close()
            outliers3_fp.flush()
            outliers3_fp.close()
            warnings_fp.flush()
            warnings_fp.close()
        message = 'Results saved to file: %s' % batch_file
        log('INFO', logfile, message, print_stdout=True)
        
    def shorten_path(self, path, length, get_parent_path=True):
        """Recursive function to shorten the path to the desired length.
        
        @param path: The absolute pathname.
        @param length: The length to shorten to.
        @keyword get_parent_path: Strips the filename at the end of returned
                result.
        @return: The shortened path.
        
        """
        if len(path) <= length:
            if get_parent_path:
                path_list = path.split(self.path_sep)
                parent_path_list = path_list[:-1]
                if not parent_path_list or len(path_list) == 1:
                    return 'UNABLE_TO_SHORTEN'
                else:
                    return self.path_sep.join(parent_path_list)
            else:
                return path
        return self.shorten_path(self.path_sep.join(path.split(self.path_sep)[1:]),
                                 length)
    
    def need_add_outlier3(self, node, shortened_path):
        """Checks if the shortened_path need to be added or updated to
        outlier3.
        
        @param node: The Node object.
        @param shortened_path: The shortened path to check.
        @return: False if node is not found in outlier3 or True if the
                shortened path is shorter than the stored path.
        
        """
        if node.id not in self.outliers3:
            return True
        if shortened_path < self.outliers3[node.id].shortened:
            return True
    
    def set_can_shorten(self, path, shortened_path):
        """Sets the can_shorten attribute to all nodes that can be trimmed.
        
        This is to help with the trimming algorithm.
        
        @param path: The original absolute path of the node.
        @param shortened_path: The shortened path that can be trimmed.
        
        """
        t_path = path.split(self.path_sep)
        t_shortened = shortened_path.split(self.path_sep)
        while t_shortened:
            ts_folder = t_shortened.pop()
            tp_folder = t_path.pop()
            if ts_folder != tp_folder:
                message = ('WARNING: can_shorten: Unexpected values %s != %s' %
                       (ts_folder,tp_folder))
                log('INFO', logfile, message, print_stdout=True)
            tp_path = self.path_sep.join(t_path + [tp_folder])
            if self._debug:
                message = ('can_shorten: Updating node \'%s\' to can_shorten' %
                       (tp_path))
                log('INFO', logfile, message, print_stdout=True)
            node = self.nodes_path[tp_path]
            node.update({'can_shorten':True})
    
    def get_node_path(self, node):
        """Looks up the node path.
        
        @param node: The Node to lookup.
        @return: The absolute path of the node.
        
        """
        return di(self.nodes_id_path_ptr[node.id])
    
    def insert_nodes(self, dir_path):
        """Recursively creates and inserts nodes and it's parents to the
        lookup table if they do not already exist.
        
        @param dir_path: The directory path to insert (without filename).
        
        """
        if not dir_path:
            return
        dir_path_list = dir_path.split(self.path_sep)
        if dir_path not in self.nodes_path:
            node = Node(data={'depth':len(dir_path_list),
                              'local_path_length':len(dir_path)})
            self.nodes_path[dir_path] = node
            self.nodes_id[node.id] = node
            self.nodes_id_path_ptr[node.id] = id(dir_path)
            self.insert_nodes_depth(node,dir_path)
        self.insert_nodes(self.path_sep.join(dir_path_list[:-1]))
    
    def insert_nodes_depth(self, node, path):
        """Inserts node ids into depth lookup table."""
        if node.depth not in self.nodes_depth:
            self.nodes_depth[node.depth] = [node.id]
        elif node.id not in self.nodes_depth[node.depth]:
            self.nodes_depth[node.depth].append(node.id)
    
    def update_tree_leaf_node_attributes(self):
        """Walks the tree and recursively updates leaf node attributes and its
        parents.
        
        """
        cnt = 0
        leaf_node_cnt = 0
        for (dir_path, tree_node) in self.walk_paths(self.dir_tree, ''):
            child_node_cnt = self.count_child_nodes(tree_node)
            if child_node_cnt == 0:
                leaf_node_cnt += 1
        message = 'Number of leaf nodes: %s' % (leaf_node_cnt)
        log('INFO', logfile, message, print_stdout=True)
        for (dir_path, tree_node) in self.walk_paths(self.dir_tree, ''):
            child_node_cnt = self.count_child_nodes(tree_node)
            if child_node_cnt != 0:
                continue
            node = self.nodes_path[dir_path.lstrip(self.path_sep)]
            self.update_leaf_node_attributes(node,
                                             has_outliers1=node.has_outliers1,
                                             has_outliers2=node.has_outliers2,
                                             has_outliers3=node.has_outliers3,
                                             longest_fn_length=node.longest_fn_length,
                                             longest_fp_length=node.longest_fp_length,
                                             _subdir_cnt=node.total_cnt
                                             )
            cnt += 1
            if cnt % 500 == 0:
                message = 'updated %s leaf nodes.' % (cnt)
                log('INFO', logfile, message, print_stdout=True)
        message = 'finished updating %s leaf nodes.' % (cnt)
        log('INFO', logfile, message, print_stdout=True)
        
    def update_leaf_node_parent_attributes(self):
        """Walks the tree and updates leaf node attributes and its
        parents.
        
        """
        cnt = 0
        leaf_node_cnt = 0
        for (dir_path, tree_node) in self.walk_paths(self.dir_tree, ''):
            child_node_cnt = self.count_child_nodes(tree_node)
            if child_node_cnt == 0:
                leaf_node_cnt += 1
        message = 'Number of leaf nodes: %s' % (leaf_node_cnt)
        log('INFO', logfile, message, print_stdout=True)
        for (t_dir_path, tree_node) in self.walk_paths(self.dir_tree, ''):
            child_node_cnt = self.count_child_nodes(tree_node)
            if child_node_cnt != 0:
                continue
            dir_path = t_dir_path.lstrip(self.path_sep)
            node = self.nodes_path[dir_path]
            self.update_node_parent_attributes(node, path=dir_path)
            cnt += 1
            if cnt % 50000 == 0:
                message = 'updated %s leaf nodes.' % (cnt)
                log('INFO', logfile, message, print_stdout=True)
        message = 'Finished updating %s leaf nodes.' % (cnt)
        log('INFO', logfile, message, print_stdout=True)
    
    def update_node_parent_attributes(self, node, path=None):
        """Update node's parent attributes.
        
        @warning: Flawed algorithm, don't use.
        
        @param node: The Node object.
        @param path: The directory path of the node.
        
        """
        if not path:
            path = self.get_node_path(node)
        # Prepare data to update.
        has_outliers1 = node.has_outliers1
        has_outliers2 = node.has_outliers2
        has_outliers3 = node.has_outliers3
        longest_fn_length = node.longest_fn_length
        longest_fp_length = node.longest_fp_length
        total_cnt = node.total_cnt
        # Update parent.
        parent_path_list = path.split(self.path_sep)[:-1]
        while parent_path_list:
            parent_path = self.path_sep.join(parent_path_list)
            parent_node = self.nodes_path[parent_path]
            # Update data for parent.
            data = {'has_outliers1':parent_node.has_outliers1 or has_outliers1,
                    'has_outliers2':parent_node.has_outliers2 or has_outliers2,
                    'has_outliers3':parent_node.has_outliers3 or has_outliers3}
            if parent_node.longest_fn_length < longest_fn_length:
                data.update({'longest_fn_length':longest_fn_length})
            if parent_node.longest_fp_length < longest_fp_length:
                data.update({'longest_fp_length':longest_fp_length})
            data.update({'subdir_cnt':parent_node.subdir_cnt + total_cnt,
                         'total_cnt':parent_node.total_cnt + total_cnt})
            parent_node.update(data)
            # Update data to update for next parent.
            has_outliers1 = parent_node.has_outliers1
            has_outliers2 = parent_node.has_outliers2
            has_outliers3 = parent_node.has_outliers3
            longest_fn_length = parent_node.longest_fn_length
            longest_fp_length = parent_node.longest_fp_length
            total_cnt = parent_node.total_cnt
            # Pop last element off list.
            parent_path_list.pop()
    
    def update_node_child_cnts(self):
        """Updates the node child counters. This translates to folder and
        sub-folder counts.
        
        """
        for (dir_path, tree_node) in self.walk_paths(self.dir_tree, ''):
            child_node_cnt = self.count_child_nodes(tree_node)
            node = self.nodes_path[dir_path.lstrip(self.path_sep)]
            local_plus_child_cnt = node.local_cnt + child_node_cnt
            subdir_plus_child_cnt = node.subdir_cnt + child_node_cnt
            total_plus_child_cnt = local_plus_child_cnt + subdir_plus_child_cnt
            node.update({'child_node_cnt':child_node_cnt,
                         'local_plus_child_cnt':local_plus_child_cnt,
                         'subdir_plus_child_cnt':subdir_plus_child_cnt,
                         'total_plus_child_cnt':total_plus_child_cnt})
    
    def writerow(self,csv_writer,row):
        """Encode everything to utf-8 before writing to csv.
        
        @param csv_writer: The csv writer object, fetched from csv_writer().
        @param row: The row to write.
        
        """
        encoded_row = [x.encode('utf-8') if isinstance(x,str) or
                       isinstance(x,unicode) else x for x in row]
        try:
            csv_writer.writerow(encoded_row)
        except UnicodeEncodeError:
            message = ' DEBUG: encoded_row=%s' % encoded_row
            log('WARNING', logfile, message, print_stdout=True)
            raise
    
    def file_generator(self):
        """Generator object for file.
        
        Each call to next() yields one line of content from the file.
        
        """
        with codecs.open(self._file, encoding=self.encoding) as f:
            for line in f:
                yield line
    
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
    
    def search(self, node, path, csv_writer=None):
        """Depth First Search Tree Walk.
        
        Searches for the highest paths that satisfies the maximum files.
        
        @keyword csv_writer: If provided will write to csv file.
        
        """
        if not node:
            if (path in self.dir_file_cnts and
                self.dir_file_cnts[path]['local_cnt'] > self.file_limit):
                if csv_writer:
                    v = self.dir_file_cnts[path]
                    row = ['WARNING','Directory local file count over limit',
                           self.file_limit,path,
                           v['local_cnt'],v['subdir_cnt'],v['total_cnt']]
                    self.writerow(csv_writer, row)
                else:
                    message = (' WARNING: DIRECTORY LOCAL FILE COUNT OVER LIMIT (%s): %s, files: %s' %
                           (self.file_limit, path, self.dir_file_cnts[path]))
                    log('INFO', logfile, message, print_stdout=True)
                self._dirs_over_limit += 1
            return
        for k in node.keys():
            # Check file count for path.
            t_path = self.path_sep.join([path,k]).lstrip(self.path_sep)
            if self.dir_file_cnts[t_path]['total_cnt'] <= self.file_limit:
                yield t_path
                continue
            if self.dir_file_cnts[t_path]['local_cnt'] > self.file_limit:
                if csv_writer:
                    v = self.dir_file_cnts[t_path]
                    row = ['WARNING','Directory local file count over limit',
                           self.file_limit,t_path,
                           v['local_cnt'],v['subdir_cnt'],v['total_cnt']]
                    self.writerow(csv_writer, row)
                else:
                    message = (' WARNING: DIRECTORY LOCAL FILE COUNT OVER LIMIT (%s): %s, files: %s' %
                           (self.file_limit,t_path, self.dir_file_cnts[t_path]))
                    log('INFO', logfile, message, print_stdout=True)
                self._dirs_over_limit += 1
            for n in self.search(node[k], t_path, csv_writer=csv_writer):
                yield n
    
    def batch_search(self, node, path, csv_writer=None):
        """Depth First Search Tree Walk.
        
        Searches for the highest paths that satisfies the maximum files.
        
        @param node: The tree node.
        @param path: The path of the node.
        @keyword csv_writer: If provided will write to csv file.
        
        """
        if not node:
            if path not in self.nodes_path:
                return
            node_obj = self.nodes_path[path]
            if node_obj.local_plus_child_cnt > self.file_limit:
                if csv_writer:
                    row = ['WARNING','Directory local file count over limit',
                           self.file_limit,path,
                           node_obj.local_plus_child_cnt,
                           node_obj.subdir_plus_child_cnt,
                           node_obj.total_plus_child_cnt]
                    self.writerow(csv_writer, row)
                else:
                    message = (' WARNING: DIRECTORY LOCAL FILE COUNT OVER LIMIT (%s): %s, files: %s' %
                           (self.file_limit, path, node_obj.local_plus_child_cnt))
                    log('INFO', logfile, message, print_stdout=True)
                self._dirs_over_limit += 1
            return
        for k in node.keys():
            # Check file count for path.
            t_path = self.path_sep.join([path,k]).lstrip(self.path_sep)
            node_obj = self.nodes_path[t_path]
            if self.search_local:
                if node_obj.local_plus_child_cnt <= self.file_limit:
                    yield (t_path, node_obj)
            else:
                if node_obj.total_plus_child_cnt <= self.file_limit:
                    yield (t_path, node_obj)
                    continue
            if node_obj.local_plus_child_cnt > self.file_limit:
                if csv_writer:
                    row = ['WARNING','Directory local file count over limit',
                           self.file_limit,t_path,
                           node_obj.local_plus_child_cnt,
                           node_obj.subdir_plus_child_cnt,
                           node_obj.total_plus_child_cnt]
                    self.writerow(csv_writer, row)
                else:
                    message = (' WARNING: DIRECTORY LOCAL FILE COUNT OVER LIMIT (%s): %s, files: %s' %
                           (self.file_limit, t_path, node_obj.local_plus_child_cnt))
                    log('INFO', logfile, message, print_stdout=True)
                self._dirs_over_limit += 1
            for (p,n) in self.batch_search(node[k], t_path, csv_writer=csv_writer):
                yield (p,n)
    
    def search_batchable(self, node, path, csv_writer=None):
        """Depth First Search Tree Walk.
        
        Searches for a returns highest nodes that are marked batchable.
        
        @attention: analyze_batchable should be run first.
        
        @param node: The tree node.
        @param path: The path of the node.
        @keyword csv_writer: If provided will write to csv file.
        
        """
        if not node:
            if path not in self.nodes_path:
                return
            node_obj = self.nodes_path[path]
            if node_obj.batchable is False and node_obj.wrote_over_limit is False:
                if csv_writer:
                    row = ['WARNING','Directory local file count over limit',
                           self.file_limit,path,
                           node_obj.local_plus_child_cnt,
                           node_obj.subdir_plus_child_cnt,
                           node_obj.total_plus_child_cnt]
                    self.writerow(csv_writer, row)
                else:
                    message = (' WARNING: DIRECTORY LOCAL FILE COUNT OVER LIMIT (%s): %s, files: %s' %
                           (self.file_limit, path, node_obj.local_plus_child_cnt))
                    log('INFO', logfile, message, print_stdout=True)
                self._dirs_over_limit += 1
                node_obj.update({'wrote_over_limit':True})
            return
        # Check if node was already trimmed.
        if path and path != self.path_sep:
            node_obj = self.nodes_path[path]
            if node_obj.trimmed:
                return
        for k in node.keys():
            # Check if node is batchable.
            t_path = self.path_sep.join([path,k]).lstrip(self.path_sep)
            node_obj = self.nodes_path[t_path]
            if node_obj.trimmed:
                # Skip going down this path if node_obj is trimmed.
                continue
            if node_obj.batchable:
                yield (t_path, node_obj)
                continue
            if node_obj.batchable is False and node_obj.wrote_over_limit is False:
                if csv_writer:
                    row = ['WARNING','Directory local file count over limit',
                           self.file_limit,t_path,
                           node_obj.local_plus_child_cnt,
                           node_obj.subdir_plus_child_cnt,
                           node_obj.total_plus_child_cnt]
                    self.writerow(csv_writer, row)
                else:
                    message = (' WARNING: DIRECTORY LOCAL FILE COUNT OVER LIMIT (%s): %s, files: %s' %
                           (self.file_limit, t_path, node_obj.local_plus_child_cnt))
                    log('INFO', logfile, message, print_stdout=True)
                self._dirs_over_limit += 1
                node_obj.update({'wrote_over_limit':True})
            for (p,n) in self.search_batchable(node[k], t_path, csv_writer=csv_writer):
                yield (p,n)
    
    def search_trimmable(self, node, path, csv_writer=None):
        """Depth First Search Tree Walk.
        
        Searches for and returns highest nodes that are marked trimmable
        and also within the file limit.
        
        @param node: The tree node.
        @param path: The path of the node.
        @keyword csv_writer: If provided will write to csv file.
        
        """
        if not node:
            return
        for k in node.keys():
            # Check if node is trimmable.
            t_path = self.path_sep.join([path,k]).lstrip(self.path_sep)
            node_obj = self.nodes_path[t_path]
            if node_obj.trimmable:
                yield (t_path, node_obj)
                continue
            for (p,n) in self.search_trimmable(node[k], t_path, csv_writer=csv_writer):
                yield (p,n)
    
    def search_unable_shorten(self, node, path, csv_writer=None):
        """Depth First Search Tree Walk.
        
        Searches for and returns highest nodes that are marked unable_to_shorten.
        
        @param node: The tree node.
        @param path: The path of the node.
        @keyword csv_writer: If provided will write to csv file.
        
        """
        if not node:
            return
        for k in node.keys():
            t_path = self.path_sep.join([path,k]).lstrip(self.path_sep)
            node_obj = self.nodes_path[t_path]
            # Check if node is unable to be shortened.
            if node_obj.unable_to_shorten:
                yield (t_path, node_obj)
                continue
            for (p,n) in self.search_unable_shorten(node[k], t_path, csv_writer=csv_writer):
                yield (p,n)
    
    def analyze_batchable(self):
        """Starts at leaf nodes and reverse search for local file count.
        Marks node batchable True or False depending on findings.
        
        Ignores empty directories (unless shortened).
        
        """
        message = 'Analyzing for batchable nodes.'
        log('INFO', logfile, message, print_stdout=True)
        cnt = 0
        for (dir_path, tree_node) in self.walk_paths(self.dir_tree, ''):
            child_node_cnt = self.count_child_nodes(tree_node)
            if child_node_cnt != 0:
                continue
            # Loop through path list.
            path_list = dir_path.lstrip(self.path_sep).split(self.path_sep)
            while path_list:
                path = self.path_sep.join(path_list)
                node = self.nodes_path[path]
                if node.local_plus_child_cnt == 0 and not node.shortened:
                    # Skip empty folders that are not shortened.
                    path_list.pop()
                    continue
                if node.batchable is False:
                    # Stop with this leaf node here.
                    break
                else:
                    if node.local_plus_child_cnt <= self.file_limit:
                        node.update({'batchable':True})
                    else:
                        node.update({'batchable':False})
                        break
                # Pop last element off list.
                path_list.pop()
            cnt += 1
            if cnt % 50000 == 0:
                message = 'analyze_batchable: updated %s nodes.' % (cnt)
                log('INFO', logfile, message, print_stdout=True)
        message = 'analyze_batchable: Finished updating %s nodes.' % (cnt)
        log('INFO', logfile, message, print_stdout=True)
    
    def analyze_trimmable(self, csv_writer=None):
        """Starts at leaf nodes and reverse search for local file count.
        Marks node trimmable True or False depending on findings.
        
        Ignores empty directories (unless shortened).
        
        @param csv_writer: CSV Writer for warnings file.
        
        """
        message = 'Analyzing for trimmable nodes.'
        log('INFO', logfile, message, print_stdout=True)
        cnt = 0
        leaf_node_cnt = 0
        for (dir_path, tree_node) in self.walk_paths(self.dir_tree, ''):
            child_node_cnt = self.count_child_nodes(tree_node)
            if child_node_cnt == 0:
                leaf_node_cnt += 1
        message = 'Number of leaf nodes: %s' % (leaf_node_cnt)
        log('INFO', logfile, message, print_stdout=True)
        for (dir_path, tree_node) in self.walk_paths(self.dir_tree, ''):
            child_node_cnt = self.count_child_nodes(tree_node)
            if child_node_cnt != 0:
                continue
            # Loop through path list.
            path_list = dir_path.lstrip(self.path_sep).split(self.path_sep)
            while path_list:
                path = self.path_sep.join(path_list)
                node = self.nodes_path[path]
                if node.local_plus_child_cnt == 0 and not node.shortened:
                    # Skip empty folders that are not shortened.
                    path_list.pop()
                    continue
                if node.can_shorten is False:
                    # Stop with this leaf node here.
                    break
                else:
                    if node.local_plus_child_cnt <= self.file_limit:
                        node.update({'trimmable':True})
                    else:
                        node.update({'trimmable':False})
                        if node.wrote_over_limit is False:
                            if csv_writer:
                                row = ['WARNING','Directory local file count over limit',
                                       self.file_limit,path,
                                       node.local_plus_child_cnt,
                                       node.subdir_plus_child_cnt,
                                       node.total_plus_child_cnt]
                                self.writerow(csv_writer, row)
                            else:
                                message = (' WARNING: DIRECTORY LOCAL FILE COUNT OVER LIMIT (%s): %s, files: %s' %
                                       (self.file_limit, path, node.local_plus_child_cnt))
                                log('INFO', logfile, message, print_stdout=True)
                            self._dirs_over_limit += 1
                            node.update({'wrote_over_limit':True})
                        break
                # Pop last element off list.
                path_list.pop()
            cnt += 1
            if cnt % 50000 == 0:
                message = 'analyze_trimmable: updated %s nodes.' % (cnt)
                log('INFO', logfile, message, print_stdout=True)
        message = 'analyze_trimmable: Finished updating %s nodes.' % (cnt)
        log('INFO', logfile, message, print_stdout=True)
    
    def add(self, t, path):
        """Adds a path to the tree.
        
        @param t: The tree.
        @param path: A list of the path structure.
        
        """
        for node in path:
            t = t[node]
    
    def count_nodes(self, node):
        """Counts the number of nodes including top level node."""
        count = 1
        for child in node.values():
            count += self.count_nodes(child)
        return count
    
    def count_child_nodes(self, node):
        """Counts the number of child nodes for a given node."""
        return self.count_nodes(node) - 1
    
    def walk_paths(self, node, path):
        """Depth First Search tree walk that returns path and corresponding
        tree node in a tuple.
        
        @attention: node here is not Node object!
        
        @param node: The Tree() node.
        @param path: The current path. Usually start with an empty string.
        
        """
        if not node:
            return
        for k in node.keys():
            # Check
            yield (path + '\\' + k, node[k])
            for (p,n) in self.walk_paths(node[k], path + '\\' + k):
                yield (p,n)
    
    def depth_first_reverse_update(self):
        cnt = 0
        sorted_keys = sorted(self.nodes_depth.keys(), reverse=True)
        message = 'Depth of nodes: %s' % (sorted_keys)
        log('INFO', logfile, message, print_stdout=True)
        for k in sorted_keys:
            v = self.nodes_depth[k]
            if self._debug:
                message = 'len of depth %s: %s' % (k, len(v))
                log('INFO', logfile, message, print_stdout=True)
            for i in v:
                node = self.nodes_id[i]
                self.update_parent_attributes(node)
                cnt += 1
                if cnt % 50000 == 0:
                    message = 'updated %s nodes.' % (cnt)
                    log('INFO', logfile, message, print_stdout=True)
        message = 'Finished updating %s nodes.' % (cnt)
        log('INFO', logfile, message, print_stdout=True)
    
    def set_debug(self, debug):
        self._debug = debug


def Tree():
    """Tree Data Structure implementation."""
    return defaultdict(Tree)


def di(obj_id):
    """Reverse of id() function."""
    return _ctypes.PyObj_FromPtr(obj_id)


def log(logtype, logfile, message, print_stdout=True, TAG=None):
    """Log message to a log file.
    
    @param logtype: [INFO|ERROR] The message type.
    @param logfile: The logfile to print to.
    @param message: A string to print to the log.
    @keyword print_stdout: If True, message will be printed to stdout,
            unless logtype is 'ERROR', then the message will be printed
            to stderr (default=True).
    @keyword TAG: TAG directly prefixed to message, but after standard
            headers.
    
    """
    ct = time.time()
    msecs = (ct - long(ct)) * 1000
    t = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    date = "%s,%03d" % (t, msecs)
    
    prefix = '%s %s PID[%s]: ' % \
        (date, logtype, os.getpid())
    
    # Clean message
    logtxt = message.strip()
    # Add TAG if necessary.
    if TAG:
        logtxt = re.sub('^', str(TAG) + ': ', logtxt)
    # Add prefix to start of message.
    logtxt = re.sub('^', str(prefix), logtxt)
    logtxt = re.sub('\r\n', os.linesep + str(prefix), logtxt)
    logtxt = re.sub('\n', os.linesep + str(prefix), logtxt)
    logtxt = re.sub('\r', '\r' + str(prefix), logtxt)
    
    with open(logfile, 'a') as f:
        f.write(logtxt.rstrip() + os.linesep)
    
    if print_stdout:
        if logtype == 'ERROR':
            print >>sys.stderr, message
        else:
            print message


###############################################################################
# Main.
###############################################################################
def usage():
    """Print usage info."""
    program_name = os.path.basename(sys.argv[0])
    message = ['Usage: %s <options>...' % program_name]
    message.append(dedent('''
    Required argument(s):
      -f <FILE_PATH>, --file=<FILE_PATH>
            The file to analyze.
    
    Optional argument(s):
      -e <ENCODING>, --encoding=<ENCODING>
            The encoding of the file to analyze.
            Tries to detect first, otherwise defaults to utf-8.
            Eg: -e utf-8
      -d <DELIMTER>, --delimiter=<DELIMITER>
            The field delimiter that appears in the file to analyze.
            Defaults to '\\t' (tab).
            Eg: -d '\\t'
      -s <PATH_SEPARATOR>, --path-separator=<PATH_SEPARATOR>
            The path separator that appears in the file to analyze.
            Deaults to the Windows style '\\' (backslash).
            Eg: -s '\\'
      -l <FILE_LIMIT>, --file-limit=<FILE_LIMIT>
            The search limit for highest paths that satisfies this file limit.
            Defaults to 30000.
      -m <MAX_PATH_LENGTH>, --max-path-length=<MAX_PATH_LENGTH>
            The max character length of the pathname to check.
            Defaults to 250.
      -n <MAX_FILE_LENGTH>, --max-file-length=<MAX_FILE_LENGTH>
            The max character length of the filename to check.
            Defaults to 190.
      -p <MAX_PARENT_FILE_LENGTH>, --max-pf-length=<MAX_PARENT_FILE_LENGTH>
            The max character length of the file and it's parent folder.
            Defaults to 250.
      --search-local
            Searches the local file counts instead of total count for FILE_LIMIT.
      -h, --help
            Displays this help screen.
    '''))
    log('INFO', logfile, '\n'.join(message), print_stdout=True)


def handle_args():
    """Handle script's command line script_args."""
    global script_args
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f:e:d:s:l:m:n:p:h',
                                   ['file=','encoding=','delimiter=',
                                    'path-separator=','file-limit=',
                                    'max-path-length=','max-file-length=',
                                    'max-pf-length=','search-local',
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
        elif o == '-d' or o == '--delimiter':
            script_args['delimiter'] = a
        elif o == '-s' or o == '--path-separator':
            script_args['path-separator'] = a
        elif o == '-l' or o == '--file-limit':
            script_args['file-limit'] = a
        elif o == '-m' or o == '--max-path-length':
            script_args['max-path-length'] = a
        elif o == '-n' or o == '--max-file-length':
            script_args['max-file-length'] = a
        elif o == '-p' or o == '--max-pf-length':
            script_args['max-pf-length'] = a
        elif o == '--search-local':
            script_args['search-local'] = True
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
    if 'file' not in script_args:
        print >>sys.stderr, 'ERROR: Missing argument(s).'
        usage()
        sys.exit(2)


def main():
    global script_args
    handle_args()
    
    analyzer = Analyzer(_file=script_args['file'],
                        encoding=script_args.get('encoding'),
                        delimiter=script_args.get('delimiter'),
                        path_sep=script_args.get('path-separator'),
                        file_limit=script_args.get('file-limit'),
                        max_path_length=script_args.get('max-path-length'),
                        max_parent_file_length=script_args.get('max-pf-length'),
                        max_file_length=script_args.get('max-file-length'),
                        search_local=script_args.get('search-local',False)
                        )
    global logfile
    logfile = os.path.join(analyzer.top_dir,'%s_%s.txt' %
                                  ('log',analyzer.timestamp))
    message = ' '.join(sys.argv)
    log('INFO', logfile, message, print_stdout=False)
    analyzer.process()
    
    message = ['\nInfo:']
    message.append('========')
    message.append('File: %s' % script_args['file'])
    message.append('Encoding: %s' % analyzer.encoding)
    message.append("Delimiter: '%s'" % analyzer.delimiter)
    message.append("Path Separator: '%s'" % analyzer.path_sep)
    message.append('File Limit: %s' % analyzer.file_limit)
    message.append('Max Path Length: %s' % analyzer.max_path_length)
    message.append('Max Parent File Length: %s' % analyzer.max_parent_file_length)
    message.append('Max File Length: %s' % analyzer.max_file_length)
    message.append('\nResults:')
    message.append('========')
    message.append('Processed %s lines.' % analyzer._file_line_cnt)
    message.append('Num Batches: %s' % analyzer._dirs_within_limit)
    message.append('Num Outliers 1: %s' % len(analyzer.outliers1))
    message.append('Num Outliers 2: %s' % len(analyzer.outliers2))
    message.append('Num Trimmed (Shortened) Paths: %s' % analyzer._trimmed)
    message.append('Num Directories over file limit: %s' % analyzer._dirs_over_limit)
    message.append('Num Paths over max path length but cannot shorten: %s' % analyzer._unable_to_shorten)
    log('INFO', logfile, '\n'.join(message), print_stdout=True)

if __name__ == '__main__':
    main()
