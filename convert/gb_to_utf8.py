#!/usr/bin/python
#-*- coding: utf-8 -*-

# Filename: gb_to_utf8.py dirname
# GB2312 to UTF-8

import os
import os.path
import sys

def convert_code(filename, in_enc = 'gbk', out_enc = 'utf-8'):
    """
    
    Arguments:
    - `filename`:
    - `in_enc`:
    - `out_enc`:
    """
    try:
        print 'convert:', filename
        f = open(filename, 'rb')
        data = f.read()
        f.close()
        new_contert = data.decode(in_enc, 'ignore')
        new_data = new_contert.encode(out_enc, 'ignore')
        f = open(filename, 'w')
        f.write(new_data)
        f.close()
        print 'Success:', filename, 'converted from', in_enc, 'to', out_enc, '!'
    except:
        print 'Error, fail convert:', filename
        print

def get_filename_list(dirname, suffix = ''):
    """
    
    Arguments:
    - `dirname`:
    - `suffix`:
    """
    flist = []
    for root, dirs, files in os.walk(dirname):
        for name in files:
            if name.endswith(suffix):
                flist.append(os.path.join(root, name))
    return flist

def explore(name):
    """
    """
    dirname = os.path.join(os.getcwd(), name)
    flist = get_filename_list(dirname)
    for fname in flist:
        convert_code(fname)
    print 'Finished!'

def main(argv):
    """
    """
    explore(argv)

if __name__ == '__main__':
    main(sys.argv[1])
