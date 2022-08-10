#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = []
__version__ = 0.1
__date__ = '2022-07-12'
__updated__ = '2022-07-12'

import copy
import json
import sys
import os
import csv
from optparse import OptionParser

EDGE_LABEL = "relationship"
VERTEX_LABEL = "node"

class Json2CSV:
    def __init__(self):
        self.get_node_table = []
        self.get_edge_table = []
        self.id_map = {}
        self.type_map = {type('123'): 'STRING', type(12): 'INT', type(1.0): 'FLOAT'}

    def getJsonData(self, fname):
        with open(fname, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                data = json.loads((line))
                if data['type'] == VERTEX_LABEL:
                    labelV = data['labels']
                    if labelV[0] not in self.get_node_table:
                        self.get_node_table.append(labelV[0])
                    # 构建一个id的映射
                    self.id_map[data['id']] = data['properties']['id']
                elif data['type'] == EDGE_LABEL:
                    labelE = data['label']
                    if labelE not in self.get_edge_table:
                        self.get_edge_table.append(labelE)

    def generateCreateTableSQL(self, fname, oname):
        type_of_properties = {}
        type_of_one_properties = []

        with open(fname, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                data = json.loads((line))
                type_of_one_properties.clear()
                # create node table
                if data['type'] == VERTEX_LABEL:
                    labelV = data['labels'][0]
                    if labelV in self.get_node_table:
                        for k, v in data['properties'].items():
                            if k == 'id':
                                type_of_one_properties.insert(0, [k, self.type_map[type(v)]])
                            else:
                                type_of_one_properties.append([k, self.type_map[type(v)]])
                        copy_node_type = copy.deepcopy(type_of_one_properties)
                        type_of_properties[labelV] = copy_node_type
                        self.get_node_table.remove(labelV)

                # create edge table
                elif data['type'] == EDGE_LABEL:
                    labelE = data['label']
                    if labelE in self.get_edge_table:

                        type_of_one_properties.append(
                            ['from_id', self.type_map[type(self.id_map[data['start']['id']])]])
                        type_of_one_properties.append(
                            ['to_id', self.type_map[type(self.id_map[data['end']['id']])]])

                        for k, v in data['properties'].items():
                            if k == 'id':
                                pass
                            else:
                                type_of_one_properties.append([k, self.type_map[type(v)]])
                        copy_edge_type = copy.deepcopy(type_of_one_properties)
                        type_of_properties[labelE] = copy_edge_type
                        self.get_edge_table.remove(labelE)

        # write into txt file
        with open(oname, 'w') as outFile:
            for tk, tv in type_of_properties.items():
                prop = ""
                for i in range(len(tv)):
                    if i == 0:
                        prop += tv[i][0] + ' ' + tv[i][1]
                    else:
                        prop += ', ' + tv[i][0] + ' ' + tv[i][1]
                create_table_sql = "CREATE TABLE IF NOT EXISTS " + tk + \
                                   "(" \
                                   + prop + \
                                   ") " \
                                   "PARTITIONED BY (ds STRING);\n"
                outFile.write(create_table_sql)

def main(argv=None):

    '''Command line options.'''

    program_name = os.path.basename(sys.argv[0])
    program_version = "v0.1"
    program_build_date = "%s" % __updated__

    program_version_string = '%%prog %s (%s)' % (
        program_version, program_build_date)
    program_longdesc = ("A utility python script to generate graph schema for Graph Compute")

    if argv is None:
        argv = sys.argv[1:]
    try:
        # setup option parser
        parser = OptionParser(version=program_version_string,
                              epilog=program_longdesc)
        parser.add_option("-i", "--in", dest="infile",
                          help="set input path [default: %default]", metavar="FILE")

        # process options
        (opts, args) = parser.parse_args(argv)

        outfile = "graph_schema.txt"
        if opts.infile:
            sys.stderr.write("infile = %s\n" % opts.infile)
            infile = opts.infile
        else:
            sys.stderr.write("json input file is required.\n")
            parser.print_help()
            return int(2)

        # MAIN BODY #

        sys.stderr.write('Processing %s\n' % opts.infile)
        xformer = Json2CSV()
        xformer.getJsonData(opts.infile)
        xformer.generateCreateTableSQL(opts.infile, outfile)
        sys.stderr.write('Generate graph schema\n')
        return 0

    except Exception as e:
        sys.stderr.write(repr(e))
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == '__main__':
    sys.exit(main())
