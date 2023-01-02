#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = []
__version__ = 0.1
__date__ = '2022-08-01'
__updated__ = '2022-08-01'

import json
import sys
import os
import time
import requests
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor
import yaml
import traceback

EDGE_LABEL = "relationship"
VERTEX_LABEL = "node"

class Json2CSV:
    def __init__(self, thread_num):
        self.split_num = thread_num
        self.id_map = {}
        self.doc_cnt = 0
        self.split_file_name = []

    def getJsonData(self, fname):
        with open(fname, 'r', encoding='utf-8') as f, open('conf.yaml', "r", encoding="utf-8") as cf:
            conf = yaml.load(cf.read(), Loader=yaml.Loader)
            for line in f.readlines():
                self.doc_cnt += 1
                data = json.loads((line))
                if data['type'] == VERTEX_LABEL:
                    # 构建一个id的映射
                    labelV = data['labels'][0]
                    self.id_map[data['id']] = data['properties'][conf['pkey'][labelV]]  #TODO
                elif data['type'] == EDGE_LABEL:
                    pass

    def split_file(self, fname):
        nums = [(self.doc_cnt * i // self.split_num) for i in range(1, self.split_num + 1)]
        line_idx = 0
        file_idx = 0
        data_list = []
        file_path = os.getcwd()
        with open(fname, 'r', encoding='utf-8') as sourceFile:
            for line in sourceFile:
                data_list.append(line)
                line_idx = line_idx + 1
                if line_idx in nums:
                    file_idx = file_idx + 1
                    save_file_name = fname.split('.json')[0] + '_' + str(file_idx) + '.json'
                    current_path = os.path.join(file_path, save_file_name)
                    self.split_file_name.append(current_path)
                    with open(current_path, 'w', encoding='utf-8') as dstFile:
                        for data in data_list:
                            dstFile.write(data)
                        data_list = []

    def deleteSplitFileAfterDeal(self):
        for path in self.split_file_name:
            os.remove(path)

    def generateCreateTableSQL(self, fname):
        type_of_one_properties = []

        try:
            with open(fname, 'r', encoding='utf-8') as f, open('conf.yaml', "r", encoding="utf-8") as cf:
                conf = yaml.load(cf.read(), Loader=yaml.Loader)
                http_reqs = []
                for line in f.readlines():
                    data = json.loads((line))
                    type_of_one_properties.clear()
                    http_request = "http://" + conf['endpoint'] + "/update?type=1&"
                    # create node table
                    if data['type'] == VERTEX_LABEL:
                        labelV = data['labels'][0]

                        for k, v in data['properties'].items():
                            if k == conf['pkey'][labelV]:   #TODO
                                type_of_one_properties.insert(0, ['pkey', v])
                            else:
                                type_of_one_properties.append([k, v])

                        req = ""
                        table_name = conf['graph_name'] + '_' + conf['node'][labelV]

                        for k, v in type_of_one_properties:
                            req = req + str(k) + '=' + str(v) + '&'
                        http_request = http_request + req + 'table=' + table_name
                        # 通过params传参
                        http_reqs.append(http_request)

                    elif data['type'] == EDGE_LABEL:
                        labelE = data['label']

                        type_of_one_properties.append(
                            ['pkey', self.id_map[data['start']['id']]])
                        type_of_one_properties.append(
                            ['skey', self.id_map[data['end']['id']]])

                        for k, v in data['properties'].items():
                            #if k == 'id':
                                #pass
                            #else:
                            type_of_one_properties.append([k, v])

                        req = ""
                        table_name = conf['graph_name'] + '_' + conf['edge'][labelE]
                        for k, v in type_of_one_properties:
                            req = req + str(k) + '=' + str(v) + '&'
                        http_request = http_request + req + 'table=' + table_name
                        http_reqs.append(http_request)

                with requests.Session() as session:
                    for req in http_reqs:
                        #print(req)
                        session.get(url=req, auth=HTTPBasicAuth(conf['user_name'], conf['password']))

        except Exception as e:
            traceback.print_exc()
            return 3


def checkConfBeforeStart():
    sys.stderr.write("ready to check 'conf.yaml' before deal with json file\n")
    with open('conf.yaml', "r", encoding="utf-8") as cf:
        conf = yaml.load(cf.read(), Loader=yaml.Loader)
        if not isinstance(conf['node'], dict):
            sys.stderr.write("somthing wrong with node in conf\n")
            return False
        if not isinstance(conf['edge'], dict):
            sys.stderr.write("somthing wrong with edge in conf\n")
            return False
    sys.stderr.write("finish to check the 'conf.yaml'\n")
    return True

def main(argv=None):

    '''Command line options.'''

    program_name = os.path.basename(sys.argv[0])
    program_version = "v0.1"
    program_build_date = "%s" % __updated__

    program_version_string = '%%prog %s (%s)' % (
        program_version, program_build_date)
    program_longdesc = ("A utility python script to convert json files into CSV format ")

    if argv is None:
        argv = sys.argv[1:]
    try:
        outfile = "generate_table_sql.txt"

        # MAIN BODY #

        if not checkConfBeforeStart():
            return -1

        start = time.perf_counter()

        with open('conf.yaml', "r", encoding="utf-8") as cf:
            conf = yaml.load(cf.read(), Loader=yaml.Loader)

            sys.stderr.write('Processing %s\n' % conf['source_file_path'])

            xformer = Json2CSV(int(conf['thread_num']))
            xformer.getJsonData(conf['source_file_path'])
            xformer.split_file(conf['source_file_path'])
            # xformer.generateCreateTableSQL(opts.infile)

            sys.stderr.write('all %s doc, ' % xformer.doc_cnt)
            sys.stderr.write('it has %s threads to deal with that\n' % xformer.split_num)

            with ThreadPoolExecutor(max_workers=conf['thread_num']) as pool:
                # 使用线程执行map计算
                # 后面元组有N个元素，因此程序启动N条线程来执行action函数
                results = pool.map(xformer.generateCreateTableSQL, xformer.split_file_name, timeout=1)

            xformer.deleteSplitFileAfterDeal()

        elapsed = time.perf_counter() - start
        sys.stderr.write('it takes %s s\n' % elapsed)
        doc_num_each_second = xformer.doc_cnt // elapsed
        sys.stderr.write('deal with %d doc each second\n' % doc_num_each_second)

        return 0

    except Exception as e:
        traceback.print_exc()
        return 2

if __name__ == '__main__':
    sys.exit(main())

