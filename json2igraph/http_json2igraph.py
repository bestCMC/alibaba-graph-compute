#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = []
__version__ = 0.1
__date__ = '2022-08-01'
__updated__ = '2022-08-01'

import copy
import json
import sys
import os
import csv
from optparse import OptionParser
import time
import requests
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor
import threading
import yaml
import pdb

EDGE_LABEL = "relationship"
VERTEX_LABEL = "node"

class Json2CSV:
    def __init__(self, thread_num):
        self.split_num = thread_num
        # self.get_node_table = []
        # self.get_edge_table = []
        self.id_map = {}
        self.doc_cnt = 0
        self.split_file_name = []

    def getJsonData(self, fname):
        with open(fname, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                self.doc_cnt += 1
                data = json.loads((line))
                if data['type'] == VERTEX_LABEL:
                    # 构建一个id的映射
                    self.id_map[data['id']] = data['properties']['id']
                elif data['type'] == EDGE_LABEL:
                    pass

    def split_file(self, fname):
        nums = [(self.doc_cnt * i // self.split_num) for i in range(1, self.split_num + 1)]  # 每份子文件最后对应的行数
        current_line = 0  # 记录当前读取的行
        file_idx = 0  # 记录当前是第几个子文件
        data_list = []
        file_path = os.getcwd()
        with open(fname, 'r', encoding='utf-8') as f1:
            for line in f1:  # 逐行读取原文件内容
                data_list.append(line)  # 存到临时变量
                current_line = current_line + 1
                if current_line in nums:  # 如果当前行已经达到了之前计算的nums对应的行，则把临时数据全部写入对应子文件
                    file_idx = file_idx + 1
                    save_file_name = fname.split('.json')[0] + '_' + str(file_idx) + '.json'  # 创建子文件名
                    current_path = os.path.join(file_path, save_file_name)
                    self.split_file_name.append(current_path)
                    with open(current_path, 'w', encoding='utf-8') as f2:
                        for data in data_list:
                            f2.write(data)
                        data_list = []  # 清空临时变量，用于下一个循环获取内容，写入下一个子文件

    def generateCreateTableSQL(self, fname):
        type_of_one_properties = []
        conf_name = "conf.yaml"

        try:
            with open(fname, 'r', encoding='utf-8') as f, open(conf_name, "r", encoding="utf-8") as cf:
                conf = yaml.load(cf.read(), Loader=yaml.Loader)
                http_reqs = []
                for line in f.readlines():
                    data = json.loads((line))
                    type_of_one_properties.clear()
                    http_request = "http://" + conf['ip'] + "/update?type=1&"
                    # create node table
                    if data['type'] == VERTEX_LABEL:
                        labelV = data['labels'][0]

                        for k, v in data['properties'].items():
                            if k == 'id':
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
                            if k == 'id':
                                pass
                            else:
                                type_of_one_properties.append([k, v])

                        req = ""
                        table_name = conf['graph_name'] + '_' + conf['edge'][labelE]
                        for k, v in type_of_one_properties:
                            req = req + str(k) + '=' + str(v) + '&'
                        http_request = http_request + req + 'table=' + table_name
                        http_reqs.append(http_request)

                with requests.Session() as session:
                    for req in http_reqs:
                        session.get(url=req, auth=HTTPBasicAuth(conf['user'], conf['password']))

        except Exception as e:
            sys.stderr.write(repr(e))
            return 3

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

        start = time.perf_counter()

        with open('conf.yaml', "r", encoding="utf-8") as cf:
            conf = yaml.load(cf.read(), Loader=yaml.Loader)

            sys.stderr.write('Processing %s\n' % conf['source_data'])

            xformer = Json2CSV(int(conf['thread_num']))
            xformer.getJsonData(conf['source_data'])
            xformer.split_file(conf['source_data'])
            # xformer.generateCreateTableSQL(opts.infile)

            sys.stderr.write('it splits %s files\n' % xformer.split_num)

            with ThreadPoolExecutor(max_workers=conf['thread_num']) as pool:
                # 使用线程执行map计算
                # 后面元组有N个元素，因此程序启动N条线程来执行action函数
                results = pool.map(xformer.generateCreateTableSQL, xformer.split_file_name, timeout=1)

        sys.stderr.write('all %s doc\n' % xformer.doc_cnt)
        elapsed = time.perf_counter() - start
        sys.stderr.write('it takes %s s\n' % elapsed)
        doc_num_each_second = xformer.doc_cnt // elapsed
        sys.stderr.write('deal with %d doc each second\n' % doc_num_each_second)
        return 0

    except Exception as e:
        sys.stderr.write(repr(e))
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == '__main__':
    sys.exit(main())
