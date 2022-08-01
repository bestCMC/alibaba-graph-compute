# alibaba-graph-compute

This Python script provides a utility to convert neo4j json files into the CSV format that is used by Graph Compute. This script is compatible with Python3.

## Usage

```
Usage: json2csv.py [options]

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -i FILE, --in=FILE    set input path [default: none]
  -d DELIMITER, --delimiter=DELIMITER
                        Set the output file delimiter [default: |]

A utility python script to convert neo4j json files into the Graph Compute CSV format. 
```

## Example Using the Tinkerpop modern graph.

Execute the Python script to produce csv files(nodes and edges) and txt file(sql to create table in odps).

```
$ python3 json2csv.py -i tinkerpop-modern.json
infile = tinkerpop-modern.json
Processing tinkerpop-modern.json
It takes 1.4884080737829208 ms
It has 12 doc
Deal with 8 doc each second
```
