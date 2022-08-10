# alibaba-graph-compute

This Python script provides a utility to convert neo4j json files into the http request format that is used by Graph Compute. This script is compatible with Python3.

## Usage

```
Usage: python3 generate_graph_schema.py [Options]

Options:
  --version             show program's version number and exit
  -i FILE, --in=FILE    set input path [default: none]

A utility python script to generate graph schema for Graph Compute.
```
```
Usage: python3 http_json2igraph.py

A utility python script to write into Graph Compute via http request.
You need to modify the conf.yaml
```

## Example
Execute the first Python script to produce txt file(graph schema).
Execute the second Python script to write into Graph Compute.
```
$ python3 generate_graph_schema.py -i tinkerpop-modern.json
infile = tinkerpop-modern.json
Processing tinkerpop-modern.json
Generate graph schema
```
```
$ python3 http_json2igraph.py
Processing test.json
it splits 60 files
all 20389824 doc
it takes 15620.67168069072 s
deal with 1305 doc each second
```
