# Abstraction Layer
The heterogeneity of different data cleaning tools makes it difficult to use them. Abstraction layer is an easy-to-use project for working with different data cleaning tools. In particular, the abstraction layer provides simple APIs for running data cleaning tools on datasets and evaluating them. 


## Installation
To install the module, make sure that you already have installed Python 2.7, Python 3.6, and Oracle Java 1.8 on your system. Furthermore, the following underlying data cleaning tools need to be installed separately: 
- NADEEF
   1. Install Apache Ant 1.8.2+.
   2. Install PostgreSQL 9.2+.
   3. Go to the NADEEF folder and compile it with command "ant all".
   4. Create a database entitled "nadeef" in the postgres.
   5. Add your postgres username and password into NADEEF config file "nadeef.conf".


## Sample Code
```python
    import dataset
    import data_cleaning_tool

    dataset_dictionary = {
        "name": "toy",
        "path": "datasets/dirty.csv"
    }
    d = dataset.Dataset(dataset_dictionary)

    data_cleaning_tool_dictionary = {
        "name": "dboost",
        "configuration": ["gaussian", "2.5"]
    }
    t = data_cleaning_tool.DataCleaningTool(data_cleaning_tool_dictionary)
    print t.run(d)

``` 


## Content
### datasets
This folder contains some sample datasets.

### tools
This folder contains some of the underlying data cleaning tools.

### dataset.py
This file contains the implementation of the dataset class.

The input dataset should respect the following assumptions:
   1. A dataset is a relational table in comma delimiter utf-8 CSV format.
   2. The first line of a dataset is the header and the rests form the data matrix.
   3. The header must have only non-space lowercase characters as field names without type description.
   4. The header is row 0 and the first tuple of data matrix is row 1.
   5. Do not name an attribute with "tid", "cast", or other database special keywords.

The input dataset dictionary to the constructor of the dataset class should respect the following structure:
```python
    dataset_dictionary = {
       "name": "dataset_name",
       "path": "dataset/path.csv",
       "clean_path": "optional/ground_truth/path.csv",
       "repaired_path": "optional/repaired_dataset/path.csv"
    }
```

### data_cleaning_tool.py
This file contains the implementation of the data cleaning tool class.

The input data cleaning tool dictionary to the constructure of the data cleaning tool class should respect the following structure:
```python
    data_cleaning_tool_dictionary = {
        "name": "data_cleaning_tool_name",
        "configuration": ["tool's", "configuration", "in", "form", "of", "a", "list"]
    }
```
Examples of currently supported tools/configurations are as follows:
```python 
    data_cleaning_tool_dictionary = {
        "name": "dboost",
        "configuration": ["histogram", "0.8", "0.2"]
    }
```
```python
    data_cleaning_tool_dictionary = {
        "name": "dboost",
        "configuration": ["gaussian", "2.5"]
    }
```
```python
    data_cleaning_tool_dictionary = {
        "name": "dboost",
        "configuration": ["mixture", "5", "0.1"]
    }
```
```python
    data_cleaning_tool_dictionary = {
        "name": "dboost",
        "configuration": ["partitionedhistogram", "5", "0.8", "0.2"]
    }
```
```python
    data_cleaning_tool_dictionary = {
        "name": "regex",
        "configuration": [["attribute_name", "^[\d]+$", "OM"], ["another_attribute_name", "^[a-z]+$", "ONM"],...]
    }
```
Note that "OM" means output values that are matched to the regex. Contrary, "ONM" stands for output non-matches.
```python
    data_cleaning_tool_dictionary = {
        "name": "katara",
        "configuration": ["path/to/domain/specific/folder"]
    }
```
```python
    data_cleaning_tool_dictionary = {
        "name": "nadeef",
        "configuration": [["an_attribute", "another_attribute"],...]
    }
```
Note that the tool outputs data cells that violate functional dependencies "an_attribute" -> "another_attribute",...
```python
    data_cleaning_tool_dictionary = {
        "name": "fd_checker",
        "configuration": [["an_attribute", "another_attribute"],...]
    }
```
Note that the tool outputs data cells that violate functional dependencies "an_attribute" -> "another_attribute",...
