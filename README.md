# Abstraction Layer

The heterogeneity of different data cleaning tools makes it difficult to use them. Abstraction layer is an easy-to-use project for working with different data cleaning tools. In particular, the abstraction layer provides simple APIs for running data cleaning tools on datasets and evaluating them. 


## Installation
To install the module, make sure that you already have installed Python 2.7 and Oracle Java 1.8 on your system. Furthermore, some of the underlying data cleaning tools need to be installed. For example, to install NADEEF, you need to check its installation instructions.


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
The dataset dictionary should respect the following structure:
    dataset_dictionary = {
       "name": "dataset_name",
       "path": "dataset/path.csv",
       "clean_path": "optional/ground_truth/path.csv",
       "repaired_path": "optional/repaired_dataset/path.csv"
    }

### data_cleaning_tool.py

This file contains the implementation of the data cleaning tool class.

