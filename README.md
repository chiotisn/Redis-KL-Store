# Redis-KL-Store
A project for storing and proccessing Key-List stores in Redis. These stores can come from different sources: csv/excel file or MySQL database.   
This project developed for "Big Data Management Systems" course. More info about this project can be found [here](https://drive.google.com/file/d/14PU6PwAFce4WgRc2bliFby7VM0X2jmHN/view?usp=sharing).

## Requirements
1. python3 
2. Redis
3. Python packages: redis-py, mysql-connector-python, xlrd

## Files Description
In **src** folder you can find all the code files needed in order use this project's functions. Each file implements one function:
* **create_KLStore**(name, data-source, query-string, position1, position2, direction)
* **filter_KLStore**(name, expression)
* **apply_kLStore**(name, func)
* **aggr_KLStore**(name, aggr, func)
* **lookUp_KLStore**(name1, name2)
* **projSel**(output_name, names_list, expression)
* **test_KLStore.py** contains unit testing methods   

Further info is available inside each file (Docstrings, comments)   
   
**test_data** folder contains files that are used in the unit testing or can be used as examples.

**data-sources.json** contains information about the data sources that can be used to create a KL store. In order to create a KL store from your own source, you have to register it first at this file. 

## Tests
**test_KLStore.py** contains 14 unit tests for all the functions of this project, in order to ensure that functions run as they should. To run tests:   
`$ python3 src/test_KLStores.py`  
**unittest** was used to write the tests.

## Use Project
To use this project download the files and import to your programm the function you want to use. (see test_KLStore.py)




