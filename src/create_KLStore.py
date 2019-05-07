import redis
import json
import csv
import mysql.connector
import xlrd
import os
import sys

#global variables
data_info = {}
r = None
initialized = False

def init():
    """Connects to Redis and gets data-sources' information. The aim of this function is to improve the performance of a programm,
        in which someone creates more than one KL store.
    """
    # connection information for Redis,replace them with your configuration information.
    redis_host = "localhost"
    redis_port = 6379
    redis_password = ""

    # create the Redis Connection object
    try:
        global r
        r = redis.Redis(host = redis_host, port = redis_port, password = redis_password, db = 0, decode_responses = True)

    except Exception as e:
        raise Exception(e)

    # open json file that contains data-sourses' information
    with open(os.path.abspath(os.path.dirname(__file__))+'/../data-sources.json', 'r') as f:
        global data_info
        data_info = json.load(f)['datasources']



def create_KLStore(name, data_source, query_string, position1, position2, direction):
    """Creates in Redis a KL store using a data source, data source can be a mysql database, a csv or an excel file.
        Data sources have to be registered in the data-sources.json file.

    :param name: KL store's name
    :type name: str
    :param data_source: name of the data-source that will be used
    :type data_source: str
    :param query_string: empty if data_source is a csv file, the index of the worksheet in the case of an excel, SQL statement for a DB data_source
    :type query_string: str
    :param position1: number specifying the first column's position in the case of a csv or excel file
    :type position1: int
    :param position2: number specifying the second column's position in the case of a csv or excel file
    :type position2: int
    :param direction: has the value 1 or 2, specifying whether KL1(D) or KL2(D) should be implemented, any other value is treated as 1
    :type: int
    """

    # checks if global variables have been initialized
    global initialized
    if initialized == False:
        init()
        initialized = True

    # checks if data_source is registered to the json file
    if data_source not in data_info:
        print(data_source + ": no such data source found.")
        sys.exit()

    # create KL store from excel
    if data_info[data_source]["type"] == "excel":

        #excel's file's info
        filepath = os.path.abspath(os.path.dirname(__file__)) + "/" + data_info[data_source]["path"] + data_info[data_source]["filename"]
        has_header = data_info[data_source]["has_header"]

        # read excel and create KL store in redis_host
        workbook = xlrd.open_workbook(filepath)
        worksheet = None
        try:
            index = int(query_string)
            worksheet = workbook.sheet_by_index(index)
            if worksheet is None:
                raise ValueError()
        except ValueError:
            # try is query_string contains the name and not the index of the worksheet (fault-tolerance)
            if query_string in workbook.sheet_names():
                worksheet = workbook.sheet_by_name(query_string)
            else:
                print(query_string + ": no such worksheet found.")
                sys.exit()

        #using pipeline to buffer redis commands and send them to server at once, to achieve better performance
        pipe = r.pipeline()
        is_header = True
        if direction == 2:
            keys = worksheet.col(position2)
            values = worksheet.col(position1)
        else:
            keys = worksheet.col(position1)
            values = worksheet.col(position2)
        for i in range(len(keys)):
            #if the file has a header line skips the first line
            if has_header == True and is_header == True:
                is_header = False
                continue

            pipe.rpush(name + ":" + keys[i].value, values[i].value)

        pipe.execute()

    # create KL store from CSV
    if data_info[data_source]["type"] == "csv":

        #csv file's info
        filepath = os.path.abspath(os.path.dirname(__file__)) + "/" + data_info[data_source]["path"] + data_info[data_source]["filename"]
        delimiter = data_info[data_source]["delimiter"]
        has_header = data_info[data_source]["has_header"]
        print(has_header)
        #read csv and create KL store in redis_host
        with open(filepath, 'r') as f:
            reader = csv.reader(f, delimiter = delimiter)

            #using pipeline to buffer redis commands and send them to server at once, to achieve better performance
            pipe = r.pipeline()
            for line in reader:
                #if the file has a header line skips the first line
                if has_header:
                    has_header = False
                    continue

                if direction == 2:
                    pipe.rpush(name + ":" + line[position2],line[position1])
                else:
                    pipe.rpush(name + ":" + line[position1],line[position2])
            pipe.execute()

    # create KL store from mySQL database
    if data_info[data_source]["type"] == "db":

        # db's info & connection
        host = data_info[data_source]["dbconnect"]["host"]
        username = data_info[data_source]["dbconnect"]["username"]
        password = data_info[data_source]["dbconnect"]["password"]
        database = data_info[data_source]["dbconnect"]["database"]
        db = mysql.connector.connect(host = host, user = username, passwd = password, database = database)

        cursor = db.cursor()
        cursor.execute(query_string)
        results = cursor.fetchall()

        pipe = r.pipeline()
        for row in results:

            if direction == 2:
                pipe.rpush(name + ":" + row[1], row[0])
            else:
                pipe.rpush(name + ":" + row[0], row[1])

        pipe.execute()
        db.close()
        cursor.close()




#if __name__ == '__main__':
#    create_KLStore("1try", "EXCEL_example", "0", 0, 2, 1)
#    create_KLStore("test_", "CSV_test", "", 0, 1, 1)
#    create_KLStore("3try", "DB_example", "SELECT name, address FROM customers", 0, 0, 1)
