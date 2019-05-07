import redis
import unittest

from create_KLStore import create_KLStore
from filter_KLStore import filter_KLStore
from apply_KLStore import apply_KLStore
from aggr_KLStore import aggr_KLStore
from lookUp_KLStore import lookUp_KLStore
from projSel_KLStore import projSel_KLStore

class TestKLStores(unittest.TestCase):
    """A Test class for all the KL stores functions"""

    r = None
    def setup(self):
        pass

    def test_create_KLStore_excel(self):
        create_KLStore("test_excel", "EXCEL_test", "0", 0, 2, 1)
        global r
        keys = r.keys("test_excel:*")
        # check if keys are the vaues of the column with index 0
        self.assertEqual(sorted(keys), sorted(["test_excel:a1","test_excel:a3","test_excel:a5","test_excel:a8"]))
        # check the values of the keys
        self.assertEqual(sorted(r.lrange("test_excel:a1",0,-1)), sorted(["A","D","C"]))
        self.assertEqual(r.lrange("test_excel:a3",0,-1), ["E"])
        self.assertEqual(r.lrange("test_excel:a5",0,-1), ["F"])
        self.assertEqual(r.lrange("test_excel:a8",0,-1), ["A"])

    def test_create_KLStore_excel_direction2(self):
        create_KLStore("test_excel_2", "EXCEL_test", "0", 0, 2, 2)
        global r
        keys = r.keys("test_excel_2:*")
        # check if keys are the vaues of the column with index 2
        self.assertEqual(sorted(keys), sorted(["test_excel_2:A","test_excel_2:E","test_excel_2:F","test_excel_2:D","test_excel_2:C"]))
        # check the values of the keys
        self.assertEqual(sorted(r.lrange("test_excel_2:A",0,-1)), sorted(["a1","a8"]))
        self.assertEqual(r.lrange("test_excel_2:E",0,-1), ["a3"])
        self.assertEqual(r.lrange("test_excel_2:F",0,-1), ["a5"])
        self.assertEqual(r.lrange("test_excel_2:D",0,-1), ["a1"])

    def test_create_KLStore_csv(self):
        create_KLStore("test_csv", "CSV_test", "", 0, 1, 1)
        global r
        keys = r.keys("test_csv:*")
        # check if keys are the vaues of the column with index 0
        self.assertEqual(sorted(keys), sorted(["test_csv:Nick","test_csv:Maria","test_csv:George"]))
        # check the values of the keys
        self.assertEqual(sorted(r.lrange("test_csv:Maria",0,-1)), sorted(["25","20"]))
        self.assertEqual(r.lrange("test_csv:Nick",0,-1), ["21"])
        self.assertEqual(r.lrange("test_csv:George",0,-1), ["56"])

    # SQL commands for the table used for testing
    # create table test_db (store varchar(50), sales double);
    # insert into test_db (store, sales) values ("st1",50);
    # insert into test_db (store, sales) values ("st1",73.2);
    # insert into test_db (store, sales) values ("st1",120.2);
    # insert into test_db (store, sales) values ("st2",350.45);
    # insert into test_db (store, sales) values ("st3",550.0);
    def test_create_KLStore_db(self):
        create_KLStore("test_db", "DB_example", "SELECT store,sales FROM test_db", 0, 0, 1)
        global r
        keys = r.keys("test_db:*")
        # check if keys are equal to "store" column values
        self.assertEqual(sorted(keys), sorted(["test_db:st1","test_db:st2","test_db:st3"]))
        # check the values of the keys
        self.assertEqual(sorted(r.lrange("test_db:st1",0,-1)), sorted(["120.2","50.0","73.2"]))
        self.assertEqual(r.lrange("test_db:st2",0,-1), ["350.45"])
        self.assertEqual(r.lrange("test_db:st3",0,-1), ["550.0"])

    def test_filter_KLStore(self):
        global r
        #create a KL Store to test function
        r.lpush("test_filter:student1", "A")
        r.lpush("test_filter:student1", "B")
        r.lpush("test_filter:student1", "D")
        r.lpush("test_filter:student2", "F")
        r.lpush("test_filter:student3", "C")
        #call function
        filter_KLStore("test_filter","el in ['A','B','C']")
        # check the values of the keys
        self.assertEqual(sorted(r.lrange("test_filter:student1",0,-1)), sorted(["A","B"]))
        self.assertEqual(r.lrange("test_filter:student2",0,-1), [])
        self.assertEqual(r.lrange("test_filter:student3",0,-1), ["C"])

    def test_apply_KLStore(self):
        global r
        #create a KL Store to test function
        r.lpush("test_apply:student1", "A")
        r.lpush("test_apply:student1", "B")
        r.lpush("test_apply:student1", "D")
        r.lpush("test_apply:student2", "F")
        r.lpush("test_apply:student3", "C")
        #call function
        apply_KLStore("test_apply", grades_evaluation)
        # check the values of the keys
        self.assertEqual(sorted(r.lrange("test_apply:student1",0,-1)), sorted(["Pass","Pass","Fail"]))
        self.assertEqual(r.lrange("test_apply:student2",0,-1), ["Fail"])
        self.assertEqual(r.lrange("test_apply:student3",0,-1), ["Pass"])

    def test_aggr_KLStore_avg(self):
        global r
        #create a KL Store to test function
        r.lpush("test_avg:t1", 2)
        r.lpush("test_avg:t1", 2.4)
        r.lpush("test_avg:t1", 3.6)
        r.lpush("test_avg:t1", 5)
        r.lpush("test_avg:t2", 3)
        r.lpush("test_avg:t2", 4)
        r.lpush("test_avg:t2", "foo")
        r.lpush("test_avg:t3", "It's a trap!")
        #call function
        aggr_KLStore("test_avg", "avg", "")
        # check the values of the keys
        self.assertEqual(r.lrange("test_avg:t1",0,-1), ["3.25"])
        self.assertEqual(r.lrange("test_avg:t2",0,-1), ["3.5"])
        self.assertEqual(r.exists("test_avg:t3"), 0)

    def test_aggr_KLStore_sum(self):
        global r
        #create a KL Store to test function
        r.lpush("test_sum:t1", 2)
        r.lpush("test_sum:t1", 2.4)
        r.lpush("test_sum:t1", 3.6)
        r.lpush("test_sum:t1", 5)
        r.lpush("test_sum:t2", 3)
        r.lpush("test_sum:t2", 4)
        r.lpush("test_sum:t2", "foo")
        r.lpush("test_sum:t3", "It's a trap!")
        #call function
        aggr_KLStore("test_sum", "sum", "")
        # check the values of the keys
        self.assertEqual(r.lrange("test_sum:t1",0,-1), ["13.0"])
        self.assertEqual(r.lrange("test_sum:t2",0,-1), ["7.0"])
        self.assertEqual(r.exists("test_sum:t3"), 0)

    def test_aggr_KLStore_count(self):
        global r
        #create a KL Store to test function
        r.lpush("test_count:t1", 2)
        r.lpush("test_count:t1", "string")
        r.lpush("test_count:t1", "test")
        r.lpush("test_count:t1", 5)
        r.lpush("test_count:t2", 3)
        r.lpush("test_count:t2", 4)
        r.lpush("test_count:t2", "foo")
        r.lpush("test_count:t3", "It's a trap!")
        #call function
        aggr_KLStore("test_count", "count", "")
        # check the values of the keys
        self.assertEqual(r.lrange("test_count:t1",0,-1), ["4"])
        self.assertEqual(r.lrange("test_count:t2",0,-1), ["3"])
        self.assertEqual(r.lrange("test_count:t3",0,-1), ["1"])

    def test_aggr_KLStore_max(self):
        global r
        #create a KL Store to test function
        r.lpush("test_max:t1", -2)
        r.lpush("test_max:t1", "string")
        r.lpush("test_max:t1", "test")
        r.lpush("test_max:t1", 5)
        r.lpush("test_max:t2", 3)
        r.lpush("test_max:t2", 4)
        r.lpush("test_max:t2", "foo")
        r.lpush("test_max:t3", "It's a trap!")
        #call function
        aggr_KLStore("test_max", "max", "")
        # check the values of the keys
        self.assertEqual(r.lrange("test_max:t1",0,-1), ["5.0"])
        self.assertEqual(r.lrange("test_max:t2",0,-1), ["4.0"])
        self.assertEqual(r.exists("test_max:t3"), 0)

    def test_aggr_KLStore_mim(self):
        global r
        #create a KL Store to test function
        r.lpush("test_min:t1", -2)
        r.lpush("test_min:t1", "string")
        r.lpush("test_min:t1", "test")
        r.lpush("test_min:t1", 5)
        r.lpush("test_min:t2", 3)
        r.lpush("test_min:t2", 4)
        r.lpush("test_min:t2", "foo")
        r.lpush("test_min:t3", "It's a trap!")
        #call function
        aggr_KLStore("test_min", "min", "")
        # check the values of the keys
        self.assertEqual(r.lrange("test_min:t1",0,-1), ["-2.0"])
        self.assertEqual(r.lrange("test_min:t2",0,-1), ["3.0"])
        self.assertEqual(r.exists("test_min:t3"), 0)

    def test_aggr_KLStore_func(self):
        global r
        #create a KL Store to test function
        r.lpush("test_func:t1", -2)
        r.lpush("test_func:t1", "string")
        r.lpush("test_func:t1", "test")
        r.lpush("test_func:t1", 5)
        r.lpush("test_func:t2", 3)
        r.lpush("test_func:t2", 4)
        r.lpush("test_func:t2", "foo")
        r.lpush("test_func:t3", "It's a trap!")
        #call function
        aggr_KLStore("test_func", "", concatenate)
        # check the values of the keys
        self.assertEqual(r.lrange("test_func:t1",0,-1), ["-25stringtest"])
        self.assertEqual(r.lrange("test_func:t2",0,-1), ["34foo"])
        self.assertEqual(r.lrange("test_func:t3",0,-1), ["It's a trap!"])

    def test_lookup_KLStore(self):
        global r
        #create a KL Store to test function
        r.lpush("test_lookup1:t1", "s3")
        r.lpush("test_lookup1:t1", "s7")
        r.lpush("test_lookup1:t1", "s9")
        r.lpush("test_lookup1:t2", "s4")
        r.lpush("test_lookup1:t2", "s7")
        r.lpush("test_lookup2:s3", "3")
        r.lpush("test_lookup2:s3", "4")
        r.lpush("test_lookup2:s4", "6")
        r.lpush("test_lookup2:s7", "14")
        r.lpush("test_lookup2:s9", "17")
        r.lpush("test_lookup2:s9", "30")
        #call function
        lookUp_KLStore("test_lookup1", "test_lookup2")
        # check the values of the keys
        self.assertEqual(sorted(r.lrange("test_lookup1:t1",0,-1)), sorted(["3","4","14","17","30"]))
        self.assertEqual(sorted(r.lrange("test_lookup1:t2",0,-1)), sorted(["6","14"]))

    def test_projsel_KLStore(self):
        global r
        #create 3 KL Stores to test function
        r.lpush("test_projsel1:t1", "s1")
        r.lpush("test_projsel1:t2", "s2")
        r.lpush("test_projsel1:t3", "s3")
        r.lpush("test_projsel1:t4", "s6")
        r.lpush("test_projsel1:t6", "s4")
        r.lpush("test_projsel1:t7", "s4")

        r.lpush("test_projsel2:t1", "4")
        r.lpush("test_projsel2:t6", "6")
        r.lpush("test_projsel2:t2", "7")
        r.lpush("test_projsel2:t3", "4")
        r.lpush("test_projsel2:t4", "4")
        r.lpush("test_projsel2:t7", "8")

        r.lpush("test_projsel3:t2", "6")
        r.lpush("test_projsel3:t3", "14")
        r.lpush("test_projsel3:t6", "17")
        r.lpush("test_projsel3:t4", "30")
        r.lpush("test_projsel3:t5", "30")
        r.lpush("test_projsel3:t7", "35")
        # call function
        projSel_KLStore("test_projsel", ["test_projsel1","test_projsel2","test_projsel3"],"##key != 't6' and ##test_projsel1 in ['s1','s2','s3','s4'] and float(##test_projsel2) < 8")
        # check the values of the keys
        keys = r.keys("test_projsel:*")
        self.assertEqual(sorted(keys), sorted(["test_projsel:t2","test_projsel:t3"]))
        self.assertEqual(sorted(r.lrange("test_projsel:t2",0,-1)), sorted(["s2","7","6"]))
        self.assertEqual(sorted(r.lrange("test_projsel:t3",0,-1)), sorted(["s3","4","14"]))

    def tearDown(self):
        global r
        all_keys = r.keys("test_*")
        for k in all_keys:
            r.delete(k)

if __name__ == '__main__':
    # create the Redis Connection object
    try:
        global r
        r = redis.Redis(host = "localhost", port = 6379, password = "", db = 0, decode_responses = True)
    except Exception as e:
        raise Exception(e)

    def grades_evaluation(str):
        if str in ["A","B","C"]:
            return "Pass"
        else:
            return "Fail"

    def concatenate(list):
        list.sort()
        aggr = ""
        for i in list:
            aggr = aggr + i
        return aggr

    unittest.main()
