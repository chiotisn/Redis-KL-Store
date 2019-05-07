import redis
import sys
import math

def isNum(str):
    try:
        tmp = float(str)
        return True
    except:
        return False

def aggr_KLStore(name, aggr, func):
    """Aggregates each list of the KL store <name> according to the specified aggregator <aggr>, or the given <func> python fuction,
        updating the list with the aggregation result

    :param name: KL store's name
    :type name: str
    :param aggr: aggragator, one of the values: avg/sum/count/min/max
    :type aggr: str
    :param func: a python function, which gets a list of strings and aggregates them
    :type func: func
    """

    # connection information for Redis,replace them with your configuration information.
    redis_host = "localhost"
    redis_port = 6379
    redis_password = ""

    # create the Redis Connection object
    try:
        r = redis.Redis(host = redis_host, port = redis_port, password = redis_password, db = 0, decode_responses = True)

    except Exception as e:
        raise Exception(e)

    # get keys from <name> KL store
    keys = r.keys(name + ":*")

    pipe = r.pipeline()

    if aggr == "avg":
        for k in keys:
            sum = 0
            counter = 0
            values = r.lrange(k,0,-1)
            for v in values:
                # chesk if value is a valid number
                if isNum(v):
                    sum += float(v)
                    counter +=1
            #delete previous values
            pipe.delete(k)
            if counter > 0:
                # calculate avg and add it in the KL store
                avg = sum/counter
                pipe.rpush(k,avg)
        pipe.execute()

    elif aggr == "sum":
        for k in keys:
            sum = 0
            counter = 0
            values = r.lrange(k,0,-1)
            for v in values:
                # chesk if value is a valid number
                if isNum(v):
                    sum += float(v)
                    counter += 1
            # delete previous values
            pipe.delete(k)
            # check is list has at least one number
            if counter > 0:
                # add sum in the KL store
                pipe.rpush(k,sum)
        pipe.execute()

    elif aggr == "count":
        for k in keys:
            values = r.lrange(k,0,-1)
            # get list's elements count
            count = len(values)
            # delete previous values
            pipe.delete(k)
            # add count in the KL store
            pipe.rpush(k,count)
        pipe.execute()

    elif aggr == "min":

        for k in keys:
            values = r.lrange(k,0,-1)
            min = math.inf
            counter = 0
            for v in values:
                # chesk if value is number and smaller than min
                if isNum(v) and float(v) < min:
                    min = float(v)
                    counter += 1
            # delete previous values
            pipe.delete(k)
            # check is list has at least one number
            if counter > 0:
                # add min in the KL store
                pipe.rpush(k,min)
        pipe.execute()

    elif aggr == "max":

        for k in keys:
            values = r.lrange(k,0,-1)
            max = - math.inf
            counter = 0
            for v in values:
                # check if value is number and bigger than max
                if isNum(v) and float(v) > max:
                    max = float(v)
                    counter += 1
            # delete previous values
            pipe.delete(k)
            # check is list has at least one number
            if counter > 0:
                # add max in the KL store
                pipe.rpush(k,max)
        pipe.execute()

    elif aggr == "" and func is not None:

        for k in keys:
            values = r.lrange(k,0,-1)
            aggr = func(values)
            # delete previous values
            pipe.delete(k)
            # add func return value in the KL store
            pipe.rpush(k, aggr)
        pipe.execute()

    else:

        print("Wrong parameters")
        sys.exit()
