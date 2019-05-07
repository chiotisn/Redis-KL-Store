import redis

def lookUp_KLStore(name1, name2):
    """For each element of each list in <name1>, this fuction performs a lookup for the element in the keys of <name2>, gets the List
                of the matched key and replaces the element in the first list with the elements of the second.

    :param name1: KL store's name, whose elements will be replaced
    :type name1: str
    :param name2: KL store's name, whose lists will replace <name1> elements
    :type name2: str
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

    pipe = r.pipeline()
    # get keys from <name1> and <name2> KL stores
    name1_keys = r.keys(name1 + ":*")
    name2_keys = r.keys(name2 + ":*")
    for k1 in name1_keys:
        # get all values from each key of <name1>
        k1_values = r.lrange(k1,0,-1)
        new_values = []
        pipe.delete(k1)

        for value in k1_values:
            # lookup for the element in the keys of <name2>
            if (name2 + ":" + value) in name2_keys:
                k2 = name2 + ":" + value
                k2_values = r.lrange(k2,0,-1)

                for v2 in k2_values:
                    new_values.append(v2)

        for v in new_values:
            pipe.lpush(k1,v)

        pipe.execute()
