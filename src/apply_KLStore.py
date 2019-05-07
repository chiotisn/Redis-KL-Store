import redis

def apply_KLStore(name, func):
    """Applies <func> on each element of a list, for each list of the KL store <name>

    :param name: KL store's name
    :type name: str
    :param func: a python function, which gets a String and returns a String
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

    pipe = r.pipeline()
    # get keys from <name> KL store
    keys = r.keys(name + ":*")
    for key in keys:
        # get all elements of a list-value of a key
        elements = r.lrange(key,0,-1)
        # deletes key
        r.delete(key)
        # add the "filtered" K-L pair
        for el in elements:
            pipe.rpush(key, func(el))

    pipe.execute()

if __name__ == '__main__':
    def func(str):
        return str + "aaaaaa"
    apply_KLStore("2try", func)
