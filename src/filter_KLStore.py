import redis

def filter_KLStore(name, expression):
    """Filters each element of list of a KL store named <name>, based on the <expression> given.

    :param name: KL store's name
    :type name: str
    :param expression: a valid boolean python expression, the element which is evaluated each time at the expression
                        must be represented with <el>. (example: "not(el > 0)")
    :type expression: str
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
        for el in elements:
            if eval(expression) == False:
                # removes all elements occured with the <el> value
                pipe.lrem(key, 0, el)

    pipe.execute()

if __name__ == '__main__':
    filter_KLStore("2try", "el == 'c'")
