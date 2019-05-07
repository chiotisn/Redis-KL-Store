import redis

def projSel_KLStore(output_name, names_list, expression):
    """Performs a join on the common keys of some KL stores, creating a new KL store
        having keys the common keys and corresponding list the concatenation of the
        individual lists filtered with the <expression>

    :param output_name: new KL store's name
    :type output_name: str
    :param name_list: the list containing the names of the KL stores are going to join together
    :type name_list: list
    :param expression: a valid boolean python expression, within the expression, key and KL stores imvolved
                        should be prepended by some special symbols. The key must be prepended with
                        "##key" and the values of each KL store with the name of the store: ##name
                        For example, if we have 3 KL stores: cust, age, transaction_value, an expression
                        could be: "##key not in [1423,1456,3422] and (##age > 20 or ##transaction_value < 10)"
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

    # create a dictionary contains all KL stores keys (without the name part)
    d = {}
    for name in names_list:
        #get keys
        keys = r.keys(name + ":*")
        d[name] = []
        for k in keys:
            # seperate KL store's name from actual keys and append it in the keys' list
            d[name].append(k.split(":")[1])

    # get keys from the first KL store in the list
    # based on these keys we will check if they are common among the other stores
    name1 = names_list[0]
    keys = d[name1]
    new_keys = []
    for k in keys:
        new_keys.append(k)

    # check what keys of the first store are common between all KL stores.
    for k in new_keys:
        # a flag variable which indicates if a store not containong the key found
        flag = False
        for klstore in names_list:
            if k not in d[klstore]:
                flag = True
        if flag == True:
            new_keys.remove(k)
    # now new_keys contains only the common keys

    pipe = r.pipeline()
    # check if the given expression is true for each key
    saved_expression = expression
    for k in new_keys:
        expression = saved_expression
        # replace ##key with the key value
        expression = expression.replace("##key","'" + k + "'")
        # replace special symbol ##name with each kl store value
        for name in names_list:
            # get the first value of the name's list
            value = r.lrange(name + ":" + k, 0 , -1)[0]
            expression = expression.replace("##" + name, "'" + value + "'")

        # check if expression is true, if yes, add the KL pair in the new store
        if eval(expression):
            for name in names_list:
                for v in r.lrange(name + ":" + k, 0 , -1):
                    pipe.lpush(output_name + ":" + k, v)

    pipe.execute()
