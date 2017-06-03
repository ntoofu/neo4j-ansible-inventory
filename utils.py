def load_conf(filepath):
    import yaml
    with open(filepath, 'r') as f:
        conf = yaml.load(f)
    return conf


def connect_to_neo4j(host, port, user, password):
    from neo4j.v1 import GraphDatabase, basic_auth
    return GraphDatabase.driver("bolt://{}:{}".format(host, port),
                                auth=basic_auth(user, password))


def reset_db(session):
    session.run("MATCH ()-[p]->() DELETE p")
    session.run("MATCH (n) DELETE n")
