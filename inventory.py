import utils


def list_inventory(session, name_rules):
    node_info = {}

    def scan_node(neo4j_id):
        cypher = "MATCH (a) WHERE ID(a) = {id} RETURN a as property"
        group = session.run(cypher, {"id": neo4j_id})
        group_prop = group.peek()["property"]
        group_key = group_prop["name"]
        cypher = "MATCH (a)-[:{0}]->(b:{1}) WHERE ID(a) = {{id}}" \
                 " RETURN ID(b) as id".format(name_rules["inclusion_relation_type"],
                                              name_rules["group_label"])
        children = session.run(cypher, {"id": neo4j_id})
        children_id = [x["id"] for x in children]
        cypher = "MATCH (a)-[:{0}]->(b:{1}) WHERE ID(a) = {{id}}" \
                 " RETURN b.name as name".format(name_rules["inclusion_relation_type"],
                                                 name_rules["host_label"])
        hosts = session.run(cypher, {"id": neo4j_id})
        hosts_name = [x["name"] for x in hosts]
        node_info[neo4j_id] = {
                "key": group_key,
                "vars": dict(group_prop),
                "hosts": hosts_name,
                "children_id": children_id
        }
        for i in children_id:
            if i not in node_info.keys():
                scan_node(i)
        return

    # find the group "all" as root node
    cypher = "MATCH (a:{0} {{name: 'all'}})" \
             " RETURN ID(a) as id".format(name_rules["group_label"])
    root_node = session.run(cypher)
    root_node_id = root_node.peek()["id"]

    scan_node(root_node_id)

    # change structure of node_info to appropreate one
    # for ansible dynamic inventory
    group_inventory = {}
    for key, val in node_info.items():
        # remove "name" vars because it is inserted by store.py
        # to handle nodes in neo4j
        if "name" in val["vars"].keys():
            del val["vars"]["name"]
        # search sub var
        sub_vars = query_subvars(key, name_rules)
        group_inventory[val["key"]] = {
                "vars": {k: v for dic in [val["vars"], sub_vars]
                         for k, v in dic.items()},
                "hosts": val["hosts"],
                "children": [node_info[x]["key"]
                             for x in val["children_id"]]
                }
    group_inventory["ungrouped"] = []

    # add '_meta' information
    group_inventory["_meta"] = {"hostvars": list_all_hostvars(session, name_rules)}
    return group_inventory


def query_subvars(node_id, name_rules):
    cypher = "MATCH (a)-[p]->(b:{0}) WHERE ID(a) = {{id}}"\
             " RETURN type(p) as label, (p.index is not null) as islist,"\
             " b as var order by p.index".format(name_rules["vars_label"])
    sub_vars = session.run(cypher, {"id": node_id})
    var = {}
    for sub_var in sub_vars:
        var_name = sub_var["label"]
        if sub_var["islist"]:
            if var_name not in var.keys():
                var[var_name] = []
            var[var_name].append(dict(sub_var["var"]))
        else:
            var[var_name] = dict(sub_var["var"])
    return var


def list_hostvars(session, name_rules, hostname):
    # find the group "all" as root node
    cypher = "MATCH (a:{0} {{name: 'all'}})" \
             " RETURN ID(a) as id".format(name_rules["group_label"])
    root_node = session.run(cypher)
    root_node_id = root_node.peek()["id"]

    cypher = "MATCH (a)-[:{0}*]->(b:{1} {{name: {{name}}}})" \
             " WHERE ID(a) = {{id}} RETURN ID(b) AS id," \
             " b AS property".format(name_rules["inclusion_relation_type"], name_rules["host_label"])
    host_result = session.run(cypher, {"name": hostname, "id": root_node_id})
    host = host_result.peek()
    host_prop = dict(host["property"])
    # remove "name" vars because it is inserted by store.py
    # to handle nodes in neo4j
    if "name" in host_prop.keys():
        del host_prop["name"]
    sub_vars = query_subvars(host["id"], name_rules)
    hostvars = {k: v for dic in [host_prop, sub_vars] for k, v in dic.items()}
    return hostvars


def list_all_hostvars(session, name_rules):
    # find the group "all" as root node
    cypher = "MATCH (a:{0} {{name: 'all'}})" \
             " RETURN ID(a) as id".format(name_rules["group_label"])
    root_node = session.run(cypher)
    root_node_id = root_node.peek()["id"]

    cypher = "MATCH (a)-[:{0}*]->(b:{1}) WHERE ID(a) = {{id}}" \
             " RETURN ID(b) as id, b AS property".format(name_rules["inclusion_relation_type"],
                                                         name_rules["host_label"])
    hosts = session.run(cypher, {"id": root_node_id})
    hostvars = {}
    for host in hosts:
        host_prop = dict(host["property"])
        # remove "name" vars because it is inserted by store.py
        # to handle nodes in neo4j
        if "name" in host_prop.keys():
            del host_prop["name"]
        sub_vars = query_subvars(host["id"], name_rules)
        host_key = host["property"]["name"]
        hostvars[host_key] = {k: v for dic in [host_prop, sub_vars]
                              for k, v in dic.items()}
    return hostvars


if __name__ == "__main__":
    import argparse
    import getpass
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        default='config.yml',
                        action='store',
                        help='config file path')

    parser.add_argument('-H', '--host',
                        default=False,
                        action='store',
                        help='host name to be shown')

    parser.add_argument('-l', '--list',
                        default=False,
                        action='store_true',
                        help='show host list')

    args = parser.parse_args()

    conf = utils.load_conf(args.config)

    if "user" in conf["neo4j"].keys() and "password" not in conf["neo4j"].keys():
        conf["neo4j"]["password"] = getpass.getpass(prompt='Enter Neo4j password')

    neo4j_driver = utils.connect_to_neo4j(conf["neo4j"]["host"],
                                          conf["neo4j"]["bolt_port"],
                                          conf["neo4j"].get("user",None),
                                          conf["neo4j"].get("password",None))
    session = neo4j_driver.session()

    if args.host:
        print(json.dumps(list_hostvars(session, conf["label_name"], args.host)))
    elif args.list:
        print(json.dumps(list_inventory(session, conf["label_name"])))

    session.close()
