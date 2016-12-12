import utils
import conf


def list_inventory(session):
    node_info = {}

    def scan_node(neo4j_id):
        cypher = "MATCH (a) WHERE ID(a) = {id} RETURN a as property"
        group = session.run(cypher, {"id": neo4j_id})
        group_prop = group.peek()["property"]
        group_key = group_prop["name"]
        cypher = "MATCH (a)-[:{0}]->(b:{1}) WHERE ID(a) = {{id}}" \
                 " RETURN ID(b) as id".format(conf.relation_label,
                                              conf.group_label)
        children = session.run(cypher, {"id": neo4j_id})
        children_id = [x["id"] for x in children]
        cypher = "MATCH (a)-[:{0}]->(b:{1}) WHERE ID(a) = {{id}}" \
                 " RETURN b.name as name".format(conf.relation_label,
                                                 conf.host_label)
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
             " RETURN ID(a) as id".format(conf.group_label)
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
        sub_vars = query_subvars(key)
        group_inventory[val["key"]] = {
                "vars": {k: v for dic in [val["vars"], sub_vars]
                         for k, v in dic.items()},
                "hosts": val["hosts"],
                "children": [node_info[x]["key"]
                             for x in val["children_id"]]
                }
    group_inventory["ungrouped"] = []

    # add '_meta' information
    group_inventory["_meta"] = {"hostvars": list_all_hostvars(session)}
    return group_inventory


def query_subvars(node_id):
    cypher = "MATCH (a)-[p]->(b:{0}) WHERE ID(a) = {{id}}"\
             " RETURN type(p) as label, (p.index is not null) as islist,"\
             " b as var order by p.index".format(conf.vars_label)
    sub_vars = session.run(, {"id": node_id})
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


def list_hostvars(session, hostname):
    # find the group "all" as root node
    cypher = "MATCH (a:{0} {{name: 'all'}})" \
             " RETURN ID(a) as id".format(conf.group_label)
    root_node = session.run(cypher)
    root_node_id = root_node.peek()["id"]

    cypher = "MATCH (a)-[:{0}*]->(b:{1} {{name: {{name}}}})" \
             " WHERE ID(a) = {{id}} RETURN ID(b) AS id," \
             " b AS property".format(conf.relation_label, conf.host_label)
    host_result = session.run(cypher, {"name": hostname, "id": root_node_id})
    host = host_result.peek()
    host_prop = dict(host["property"])
    # remove "name" vars because it is inserted by store.py
    # to handle nodes in neo4j
    if "name" in host_prop.keys():
        del host_prop["name"]
    sub_vars = query_subvars(host["id"])
    hostvars = {k: v for dic in [host_prop, sub_vars] for k, v in dic.items()}
    return hostvars


def list_all_hostvars(session):
    # find the group "all" as root node
    cypher = "MATCH (a:{0} {{name: 'all'}})" \
             " RETURN ID(a) as id".format(conf.group_label)
    root_node = session.run(cypher)
    root_node_id = root_node.peek()["id"]

    cypher = "MATCH (a)-[:{0}*]->(b:{1}) WHERE ID(a) = {{id}}" \
             " RETURN ID(b) as id, b AS property".format(conf.relation_label,
                                                         conf.host_label)
    hosts = session.run(cypher, {"id": root_node_id})
    hostvars = {}
    for host in hosts:
        host_prop = dict(host["property"])
        # remove "name" vars because it is inserted by store.py
        # to handle nodes in neo4j
        if "name" in host_prop.keys():
            del host_prop["name"]
        sub_vars = query_subvars(host["id"])
        host_key = host["property"]["name"]
        hostvars[host_key] = {k: v for dic in [host_prop, sub_vars]
                              for k, v in dic.items()}
    return hostvars


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(
                description='Arguments for talking to vCenter')
    parser.add_argument('-n', '--neo4j-host',
                        default='localhost',
                        action='store',
                        help='Neo4j service to connect to')

    parser.add_argument('-o', '--neo4j-port',
                        type=int,
                        default=7687,
                        action='store',
                        help='Port to connect on with bolt')

    parser.add_argument('-u', '--neo4j-user',
                        required=False,
                        action='store',
                        help='Username to use')

    parser.add_argument('-p', '--neo4j-password',
                        required=False,
                        action='store',
                        help='Password to use')

    parser.add_argument('-H', '--host',
                        default=None,
                        help='inventory hostname whose hostvars will be shown')

    parser.add_argument('-l', '--list',
                        default=False,
                        action='store_true',
                        help='show host list')

    args = parser.parse_args()

    if args.neo4j_user and not args.neo4j_password:
        args.neo4j_password = getpass.getpass(prompt='Enter password')

    neo4j_driver = utils.connect_to_neo4j(args.neo4j_host,
                                          args.neo4j_port,
                                          args.neo4j_user,
                                          args.neo4j_password)
    session = neo4j_driver.session()

    if args.host:
        print(json.dumps(list_hostvars(session, args.host)))
    elif args.list:
        print(json.dumps(list_inventory(session)))

    session.close()
