import utils
import conf
import neo4j.v1.exceptions


def _parse_inventory(inventory):

    node_info = {}
    hosts = set()
    # retrieve all groups
    for groupname, group in inventory.groups.items():
        node_name = groupname

        # store all node data into 'node_info'
        children = [child.name for child in group.child_groups
                    if child.name is not "ungrouped"]
        children.extend([host.name for host in group.hosts])
        node_info[groupname] = {
                "name": node_name,
                "label": conf.group_label,
                "vars": group.vars,
                "child_keys": children,
                "is_host": False,
                "neo4j_id": None
                }

        # cache hosts
        hosts |= set(group.hosts)

    # append hosts to 'node_info'
    for host in hosts:
        node_info[host.name] = {
                "name": host.name,
                "label": conf.host_label,
                "vars": host.vars,
                "child_keys": [],
                "is_host": True,
                "neo4j_id": None
                }

    return node_info


def _create_inventory_tree_in_neo4j(node_info, session):
    # create all nodes and store node id
    for key, node in node_info.items():
        cypher = "CREATE (a:{0} {{name:{{name}}}})" \
                 " RETURN ID(a) as id".format(node["label"])
        created_node = session.run(cypher, {"name": node["name"]})
        node["neo4j_id"] = created_node.peek()["id"]

    # create all pathes
    for key, node in node_info.items():
        parent_id = node["neo4j_id"]
        for child_key in node["child_keys"]:
            child_id = node_info[child_key]["neo4j_id"]
            cypher = "MATCH (p),(c)" \
                     " WHERE ID(p) = {{pid}} AND ID(c) = {{cid}}" \
                     " CREATE (p) -[:{0}]->(c)".format(conf.relation_label)
            session.run(cypher, {"pid": parent_id, "cid": child_id})

    return


def _create_subelement(session, key, var, node_id, path_idx=None):
    pass
    query_str = ", ".join(["{0}: {{val}}.{0}".format(k) for k in var.keys()])
    cypher = "MATCH (a:{0} {{{1}}})" \
             " RETURN ID(a) as id".format(conf.vars_label, query_str)
    vars_node = session.run(cypher, {"val": dict(var)})
    try:
        vars_node_id = vars_node.peek()["id"]
    except neo4j.v1.exceptions.ResultError:
        cypher = "CREATE (a:{0} {{val}})" \
                 " RETURN ID(a) as id".format(conf.vars_label)
        vars_node = session.run(cypher, {"val": dict(var)})
        vars_node_id = vars_node.peek()["id"]
    if path_idx is None:
        cypher = "MATCH (a), (b) WHERE ID(a) = {{nid}} AND ID(b) = {{vid}}" \
                 " CREATE (a)-[:{0}]->(b)".format(key)
    else:
        cypher = "MATCH (a), (b) WHERE ID(a) = {{nid}} AND ID(b) = {{vid}}" \
                 " CREATE (a)-[:{0} {{index: {1}}}]->(b)".format(key, path_idx)
    session.run(cypher, {"nid": node_id, "vid": vars_node_id})


def _set_vars_to_neo4j(node_info, session):
    import collections

    def _type_sanitize(var):
        if True in [isinstance(var, var_type)
                    for var_type in [int, float, str, bool]]:
            return var
        if isinstance(var, collections.Sequence):
            types = set([type(elem) for elem in var])
            if len(types) > 1 or isinstance(var[0], collections.Container):
                return [str(elem) for elem in var]
            return var
        return str(var)

    for nodename, node in node_info.items():
        for k, v in node["vars"].items():
            if isinstance(v, collections.Mapping):
                _create_subelement(session, k, v, node["neo4j_id"])
            elif (isinstance(v, collections.Sequence) and
                  len(set([type(w) for w in v])) == 1 and
                  isinstance(v[0], collections.Mapping)):
                for i, w in enumerate(v):
                    _create_subelement(session, k, w, node["neo4j_id"], i)
            else:
                plain_val = _type_sanitize(v)
                cypher = "MATCH (a) WHERE ID(a) = {{id}}" \
                         " SET a.{0} = {{val}}".format(k)
                session.run(cypher, {"id": node["neo4j_id"], "val": plain_val})


def store(session, inventory):
    node_info = _parse_inventory(inventory)
    utils.reset_db(session)
    _create_inventory_tree_in_neo4j(node_info, session)
    _set_vars_to_neo4j(node_info, session)
    return

if __name__ == "__main__":
    import argparse
    from ansibleutils.ansibleutils import load_ansible_inventory

    parser = argparse.ArgumentParser(
            description='connect to Neo4j server'
                        ' and return Ansible dynamic inventory')
    parser.add_argument('-n', '--neo4j-host',
                        default='localhost',
                        action='store',
                        help='Neo4j service to connect to')

    parser.add_argument('-o', '--neo4j-port',
                        type=int,
                        default=7687,
                        action='store',
                        help='Port to connect on for bolt')

    parser.add_argument('-u', '--neo4j-user',
                        required=False,
                        action='store',
                        help='Username to use')

    parser.add_argument('-p', '--neo4j-password',
                        required=False,
                        action='store',
                        help='Password to use')

    parser.add_argument('-b', '--basedir',
                        default='.',
                        action='store',
                        help='Ansible basedir path')

    parser.add_argument('-i', '--inventory',
                        required=True,
                        action='store',
                        help='Ansible inventory path')

    parser.add_argument('-P', '--vault-password',
                        required=False,
                        action='store',
                        help='Password to use')

    args = parser.parse_args()

    if args.neo4j_user and not args.neo4j_password:
        args.neo4j_password = getpass.getpass(prompt='Enter password')

    neo4j_driver = utils.connect_to_neo4j(args.neo4j_host,
                                          args.neo4j_port,
                                          args.neo4j_user,
                                          args.neo4j_password)
    inventory = load_ansible_inventory(args.basedir,
                                       args.inventory,
                                       args.vault_password)
    session = neo4j_driver.session()
    store(session, inventory)
    session.close()
