import utils
import neo4j.v1.exceptions


def _parse_inventory(inventory, name_rules):

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
                "label": name_rules["group_label"],
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
                "label": name_rules["host_label"],
                "vars": host.vars,
                "child_keys": [],
                "is_host": True,
                "neo4j_id": None
                }

    return node_info


def _create_inventory_tree_in_neo4j(node_info, session, name_rules):
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
                     " CREATE (p) -[:{0}]->(c)".format(name_rules["inclusion_relation_type"])
            session.run(cypher, {"pid": parent_id, "cid": child_id})

    return


def _create_subelement(session, name_rules, key, var, node_id, path_idx=None):
    cypher = "CREATE (a:{0} {{val}})" \
             " RETURN ID(a) as id".format(name_rules["vars_label"])
    vars_node = session.run(cypher, {"val": dict(var)})
    vars_node_id = vars_node.peek()["id"]
    if path_idx is None:
        cypher = "MATCH (a), (b) WHERE ID(a) = {{nid}} AND ID(b) = {{vid}}" \
                 " CREATE (a)-[:{0}]->(b)".format(key)
    else:
        cypher = "MATCH (a), (b) WHERE ID(a) = {{nid}} AND ID(b) = {{vid}}" \
                 " CREATE (a)-[:{0} {{index: {1}}}]->(b)".format(key, path_idx)
    session.run(cypher, {"nid": node_id, "vid": vars_node_id})


def _set_vars_to_neo4j(node_info, session, name_rules):
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
                _create_subelement(session, name_rules, k, v, node["neo4j_id"])
            elif (isinstance(v, collections.Sequence) and
                  len(set([type(w) for w in v])) == 1 and
                  isinstance(v[0], collections.Mapping)):
                for i, w in enumerate(v):
                    _create_subelement(session, name_rules, k, w, node["neo4j_id"], i)
            else:
                plain_val = _type_sanitize(v)
                cypher = "MATCH (a) WHERE ID(a) = {{id}}" \
                         " SET a.{0} = {{val}}".format(k)
                session.run(cypher, {"id": node["neo4j_id"], "val": plain_val})


def store(session, inventory, name_rules):
    node_info = _parse_inventory(inventory, name_rules)
    utils.reset_db(session)
    _create_inventory_tree_in_neo4j(node_info, session, name_rules)
    _set_vars_to_neo4j(node_info, session, name_rules)
    return

if __name__ == "__main__":
    import argparse
    import getpass
    from ansibleutils.ansibleutils import load_ansible_inventory

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        default='config.yml',
                        action='store',
                        help='config file path')

    args = parser.parse_args()

    conf = utils.load_conf(args.config)

    if "user" in conf["neo4j"].keys() and "password" not in conf["neo4j"].keys():
        conf["neo4j"]["password"] = getpass.getpass(prompt='Enter Neo4j password')

    if "use_vault" in conf["ansible"].keys() and "password" not in conf["ansible"].keys():
        conf["ansible"]["password"] = getpass.getpass(prompt='Enter Ansible Vault password')

    inventory = load_ansible_inventory(conf["ansible"]["playbook_dir"],
                                       conf["ansible"]["inventory_path"],
                                       conf["ansible"].get("password",None))

    neo4j_driver = utils.connect_to_neo4j(conf["neo4j"]["host"],
                                          conf["neo4j"]["bolt_port"],
                                          conf["neo4j"].get("user",None),
                                          conf["neo4j"].get("password",None))
    session = neo4j_driver.session()
    store(session, inventory, conf["label_name"])
    session.close()
