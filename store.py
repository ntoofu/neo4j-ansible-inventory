import utils
import neo4j.v1.exceptions
from definition import Neo4jNode, Definition
from config import user_def

class AnsibleToNeo4j:
    def __init__(self, definition=Definition()):
        self.definition = definition
        self._rel_label = definition.relation_label
        self._rep_label = definition.representing_node.label
        self._rep_rel_label = definition.rep_rel_label
        self._rep_name = definition.representing_node.name
        self._name_rule = definition.node_name_rule
        self._vars_label = definition.vars_label
        self._vars_rule = definition.vars_rule

    def _parse_inventory(self, inventory):
        uuid_to_node = {}
        # create all nodes
        ansible_to_node = {}
        hosts = set()
        for groupname, group in inventory.groups.items():
            ansible_to_node[("group",groupname)] = Neo4jNode()
            hosts |= set(group.hosts)
        for host in hosts:
            ansible_to_node[("host",host.name)] = Neo4jNode()

        # setup all nodes
        for ans_type, ans_nodes in {"group": inventory.groups.values(), "host": hosts}.items():
            for ans_node in ans_nodes:
                node = ansible_to_node[(ans_type,ans_node.name)]
                namerule = self._name_rule(ans_node)
                node.name = namerule.neo4j_name
                node.label = namerule.neo4j_label
                node.vars = self._vars_rule.from_ansible(node.name, node.label, ans_node)
                if ans_type == "group":
                    node.childnodes = [ansible_to_node[("group",g.name)].uuid for g in ans_node.child_groups]
                    node.childnodes.extend([ansible_to_node[("host",h.name)].uuid for h in ans_node.hosts])
                uuid_to_node[node.uuid] = node

        return uuid_to_node


    def _create_inventory_in_neo4j(self, nodes, session):
        # create inventory unique node
        cypher = "CREATE (a:{0} {{name: {{rep_name}}}}) RETURN ID(a) as id".format(self._rep_label)
        result = session.run(cypher, {"rep_name": self._rep_name})
        rep_id = result.peek()["id"]

        # create all nodes and store node id
        for uuid, node in nodes.items():      # iterate over ["host","group"]
            cypher = "MATCH (b) WHERE ID(b) = {{id}} CREATE (a:{0} {{name:{{name}}}})<-[:{1}]-(b)" \
                     " RETURN ID(a) as id".format(node.label, self._rep_rel_label)
            created_node = session.run(cypher, {"name": node.name, "id": rep_id})
            node.id = created_node.peek()["id"]

        # create all pathes
        for uuid, node in nodes.items():
            parent_id = node.id
            for child_uuid in node.childnodes:
                child_id = nodes[child_uuid].id
                cypher = "MATCH (p),(c)" \
                         " WHERE ID(p) = {{pid}} AND ID(c) = {{cid}}" \
                         " CREATE (p) -[:{0}]->(c)".format(self._rel_label)
                session.run(cypher, {"pid": parent_id, "cid": child_id})

        # create all vars
        for uuid, node in nodes.items():
            self._set_vars(node.id, node.vars, session)

        return


    def _create_subelement(self, session, key, var, node_id, path_idx=None):
        query_str = ", ".join(["{0}: {{val}}.{0}".format(k) for k in var.keys()])
        cypher = "MATCH (a:{0} {{{1}}})" \
                 " RETURN ID(a) as id".format(self._vars_label, query_str)
        vars_node = session.run(cypher, {"val": dict(var)})
        try:
            vars_node_id = vars_node.peek()["id"]
        except neo4j.v1.exceptions.ResultError:
            cypher = "CREATE (a:{0} {{val}})" \
                     " RETURN ID(a) as id".format(self._vars_label)
            vars_node = session.run(cypher, {"val": dict(var)})
            vars_node_id = vars_node.peek()["id"]
        if path_idx is None:
            cypher = "MATCH (a), (b) WHERE ID(a) = {nid} AND ID(b) = {vid}" \
                     " CREATE (a)-[p:ANSIBLE_VARS {name:{path_name}}]->(b)"
            session.run(cypher, {"nid": node_id, "vid": vars_node_id, "path_name": key})
        else:
            cypher = "MATCH (a), (b) WHERE ID(a) = {nid} AND ID(b) = {vid}" \
                     " CREATE (a)-[p:ANSIBLE_VARS {name:{path_name}, index:{path_index}}]->(b)"
            session.run(cypher, {"nid": node_id, "vid": vars_node_id, "path_name": key, "path_index": path_idx})


    def _set_vars(self, node_id, kvs, session):
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

        for k, v in kvs.items():
            if isinstance(v, collections.Mapping):
                self._create_subelement(session, k, v, node_id)
            elif (isinstance(v, collections.Sequence) and
                  len(set([type(w) for w in v])) == 1 and
                  isinstance(v[0], collections.Mapping)):
                for i, w in enumerate(v):
                    self._create_subelement(session, k, w, node_id, i)
            else:
                plain_val = _type_sanitize(v)
                cypher = "MATCH (a) WHERE ID(a) = {{id}}" \
                         " SET a.{0} = {{val}}".format(k)
                session.run(cypher, {"id": node_id, "val": plain_val})


    def store(self, session, inventory):
        node_info = self._parse_inventory(inventory)
        utils.reset_db(session)
        self._create_inventory_in_neo4j(node_info, session)
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
    a2n = AnsibleToNeo4j(user_def)
    a2n.store(session, inventory)
    session.close()
