import utils
from definition import Neo4jNode, Definition
from config import user_def

class Neo4jToAnsible:

    def __init__(self, definition=Definition()):
        self.definition = definition
        self._rel_label = definition.relation_label
        self._rep_label = definition.representing_node.label
        self._rep_rel_label = definition.rep_rel_label
        self._rep_name = definition.representing_node.name
        self._name_rule = definition.node_name_rule
        self._vars_label = definition.vars_label
        self._vars_rule = definition.vars_rule

    def list_inventory(self, session):
        node_info = {}

        # find the representing node
        cypher = "MATCH (a:{0} {{name: {{name}} }}) RETURN ID(a) as id".format(self._rep_label)
        rep_node = session.run(cypher, {"name": self._rep_name}).peek()
        rep_id = rep_node["id"]

        # find all nodes used in this ansible inventory
        cypher = "MATCH (a)<-[:{0}]-(b) WHERE ID(b) = {{id}} RETURN ID(a) as id, a.name as name, LABELS(a) as label".format(self._rep_rel_label)
        all_nodes = session.run(cypher, {"id": rep_id})
        neo4j_node_list = [Neo4jNode(x["label"][0], x["name"], id=x["id"]) for x in all_nodes]

        # create dictionary whose structure is equivalent to the dynamic inventory
        groups = {}
        hostvars = {}
        for neo4j_node in neo4j_node_list:
            node = self._name_rule(neo4j_node)

            # get variables
            var = self._vars_rule.from_neo4j(neo4j_node, session)

            if node.ansible_is_host:
                hostvars[node.ansible_name] = var
            else:
                # find child_groups of the node
                cypher = "MATCH (a)-[:{0}]->(b)<-[:{1}]-(c)" \
                         " WHERE ID(a) = {{id}} AND ID(c) = {{rep_id}}" \
                         " RETURN b.name as name, LABELS(b) as label".format(self._rel_label, self._rep_rel_label)
                results = session.run(cypher, {"id": neo4j_node.id, "rep_id": rep_id})
                children = [self._name_rule(Neo4jNode(x["label"][0], x["name"])) for x in results]
                groups[node.ansible_name] = { "vars": var,
                                              "hosts": [x.ansible_name for x in children if x.ansible_is_host],
                                              "children": [x.ansible_name for x in children if not x.ansible_is_host] }
        groups["_meta"] = {"hostvars": hostvars}
        return groups



    def list_hostvars(self, session, hostname):
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
        hostvars = {k: v for dic in [host_prop, sub_vars] for k, v in dic.items()}
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

    n2a = Neo4jToAnsible(user_def)

    if args.host:
        print(json.dumps(n2a.list_hostvars(session, args.host)))
    elif args.list:
        print(json.dumps(n2a.list_inventory(session)))

    session.close()
