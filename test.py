from ansibleutils.ansibleutils import load_ansible_inventory
from ansibleutils.ansibleutils import diff_ansible_inventory
import unittest
import utils
from store import store


class Neo4jAnsibleTestCase(unittest.TestCase):
    def setUp(self):
        conf = utils.load_conf("test_config.yml")
        neo4j_driver = utils.connect_to_neo4j(conf["neo4j"]["host"],
                                              conf["neo4j"]["bolt_port"],
                                              conf["neo4j"].get("user",None),
                                              conf["neo4j"].get("password",None))
        session = neo4j_driver.session()
        self.static_inventory = load_ansible_inventory(
                                    conf["ansible"]["playbook_dir"],
                                    conf["ansible"]["inventory_path"],
                                    None)
        store(session, self.static_inventory, conf["label_name"])
        session.close()

    def test_same_inventories(self):
        dynamic_inventory = load_ansible_inventory(
                                "dummy/", "test_inventory.sh", None)
        result = diff_ansible_inventory(
                    self.static_inventory, dynamic_inventory, True)
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
