from ansibleutils.ansibleutils import load_ansible_inventory
from ansibleutils.ansibleutils import diff_ansible_inventory
import unittest
import utils
from store import store


class Neo4jAnsibleTestCase(unittest.TestCase):
    def setUp(self):
        neo4j_driver = utils.connect_to_neo4j("localhost", 7687, "", "")
        session = neo4j_driver.session()
        basedir = "ansibleutils/testdata/1"
        inventory_path = "ansibleutils/testdata/1/inventory"
        self.static_inventory = load_ansible_inventory(
                                    basedir, inventory_path, "")
        store(session, self.static_inventory)
        session.close()

    def test_same_inventories(self):
        dynamic_inventory = load_ansible_inventory(
                                "dummy/", "test_inventory.sh", None)
        result = diff_ansible_inventory(
                    self.static_inventory, dynamic_inventory, True)
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
