# neo4j-ansible-inventory

Store or fetch [Ansible](https://github.com/ansible/ansible) inventory data
to or from [Neo4j](https://github.com/neo4j/neo4j)

## Installation

`pip install -r requirements.txt`

## Usage

```shell
# Run Neo4j
docker run -d -e 'NEO4J_AUTH=none' -p 7474:7474 -p 7687:7687 -v /tmp/neo4j/data:/data -v /tmp/neo4j/logs:/logs --name neo4j neo4j:3.0

# Store ansible inventory data to Neo4j
# WARNING: this command will remove existing nodes and pathes in the Neo4j
python store.py -n localhost -b path/to/ansible/basedir -i path/to/ansible/inventory

# Fetch data from Neo4j and return Ansible dynamic inventory
python inventory.py -n localhost --list
python inventory.py -n localhost --host hostname
```

## Requirements
- ansible 2.1.0
