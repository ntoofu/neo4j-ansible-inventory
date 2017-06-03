# neo4j-ansible-inventory

Store or fetch [Ansible](https://github.com/ansible/ansible) inventory data
to or from [Neo4j](https://github.com/neo4j/neo4j)

## Installation

`pip install -r requirements.txt`

## Usage

```shell
# Edit config file
$EDITOR config.yml

# Run Neo4j
docker run -d -e 'NEO4J_AUTH=none' -p 7474:7474 -p 7687:7687 -v /tmp/neo4j/data:/data -v /tmp/neo4j/logs:/logs --name neo4j neo4j:3.0

# Build runtime (necessary whenever you edit any files in this project)
docker build -t neo4j-ansible-inventory .

# Store ansible inventory data to Neo4j
# WARNING: this command will remove existing nodes and pathes in the Neo4j
docker run neo4j-ansible-inventory store.py -c config.yml

# Fetch data from Neo4j and return Ansible dynamic inventory
docker run neo4j-ansible-inventory inventory.py -c config.yml --list
docker run neo4j-ansible-inventory inventory.py -c config.yml --host hostname
```

## Test
```shell
# WARNING: this command will remove existing nodes and pathes in the Neo4j
./run_test.sh
```

## Requirements
- ansible 2.1.0
- Neo4j 3.0
