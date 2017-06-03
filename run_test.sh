docker_image_ver="1.0"
docker_image="neo4j-ansible-inventory:${docker_image_ver}"

# if [[ "$(docker images -q $docker_image 2> /dev/null)" == "" ]]; then
#     docker build -t "$docker_image" .
# fi
docker build -t "$docker_image" .
docker run --rm -d -e NEO4J_AUTH=none -p 7474:7474 -p 7687:7687 --name test_neo4j-ansible-inventory_neo4j neo4j:3.0 || die
sleep 10
docker run --rm --name test_neo4j-ansible-inventory_python --link test_neo4j-ansible-inventory_neo4j "$docker_image" test.py
docker stop test_neo4j-ansible-inventory_neo4j
