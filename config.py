from definition import *

vr = VarsRule()
vr.register(Matcher(".*",".*"), VarsQuerier(all_ansible_querier_factory(), all_neo4j_querier_factory()))
user_def = Definition(rep_node_name="ansible", rep_node_label="SCRIPT", rep_rel_label="USE", relation_label="HAS", vars_label="ANSIBLE_VARS", node_name_rule=DefaultNodeNameRule, vars_rule=vr)
