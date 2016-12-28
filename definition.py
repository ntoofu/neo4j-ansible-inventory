from abc import ABCMeta, abstractmethod, abstractclassmethod
from ansible.inventory.host import Host
from ansible.inventory.group import Group
import uuid, collections, re


class Neo4jNode:
    def __init__(self, label=None, name=None, vars={}, childnodes=[], id=None):
        self.label = label
        self.name = name
        self.vars = vars
        self.childnodes = childnodes
        self.id = id
        self._uuid = uuid.uuid4()
        return

    @property
    def uuid(self):
        return self._uuid


class BaseNodeNameRule(metaclass=ABCMeta):
    def __init__(self, obj):
        if isinstance(obj, Host):
            self.type = self.__class__._type_from_ansible(obj.name, True)
            self.name = self.__class__._name_from_ansible(obj.name, True)
        elif isinstance(obj, Group):
            self.type = self.__class__._type_from_ansible(obj.name, False)
            self.name = self.__class__._name_from_ansible(obj.name, False)
        elif isinstance(obj, Neo4jNode):
            self.type = self.__class__._type_from_neo4j(obj.label, obj.name)
            self.name = self.__class__._name_from_neo4j(obj.label, obj.name)
        else:
            raise TypeError("not support")

    @abstractclassmethod
    def _type_from_ansible(cls, name, is_host):
        pass

    @abstractclassmethod
    def _name_from_ansible(cls, name, is_host):
        pass

    @abstractclassmethod
    def _type_from_neo4j(cls, label, name):
        pass

    @abstractclassmethod
    def _name_from_neo4j(cls, label, name):
        pass

    @property
    @abstractmethod
    def ansible_name(self):
        pass

    @property
    @abstractmethod
    def ansible_is_host(self):
        pass

    @property
    @abstractmethod
    def neo4j_name(self):
        pass

    @property
    @abstractmethod
    def neo4j_label(self):
        pass


class DefaultNodeNameRule(BaseNodeNameRule):

    @classmethod
    def _type_from_ansible(cls, name, is_host):
        return "ANSIBLE_HOST" if is_host else "ANSIBLE_GROUP"

    @classmethod
    def _name_from_ansible(cls, name, is_host):
        return name

    @classmethod
    def _type_from_neo4j(cls, label, name):
        return label

    @classmethod
    def _name_from_neo4j(cls, label, name):
        return name

    @property
    def ansible_name(self):
        return self.name

    @property
    def ansible_is_host(self):
        return True if self.type == "ANSIBLE_HOST" else False

    @property
    def neo4j_name(self):
        return self.name

    @property
    def neo4j_label(self):
        return self.type


class Matcher(object):
    def __init__(self, name_re, label_re):
        self._name_re = re.compile(name_re)
        self._label_re = re.compile(label_re)
        self._hash = (name_re, label_re).__hash__()

    def __hash__(self):
        return self._hash

    def matches(self, name, label):
        return self._name_re.match(name) != None and self._label_re.match(label) != None


class VarsQuerier(object):
    def __init__(self, from_ansible, from_neo4j):
        self._from_ansible = from_ansible
        self._from_neo4j = from_neo4j

    def from_ansible(self, ansible_obj):
        if isinstance(ansible_obj, Host) or isinstance(ansible_obj, Group):
            return self._from_ansible(ansible_obj)
        else:
            raise TypeError("not support")

    def from_neo4j(self, neo4j_id, session):
        return self._from_neo4j(neo4j_id, session)


class VarsRule(object):
    def __init__(self):
        self._rules = {}

    def register(self, matcher, vars_querier):
        if not isinstance(matcher, Matcher) or not isinstance(vars_querier, VarsQuerier):
            raise TypeError("not support")
        if matcher not in self._rules.keys():
            self._rules[matcher] = []
        self._rules[matcher].append(vars_querier)
        return

    def from_ansible(self, name, label, ansible_obj):
        kvs = {}
        for matcher, queiers in self._rules.items():
            if not matcher.matches(name, label):
                continue
            kvs = {k: v for q in queiers for k, v in q.from_ansible(ansible_obj).items()}
        return kvs

    def from_neo4j(self, neo4jnode, session):
        kvs = {}
        for matcher,queiers in self._rules.items():
            if not matcher.matches(neo4jnode.name, neo4jnode.label):
                continue
            kvs = {k: v for q in queiers for k, v in q.from_neo4j(neo4jnode.id, session).items()}
        return kvs


class Definition:
    def __init__(self, rep_node_name="Ansible", rep_node_label="SCRIPT", rep_rel_label="USE", relation_label="HAS", vars_label="ANSIBLE_VARS", node_name_rule=DefaultNodeNameRule, vars_rule=VarsRule()):
        self.representing_node = Neo4jNode(label=rep_node_label, name=rep_node_name)
        self.rep_rel_label = rep_rel_label
        self.relation_label = relation_label
        self.node_name_rule = node_name_rule
        self.vars_rule = vars_rule
        self.vars_label = vars_label


def SimpleAnsibleQuerierFactory(ansible_key, neo4j_key):
    return (lambda ans_obj: {neo4j_key: ans_obj.vars.get(ansible_key, None)})


def SimpleNeo4jQuerierFactory(neo4j_key, ansible_key):
    cache = {}

    def _impl(node_id, session, neo4j_key, ansible_key):
        if(node_id not in cache.keys()):
            result = session.run("MATCH (a) WHERE ID(a) = {0} RETURN a AS property".format(node_id))
            cache[node_id] = result.peek()['property']
        return {ansible_key: cache[node_id].get(neo4j_key, None)}

    return lambda node_id, session: _impl(node_id, session, neo4j_key, ansible_key)


def all_ansible_querier_factory():
    return (lambda ans_obj: dict(ans_obj.vars))


def all_neo4j_querier_factory():
    def _impl(node_id, session):
        result = session.run("MATCH (a) WHERE ID(a) = {0} RETURN a AS property".format(node_id))
        props = result.peek()['property']
        result = session.run("MATCH (a)-[p]->(b) WHERE ID(a) = {0} RETURN p.name AS key, b AS subprop, p.index AS idx ORDER BY TYPE(p), p.index".format(node_id))
        subprops = {}
        for r in result:
            if r["idx"] is None:
                subprops[r["key"]] = dict(r["subprop"])
            else:
                if r["idx"] == 0:
                    subprops[r["key"]] = []
                subprops[r["key"]].append(dict(r["subprop"]))
        return dict(props)
    return _impl

