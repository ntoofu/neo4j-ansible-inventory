from abc import ABCMeta, abstractmethod, abstractclassmethod
from ansible.inventory.host import Host
from ansible.inventory.group import Group
import uuid, collections


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
        self._hash = (name_re, label_re)

    def __hash__(self):
        return self._hash

    def matches(self, neo4j_node):
        return self._name_re.match(neo4j_node.name) != None and self._label_re.match(neo4j_node.label) != None


class RetrieveVar(object):
    def __init__(self, key, from_ansible, from_neo4j):
        self._key = key
        self._from_ansible = from_ansible
        self._from_neo4j = from_neo4j

    def from_ansible(self, ansible_obj):
        if isinstance(ansible_obj, Host) or isinstance(ansible_obj, Group):
            return {self._key: self._from_ansible(ansible_obj.vars)}
        else:
            raise TypeError("not support")

    def from_neo4j(self, neo4j_id, session):
        return {self._key: self._from_ansible(neo4j_id, session)}

class VarsRule(object):
    def __init__(self):
        self._rules = {}

    def register(self, matcher, retrievevar):
        if not isinstance(matcher, Matcher) or not isinstance(retrievevar, RetrieveVar):
            raise TypeError("not support")
        if matcher not in self._rules.keys():
            self._rules[matcher] = []
        self._rules[matcher].append(retrievevar)
        return

    def from_ansible(self, name, label, ansible_obj):
        kvs = {}
        for k,v in self._rules.items():
            if not k.matches(name, label):
                continue
            kv = v.from_ansible(ansible_obj)
            kvs[kv.k] = v
        return kvs

    def from_neo4j(self, neo4jnode, session):
        kvs = {}
        for k,v in self._rules.items():
            if not k.matches(neo4jnode.name, neo4jnode.label):
                continue
            kv = v.from_neo4j(neo4jnode.id, session)
            kvs[kv.k] = v
        return kvs


class Definition:
    def __init__(self, rep_node_name="Ansible", rep_node_label="SCRIPT", rep_rel_label="USE", relation_label="HAS", vars_label="ANSIBLE_VARS", node_name_rule=DefaultNodeNameRule, vars_rule=VarsRule()):
        self.representing_node = Neo4jNode(label=rep_node_label, name=rep_node_name)
        self.rep_rel_label = rep_rel_label
        self.relation_label = relation_label
        self.node_name_rule = node_name_rule
        self.vars_rule = vars_rule
        self.vars_label = vars_label
