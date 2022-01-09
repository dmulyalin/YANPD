"""
Neoconnector
============

Module to translate Python structures to Neo4j database content - links and nodes.

Sample usage::

    from neo4japi.neoconnector import neoconnector
    
    # define your data
    data = {
        "nodes": [
            {
                "labels": ["PERSON", "Engineer"],
                "uuid": 1234,
                "age": 40,
                "weight": 70,
                "name": "Gus",
            },
            {
                "labels": ["PERSON", "Teacher"],
                "uuid": 5678,
                "age": 35,
                "weight": 50,
                "name": "Eve",
            },
        ],
        "links": [
            {"type": "KNOWS", "source": 1234, "target": 5678, "since": "1995"},
        ],
    }
    
    # connect to Neo4j database
    conn_data = {
        "url": "http://1.2.3.4:7474/",
        "username": "neo4j",
        "password": "neo4j",
        "database": "neo4j"
    }
    conn = neoconnector(**conn_data)
    
    # push data to graph database
    conn.from_dict(data)
"""
import requests
import logging
import uuid

from typing import List, Tuple, Dict

default_cypher_collection = {
    "node_create": """
CREATE (n:DEFAULT:{labels} $props)
    """,
    "node_merge": """
MERGE (n:DEFAULT {{uuid: $uuid}}) 
SET n += $props
SET n:{labels}
    """,
    "node_get": """
MATCH (n:DEFAULT{labels} {{{props}}}) 
RETURN DISTINCT n
ORDER BY n.{order_by}
SKIP $skip 
LIMIT $limit
    """,
    "node_get_by_uuid": """
MATCH (n:DEFAULT {uuid: $uuid}) USING INDEX n:DEFAULT(uuid) 
RETURN n
    """,
    "node_delete": """
MATCH (n:DEFAULT{labels} {{{props}}})
DETACH DELETE n
    """,
    "node_delete_by_uuid": """
MATCH (n:DEFAULT {uuid: $uuid}) USING INDEX n:DEFAULT(uuid) 
DETACH DELETE n
    """,
    "link_create": """
MATCH (n1:DEFAULT {{uuid: $src}}) USING INDEX n1:DEFAULT(uuid)
MATCH (n2:DEFAULT {{uuid: $tgt}}) USING INDEX n2:DEFAULT(uuid)
CREATE (n1)-[r:{type} $props]->(n2)  
    """,
    "link_merge": """
MATCH (n1 {{uuid: '{src}'}}) USING INDEX n1:DEFAULT(uuid)
MATCH (n2 {{uuid: '{tgt}'}}) USING INDEX n2:DEFAULT(uuid)
MERGE (n1)-[r:{type}]->(n2)
SET r += {{ {properties} }}    
    """,
    "link_get_nondirectional": """
MATCH (source:DEFAULT{s_labels} {{{s_props}}})-[link{r_type} {{{r_props}}}]-(target:DEFAULT{t_labels} {{{t_props}}}) 
RETURN DISTINCT link 
ORDER BY {order_by}
SKIP $skip
LIMIT $limit
    """,
    "link_get_and_update_nondirectional": """
MATCH (source:DEFAULT{s_labels} {{{s_props}}})-[link{r_type} {{{r_props}}}]-(target:DEFAULT{t_labels} {{{t_props}}}) 
SET link += $properties
    """,
    "link_get_and_delete_nondirectional": """
MATCH (source:DEFAULT{s_labels} {{{s_props}}})-[link{r_type} {{{r_props}}}]-(target:DEFAULT{t_labels} {{{t_props}}}) 
DELETE link
    """,
    "create_unique_constraint": """
CREATE CONSTRAINT {property} IF NOT EXISTS ON (n:DEFAULT) ASSERT n.{property} IS UNIQUE
    """,
    "delete_all": """
MATCH (n) DETACH DELETE n
    """,
}


class neoconnector:
    """
    Main neoconnector class.
    
    :param url: (str) Neo4j database HTTP API URL in format ``http://name.com:7484``
    :param databaseName: (str) name of database, default is ``neo4j``
    :param username: (str) database username, default is ``neo4j``
    :param password: (str) database password, default is ``neo4j``
    :param settings: (dict) neoconnector class settings dictionary
    :param enforce_uuid: (bool) if True adds ``uuid`` property unique constraint to Ne04j 
        database and adds ``uuid`` property to each link, default is False
    """
    
    def __init__(
        self, 
        url: str, 
        databaseName: str ="neo4j", 
        username: str ="neo4j", 
        password: str="neo4j", 
        settings:dict ={}, 
        enforce_uuid: bool = False,
    ):
        self.url = url
        self.databaseName = databaseName
        self.credentials = (username, password)
        self.enforce_uuid = enforce_uuid
        self.settings = {
            "requests_timeout": 30,
            "in_one_go": None,  # URI to begin and commit transaction in one go
            **settings,
        }

        self.discover()
        
        if self.enforce_uuid:
            self.run("create_unique_constraint", {"property": "uuid"}, render=True)

    def discover(self) -> None:
        """
        Method to do initial connection attempt and discover HTTP endpoints
        through Discovery API - https://neo4j.com/docs/http-api/current/discovery/
        as well as to for ``in_one_go`` URI for doing full transaction in one go.
        """
        response = requests.get(
            self.url, auth=self.credentials, timeout=self.settings["requests_timeout"]
        )
        self.settings.update(response.json())
        # run sanity check
        if self.settings["neo4j_version"].split(".")[0] != "4":
            raise RuntimeError(
                "{} - Unsupported Neo4j version, only 4.x.x supported".format(
                    self.settings["neo4j_version"]
                )
            )
        # form in_one_go transaction URI
        self.settings["transaction"] = self.settings["transaction"].format(
            databaseName=self.databaseName
        )
        self.settings["in_one_go"] = "{}/commit".format(self.settings["transaction"])

    def tx_start(self, statements: list = []) -> Tuple[int, Dict]:
        """
        Method to start new transaction

        :param statements: (list) list of cypher statements for send method
        :return: (tuple) ID for started transaction and Neo4j response dictionary
        """
        response = self.send(statements=statements, tx=True)
        return int(response["commit"].split("/")[-2]), response

    def tx_rollback(self, tx: int) -> bool:
        """
        Method to rollback/discard active transaction.

        :param tx: (int) transaction ID
        :return: (bool) True on success and False on failure
        """
        response = self.send(statements=[], tx=tx, method="delete")
        return True if response["errors"] == [] else False

    def tx_add(self, statements: list, tx: int) -> Dict:
        """
        Method to run queries inside a transaction

        :param statements: (list) list of cypher statements for send method
        :param tx: (int) transaction ID
        :return: (dict) Neo4j database response dictionary
        """
        return self.send(statements=statements, tx=tx)

    def tx_commit(self, tx: int, statements: list = []) -> Dict:
        """
        Method to commit transaction.

        :param tx: (int) transaction ID
        :param statements: (list) list of cypher statements for send method
        :return: (dict) Neo4j database response dictionary
        """
        return self.send(statements=statements, tx="{}/commit".format(tx))

    def from_dict(self, data: Dict, rate: int = 100, method: str = "create") -> List:
        """
        Function that takes nodes and links dictionary and creates them in Neo4j database.

        Sample data dictionary::

            {
                "nodes": [
                    {"labels": ["PERSON", "Engineer"], "uuid": "1234", "age": 40, "weight": 70, "name": "Gus"},
                    {"labels": ["PERSON", "Teacher"], "uuid": "5678", "age": 35, "weight": 50, "name": "Eve"},
                ],
                "links": [
                    {"type": "KNOWS", "source": "1234", "target": "5678", "since": "1995"},
                ]
            }

        Each node dictionary must contain ``labels`` and ``uuid`` keys, other optional
        key-value pairs used as node properties.

        Each link dictionary must contain ``source``, ``target`` and ``type`` keys, other
        optional key-value pairs used as link properties.

        Data dictionary processing starts with nodes following with links.

        Nodes and links added using rate supplied, starting separate transaction for
        each batch of items.

        If error encountered, ``from_dict`` rolls back current transaction, error raised.

        Performance of this method is relatively slow - to create 1000 nodes with 1000 links 
        between them it takes around 1 second, increasing rate reduces overall time in 
        favor of increasing load on the database.
        
        :param data: (dictionary) dictionary with nodes and links definition
        :param rate: (int) how many items create per transaction, must be >= 1, default 100
        :param method: (str) ``create`` (default) or ``merge``, create is faster, but merge 
            allows to update existing nodes and links
        :return: (list) list of Neo4j database response dictionaries
        """
        ret = []
        tx = None
        rate = max(1, rate)
        try:
            # create nodes
            node_process_method = self.update_node if method == "merge" else self.create_node
            for i in range(0, len(data.get("nodes", [])), rate):
                # create statements list
                statements = [
                    node_process_method(
                        labels=node.pop("labels"),
                        uuid=node.pop("uuid"),
                        properties=node,
                        dry_run=True,
                    )
                    for node in data.get("nodes", [])[i : i + rate]
                ]
                # start and commit transaction
                tx, resp = self.tx_start(statements)
                ret.append(resp)
                ret.append(self.tx_commit(tx))
            # create links
            for i in range(0, len(data.get("links", [])), rate):
                # create statements list
                statements = [
                    self.create_link(
                        source=link.pop("source"),
                        target=link.pop("target"),
                        type=link.pop("type"),
                        properties=link,
                        dry_run=True,
                    )
                    for link in data.get("links", [])[i : i + rate]
                ]
                # start and commit transaction
                tx, resp = self.tx_start(statements)
                ret.append(resp)
                ret.append(self.tx_commit(tx))
        except Exception as e:
            if tx:
                self.tx_rollback(tx)
            raise e
        return ret

    def create_node(
        self, labels: list, uuid: str = "", properties: dict = {}, dry_run: bool = False
    ) -> Dict:
        """
        Method to create single node.

        :param uuid: (str) unique node id
        :param labels: (list) list of labels for the node
        :param properties: (dict) dictionary of node properties
        :param dry_run: (bool) if True will create node in the database, if False (default)
            will return Cypher statement dictionary for send method.
        """
        properties["uuid"] = properties.get("uuid", uuid)

        statement = {
            "statement": default_cypher_collection["node_create"].format(
                labels=":".join(labels)
            ),
            "parameters": {"props": properties},
        }

        if not dry_run:
            return self.send([statement])
        else:
            return statement

    def get_node(
        self,
        uuid: str = None,
        labels: list = [],
        properties: dict = {},
        dry_run: bool = False,
        skip: int = 0,
        limit: int = 1000000,
        order_by: str = "name",
        descending: bool = False,
    ) -> Dict:
        """
        Method to retrieve node(s) information from database identifying node(s) by uuid 
        attribute or combination of labels and properties.

        :param uuid: (str) unique node id
        :param labels: (list) list of labels for the node
        :param properties: (dict) dictionary of node properties
        :param dry_run: (bool) if True will update node in the database, if False (default)
            will return Cypher statement dictionary for send method.
        :param skip: (int) number of nodes to skip, default is 0
        :param limit: (int) limit number of nodes to return, default is 1 000 000
        :param order_by: (str) property to order returned nodes by, default is ``name``
        :param descending: (bool) if True sorting done in reverse order, default is False
        :return: (dict) Response dictionary
        """
        uuid = properties.pop("uuid", uuid)

        if uuid:
            statement = {
                "statement": default_cypher_collection["node_get_by_uuid"],
                "parameters": {"uuid": uuid},
            }
        else:
            statement = {
                "statement": default_cypher_collection["node_get"].format(
                    labels=":" + ":".join(labels) if labels else "",
                    props=", ".join(
                        [
                            '{}: "{}"'.format(k, v)
                            if isinstance(v, str)
                            else "{}: {}".format(k, v)
                            for k, v in properties.items()
                        ]
                    ),
                    order_by=order_by + " DESC" if descending else order_by,
                ),
                "parameters": {"skip": skip, "limit": limit},
            }

        if not dry_run:
            return self.send([statement])
        else:
            return statement

    def delete_node(
        self,
        uuid: str = None,
        labels: list = [],
        properties: dict = {},
        dry_run: bool = False
    ) -> Dict:
        """
        Method to delete node(s) from database identifying node(s) by uuid 
        attribute or combination of labels and properties.

        :param uuid: (str) unique node id
        :param labels: (list) list of labels for the node
        :param properties: (dict) dictionary of node properties
        :param dry_run: (bool) if True will update node in the database, if False (default)
            will return Cypher statement dictionary for send method.
        :return: (dict) Response dictionary
        """
        uuid = properties.pop("uuid", uuid)

        if uuid:
            statement = {
                "statement": default_cypher_collection["node_delete_by_uuid"],
                "parameters": {"uuid": uuid},
            }
        else:
            statement = {
                "statement": default_cypher_collection["node_delete"].format(
                    labels=":" + ":".join(labels) if labels else "",
                    props=", ".join(
                        [
                            '{}: "{}"'.format(k, v)
                            if isinstance(v, str)
                            else "{}: {}".format(k, v)
                            for k, v in properties.items()
                        ]
                    )
                ),
                "parameters": {},
            }

        if not dry_run:
            return self.send([statement])
        else:
            return statement
            
    def update_node(
        self,
        uuid: str = None,
        labels: list = [],
        properties: dict = {},
        dry_run: bool = False,
    ) -> Dict:
        """
        Method to update node(s). Uses ``MERGE`` statement to ensure node exists
        and adds provided properties and labels to it.

        :param uuid: (str) unique node id
        :param labels: (list) list of labels for the node
        :param properties: (dict) dictionary of node properties
        :param dry_run: (bool) if True will update node in the database, if False (default)
            will return Cypher statement dictionary for send method.
        """
        uuid = properties.pop("uuid", uuid)

        statement = {
            "statement": default_cypher_collection["node_merge"].format(
                labels=":".join(labels),
            ),
            "parameters": {"props": properties, "uuid": uuid},
        }

        if not dry_run:
            return self.send([statement])
        else:
            return statement
    
    def create_link(
        self,
        source: str,
        target: str,
        type: str,
        properties: dict = {},
        dry_run: bool = True,
    ) -> Dict:
        """
        Method to create single link.

        :param source: (str) UUID property of source node
        :param target: (str) UUID property of target node
        :param type: (str) link type
        :param properties: (dict) dictionary of link properties, add link UUID by default
        :param dry_run: (bool) if True will create link in the database, if False (default)
            will return Cypher statement dictionary for send method.
        :return: (dict) response dictionary
        """
        if self.enforce_uuid:
            properties.setdefault("uuid", str(uuid.uuid1()))
        statement = {
            "statement": default_cypher_collection["link_create"].format(type=type),
            "parameters": {
                "props": properties,
                "src": source,
                "tgt": target,
            },
        }

        if not dry_run:
            return self.send([statement])
        else:
            return statement

    def get_link(
        self,
        source: str = "",
        source_properties: Dict = {},
        source_labels: List = [],
        target: str = "",
        target_properties: Dict = {},
        target_labels: List = [],
        type: str = "",
        properties: dict = {},
        dry_run: bool = True,
        skip: int = 0,
        limit: int = 1000000,
        order_by: str = None,
        descending: bool = False,
    ) -> Dict:
        """
        Method to retrieve information about link(s) from database.
        
        Link can be identified by source, target, type, properties or 
        combination of them.
        
        :param source: (str) UUID property of source node
        :param source_properties: (dict) properties of source node
        :param source_labels: (list) labels of source node
        :param target: (str) UUID property of target node
        :param target_properties: (dict) properties of target node
        :param target_labels: (list) labels of target node    
        :param type: (str) link type
        :param properties: (dict) dictionary of link properties
        :param dry_run: (bool) if True will create link in the database, if False (default)
            will return Cypher statement dictionary for send method.
        :param skip: (int) number of links to skip, default is 0
        :param limit: (int) limit number of links to return, default is 1 000 000
        :param order_by: (str) property to order returned links by; default is ``ID(link)``
        :param descending: (bool) if True sorting done in reverse order, default is False
        :return: (dict) response dictionary
        """
        if source:
            source_properties["uuid"] = source
        if target:
            target_properties["uuid"] = target
        order_by = "link.{ob}".format(ob=order_by) if order_by else "ID(link)"
        
        statement = {
            "statement": default_cypher_collection["link_get_nondirectional"].format(
                r_type=":{}".format(type) if type else "",
                r_props=self._form_properties(properties),
                s_labels=":" + ":".join(source_labels) if source_labels else "",
                s_props=self._form_properties(source_properties), 
                t_labels=":" + ":".join(target_labels) if target_labels else "",
                t_props=self._form_properties(target_properties),
                order_by=order_by + " DESC" if descending else order_by,
            ),
            "parameters": {"skip": skip, "limit": limit},
        }

        if not dry_run:
            return self.send([statement])
        else:
            return statement

    def update_link(
        self,
        source: str = "",
        source_properties: Dict = {},
        source_labels: List = [],
        target: str = "",
        target_properties: Dict = {},
        target_labels: List = [],
        type: str = "",
        old_properties: dict = {},
        new_properties: dict = {},
        dry_run: bool = True
    ) -> Dict:
        """
        Method to update link(s) properties in database.
        
        Link can be identified by source and target node properties, link type, 
        link properties or combination of them.

        :param old_properties: (dict) dictionary of link old properties to find the link
        :param new_properties: (dict) dictionary of link new properties to update the link    
        :param source: (str) UUID property of source node
        :param source_properties: (dict) properties of source node
        :param source_labels: (list) labels of source node
        :param target: (str) UUID property of target node
        :param target_properties: (dict) properties of target node
        :param target_labels: (list) labels of target node    
        :param type: (str) link type
        :param dry_run: (bool) if True will create link in the database, if False (default)
            will return Cypher statement dictionary for send method.
        :return: (dict) database response dictionary
        """
        if source:
            source_properties["uuid"] = source
        if target:
            target_properties["uuid"] = target
        
        statement = {
            "statement": default_cypher_collection["link_get_and_update_nondirectional"].format(
                r_type=":{}".format(type) if type else "",
                r_props=self._form_properties(old_properties),
                s_labels=":" + ":".join(source_labels) if source_labels else "",
                s_props=self._form_properties(source_properties), 
                t_labels=":" + ":".join(target_labels) if target_labels else "",
                t_props=self._form_properties(target_properties),
            ),
            "parameters": {"properties": new_properties},
        }

        if not dry_run:
            return self.send([statement])
        else:
            return statement
        
    def delete_link(
        self,
        source: str = "",
        source_properties: Dict = {},
        source_labels: List = [],
        target: str = "",
        target_properties: Dict = {},
        target_labels: List = [],
        type: str = "",
        properties: dict = {},
        dry_run: bool = True
    ) -> Dict:
        """
        Method to delete link(s) from database.
        
        Link can be identified by source and target node properties, link type, 
        link properties or combination of them.

        :param properties: (dict) dictionary of link properties to match the link
        :param source: (str) UUID property of source node
        :param source_properties: (dict) properties of source node
        :param source_labels: (list) labels of source node
        :param target: (str) UUID property of target node
        :param target_properties: (dict) properties of target node
        :param target_labels: (list) labels of target node    
        :param type: (str) link type
        :param dry_run: (bool) if True will create link in the database, if False (default)
            will return Cypher statement dictionary for send method.
        :return: (dict) database response dictionary
        """
        if source:
            source_properties["uuid"] = source
        if target:
            target_properties["uuid"] = target
        
        statement = {
            "statement": default_cypher_collection["link_get_and_delete_nondirectional"].format(
                r_type=":{}".format(type) if type else "",
                r_props=self._form_properties(properties),
                s_labels=":" + ":".join(source_labels) if source_labels else "",
                s_props=self._form_properties(source_properties), 
                t_labels=":" + ":".join(target_labels) if target_labels else "",
                t_props=self._form_properties(target_properties),
            ),
            "parameters": {},
        }

        if not dry_run:
            return self.send([statement])
        else:
            return statement
        
    def _form_properties(self, properties: Dict) -> str:
        """
        Helper function to form valid CYpher properties string.
        
        :param properties: (dict) properties dictionary
        :return: (str) valid Cypher properties string to include in query
        """
        return ", ".join(
            [
                '{}: "{}"'.format(k, v)
                if isinstance(v, str)
                else "{}: {}".format(k, v)
                for k, v in properties.items()
            ]
        ) if properties else ""
    
    def run(
        self,
        cypher: str,
        properties: dict = {},
        includeStats: bool = True,
        render: bool = False,
    ) -> Dict:
        """
        Method to run single cypher statement string or statement from
        built-in cypher collection.

        :param cypher: (str) cypher statement string
        :param properties: (dict) parameters dictionary for this statement
        :param includeStats: (bool) if True (default) will include query statistics
            in response
        :param render: (bool) if True will use properties to format cypher statement
            using Python string's ``format`` method
        """
        cypher = default_cypher_collection.get(cypher, cypher)

        if render:
            cypher = cypher.format(**properties)

        statement = {
            "statement": cypher,
            "parameters": properties,
            "includeStats": True,
        }

        return self.send([statement])

    def send(
        self,
        statements,
        tx: [int, bool] = False,
        method: str = "post",
        ret_graph: bool = False,
    ) -> Dict:
        """
        Method to POST provided statements to Neo4j database returning response.

        Sample statements list::

            [{"statement": cypher_string, "parameters": cypher_props, "includeStats" : True}]

        :param statements: (list) list of statement dictionaries [{"statement": "cypher string"}]
        :param tx: (bool or int) transaction id, if True, will start new transaction,
            if int, will use it as transaction id
        :param method: (str) requests method to use - post or delete supported
        :param ret_graph: (bool) if True returns graph data structure in addition to rows, default is False
        :return: Neo4j HTTP API response dictionary
        :raises: Neo4jErrors
        """
        # form request URI
        if tx == True:
            uri = self.settings["transaction"]
        elif tx:
            uri = "{}/{}".format(self.settings["transaction"], tx)
        else:
            uri = self.settings["in_one_go"]

        # run request
        if method.lower() == "post":
            response = requests.post(
                uri,
                json={
                    "statements": statements,
                    "resultDataContents": ["row", "graph"] if ret_graph else ["row"],
                },
                auth=self.credentials,
                timeout=30,
            )
        elif method.lower() == "delete":
            response = requests.delete(
                uri, json={"statements": statements}, auth=self.credentials, timeout=30
            )

        json_response = response.json()

        # check for errors
        if json_response.get("errors"):
            raise Neo4jExceptions(json_response["errors"])

        return json_response


class Neo4jExceptions(Exception):
    """
    Exception that is raised when Neo4j responds to a request with
    one or more error message.
    """

    def __init__(self, errors: List[dict]):
        self.errors = errors

    def __iter__(self):
        return iter(self.errors)
