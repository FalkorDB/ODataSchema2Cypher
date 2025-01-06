import os
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import requests
from falkordb import FalkorDB

def parse_odata_schema(schema):
    """
    This function parses the OData schema and returns entities and relationships.
    """
    entities = {}
    relationships = []

    root = ET.fromstring(schema)
    for entity_type in root.findall(".//EntityType"):
        entity_name = entity_type.get("Name")
        entities[entity_name] = {
            "properties": [prop.get("Name") for prop in entity_type.findall(".//Property")],
            "navigation_properties": [nav.get("Name") for nav in entity_type.findall(".//NavigationProperty")]
        }

    for entity_name, entity in entities.items():
        for nav_prop in entity["navigation_properties"]:
            relationships.append((entity_name, nav_prop))

    return entities, relationships

def generate_cypher_queries(entities, relationships):
    """
    This function generates Cypher queries for entities and relationships.
    """
    queries = []

    for entity_name, entity in entities.items():
        query = f"CREATE (n:{entity_name} {{"
        query += ", ".join([f"{prop}: ${{prop}}" for prop in entity["properties"]])
        query += "})"
        queries.append(query)

    for relationship in relationships:
        query = f"MATCH (a:{relationship[0]}), (b:{relationship[1]}) CREATE (a)-[:RELATED_TO]->(b)"
        queries.append(query)

    return queries

def main():
    """
    This function calls an OData service, parses the schema, and generates Cypher queries.
    """

    # load environment variables
    load_dotenv()
    odata_url = os.getenv("ODATA_URL")
    odata_user = os.getenv("ODATA_USER")
    odata_password = os.getenv("ODATA_PASSWORD")

    # Connect to FalkorDB
    db = FalkorDB(host='localhost', port=6379)
    graph = db.select_graph('priority')

    # Call the OData service pass user name and password
    response = requests.get(odata_url, auth=(odata_user, odata_password))

    # Parse the OData schema
    entities, relationships = parse_odata_schema(response.text)

    # Generate Cypher queries
    cypher_queries = generate_cypher_queries(entities, relationships)

    # Print the Cypher queries
    for query in cypher_queries:
        print(query)

if __name__ == "__main__":
    main()
