import os
import re
import tqdm
import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from falkordb import FalkorDB

def parse_odata_schema(schema):
    """
    This function parses the OData schema and returns entities and relationships.
    """
    entities = {}
    relationships = {}

    root = ET.fromstring(schema)

    # Define namespaces
    namespaces = {
        'edmx': "http://docs.oasis-open.org/odata/ns/edmx",
        'edm': "http://docs.oasis-open.org/odata/ns/edm"
    }

    schema_element = root.find(".//edmx:DataServices/edm:Schema", namespaces)
    entity_types = schema_element.findall("edm:EntityType", namespaces)
    for entity_type in tqdm.tqdm(entity_types, "Parsing OData schema"):
        entity_name = entity_type.get("Name")
        entities[entity_name] = {prop.get("Name"): prop.get("Type") for prop in entity_type.findall("edm:Property", namespaces)}
        description = entity_type.findall("edm:Annotation", namespaces)
        if len(description) == 1:
            entities[entity_name]["description"] = description[0].get("String").replace("'", "\\'")

        for rel in entity_type.findall("edm:NavigationProperty", namespaces):
            if rel.get("Name") not in relationships:
                relationships[rel.get("Name")] = []    
            relationships[rel.get("Name")].append({
                "from": entity_name,
                "to": re.findall("Priority.OData.(\\w+)\\b", rel.get("Type"))[0]
            })

    return entities, relationships

def generate_cypher_queries(entities, relationships):
    """
    This function generates Cypher queries for entities and relationships.
    """
    entities_queries = []
    relationships_queries = []

    for entity_name, props in tqdm.tqdm(entities.items(), "Generating create entity Cypher queries"):
        query = f"CREATE (n:{entity_name} {{"
        query += ", ".join([f"{key}: '{value}'" for key, value in props.items()])
        query += "})"
        entities_queries.append(query)

    for relationship_name, relationships in tqdm.tqdm(relationships.items(), "Generating create relationship Cypher queries"):
        for relationship in relationships:
            query = f"MATCH (a:{relationship["from"]}), (b:{relationship["to"]}) CREATE (a)-[:{relationship_name}]->(b)"
            relationships_queries.append(query)

    return entities_queries, relationships_queries

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
    print("Downloading OData schema...")
    response = requests.get(odata_url, auth=(odata_user, odata_password))

    # Parse the OData schema
    print("Parsing OData schema...")
    entities, relationships = parse_odata_schema(response.text)

    # Generate Cypher queries
    entities_queries, relationships_queries = generate_cypher_queries(entities, relationships)

    # Run the Create entities Cypher queries
    for query in tqdm.tqdm(entities_queries, "Creating entities"):
        res = graph.query(query)

    # Run the Create relationships Cypher queries
    for query in tqdm.tqdm(relationships_queries, "Creating relationships"):
        res = graph.query(query)

if __name__ == "__main__":
    main()
