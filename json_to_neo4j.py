import json
from neo4j import GraphDatabase
import os
import api_keys

class JSONLToNeo4j:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.environ["NEO4J_URI"],
            auth=(
                os.environ["NEO4J_USERNAME"],
                os.environ["NEO4J_PASSWORD"]
            )
        )

    def close(self):
        self.driver.close()

    def load_jsonl(self, jsonl_path: str):
        with open(jsonl_path, "r") as f:
            records = [json.loads(line) for line in f]

        with self.driver.session() as session:

            # CLEAN OLD GRAPH
            session.run("MATCH (n) DETACH DELETE n")

            # CREATE NODES
            for r in records:
                if r["type"] == "node":
                    label = r["label"]
                    node_id = r["id"]
                    props = r.get("properties", {})

                    session.run(
                        f"""MERGE (n:{label} {{id: $id}}) SET n += $props""", #type: ignore
                        id=node_id,
                        props=props
                    )

            # CREATE EDGES
            for r in records:
                if r["type"] == "edge":
                    rel_type = r["label"]

                    # print(f"EDGE: {r['from']} -> {r['to']} ({rel_type})")

                    session.run(
                        f"""MATCH (a {{id: $from_id}}) MATCH (b {{id: $to_id}}) MERGE (a)-[:{rel_type}]->(b)""", #type: ignore
                        from_id=r["from"],
                        to_id=r["to"]
                    )

        print(f"Graph successfully created from: {jsonl_path}")

# MAIN FUNCT
def load_graph_from_jsonl(input_file_path: str):
    loader = JSONLToNeo4j()
    try:
        loader.load_jsonl(input_file_path)
    finally:
        loader.close()

# CLI EN
if __name__ == "__main__":
    load_graph_from_jsonl("files/sample.jsonl")