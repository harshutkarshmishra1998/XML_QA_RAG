import os
import json
from xml_to_json import xml_to_jsonl
from json_to_neo4j import load_graph_from_jsonl
from neo4j_qa import create_qa_chain, ask_graph_question
import api_keys

file_name = "sample_2"

input_path=f"files/{file_name}.xml"
output_path=f"files/{file_name}.jsonl"
uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
user = os.environ.get("NEO4J_USERNAME", "neo4j")
pwd = os.environ.get("NEO4J_PASSWORD", "password")

xml_to_jsonl(input_path, output_path)
load_graph_from_jsonl(output_path)

qa_chain = create_qa_chain(uri, user, pwd)

test_questions = [
    "Which functions write to CUSTOMER_FINAL?",
    "What mappings read from CUSTOMER_RAW?",
    "List all tasks that run the mapping m_Load_Customer.",
    "Which database contains the table CUSTOMER_RAW?",
    "What are the columns in the table CUSTOMER_FINAL?"
]

for q in test_questions:
    ask_graph_question(qa_chain, q)