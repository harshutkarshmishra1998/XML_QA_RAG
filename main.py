import os
from xml_to_json import xml_to_jsonl
from json_to_neo4j import load_graph_from_jsonl
from neo4j_qa import ask_graph_question
import api_keys

file_name = "sample_2"

input_path = f"files/{file_name}.xml"
output_path = f"files/{file_name}.jsonl"

xml_to_jsonl(input_path, output_path)

load_graph_from_jsonl(output_path)

test_questions = [
    # Basic Metadata
    "Which database contains the table CUSTOMER_RAW?",
    "What are the columns in the table CUSTOMER_FINAL?",
    "List the data types for all columns in CUSTOMER_RAW.",
    
    # Lineage and Data Flow
    "What mappings read from CUSTOMER_RAW?",
    "Which functions (mappings) write to CUSTOMER_FINAL?",
    "What transformation logic is used in the mapping m_Load_Customer?",
    
    # Task and Workflow Logic
    "List all tasks that run the mapping m_Load_Customer.",
    "What is the name of the command task and what script does it run?",
    "Identify the session task that triggers the customer loading process.",
    
    # Advanced / Relationship
    "Trace the flow from the source CUSTOMER_RAW to its final target.",
    "Which task instances are part of the Customer_Workflow worklet?",
    "Are there any filters applied to the data during the load mapping?"
]

for q in test_questions:
    ask_graph_question(q)