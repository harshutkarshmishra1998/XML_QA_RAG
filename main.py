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

# For Sample 1
# test_questions = [
#     # Basic Metadata
#     "Which database contains the table CUSTOMER_RAW?",
#     "What are the columns in the table CUSTOMER_FINAL?",
#     "List the data types for all columns in CUSTOMER_RAW.",
    
#     # Lineage and Data Flow
#     "What mappings read from CUSTOMER_RAW?",
#     "Which functions (mappings) write to CUSTOMER_FINAL?",
#     "What transformation logic is used in the mapping m_Load_Customer?",
    
#     # Task and Workflow Logic
#     "List all tasks that run the mapping m_Load_Customer.",
#     "What is the name of the command task and what script does it run?",
#     "Identify the session task that triggers the customer loading process.",
    
#     # Advanced / Relationship
#     "Trace the flow from the source CUSTOMER_RAW to its final target.",
#     "Which task instances are part of the Customer_Workflow worklet?",
#     "Are there any filters applied to the data during the load mapping?"
# ]

# For Sample 2
test_questions = [
    # EASY: Basic Node & Property Lookups
    "What is the name of the database mentioned in the file?",
    "List all columns in the table ALV_REF_TRN_GEOLOAD_S.",
    "What is the data type and precision of the column TRANS_ID_SK?",
    "Which table belongs to the Teradata database?",
    
    # MEDIUM: Relationships and Data Flow
    "Which mapping reads from the table ALV_REF_TRN_GEOLOAD_S?",
    "List all tasks that are categorized as 'Session' types.",
    "Which mapping is executed by the task s_m_ALV_99921_TRN_GET_WRCNTR_SK_CLLI?",
    "Find all columns that have a key type of 'NOT A KEY'.",
    
    # HARD: Complex Dependencies and Logic
    "Which task depends on the completion of s_m_ALV_99921_TRN_GET_WRCNTR_SK_CLLI?",
    "Trace the dependency: What task must run before wklt_LOAD_LIS_WRCNTR_UPD can start?",
    "List all unique mapping names that write to any table in this graph.",
    "Find the full path of tasks leading to s_m_ALV_999_FINAL_ACCESS_LINE_LIS_HST_UPD."
]

for q in test_questions:
    ask_graph_question(qa_chain, q)