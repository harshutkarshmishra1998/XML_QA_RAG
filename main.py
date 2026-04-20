import os
import json
from xml_to_json import xml_to_jsonl
from json_to_neo4j import load_graph_from_jsonl
from neo4j_qa import create_qa_chain, ask_graph_question

def main():
    # ---------------------------------------------------------
    # 1. CONFIGURATION
    # ---------------------------------------------------------
    input_xml = "files/sample_2.xml"
    intermediate_jsonl = "files/sample_2.jsonl"
    
    # Environment variables (Fallback to defaults for local dev)
    neo4j_uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.environ.get("NEO4J_USERNAME", "neo4j")
    neo4j_password = os.environ.get("NEO4J_PASSWORD", "password")

    print("🌟 Starting End-to-End XML-to-Graph QA Workflow 🌟\n")

    # ---------------------------------------------------------
    # 2. TRANSFORM: XML -> JSONL
    # ---------------------------------------------------------
    print(f"Step 1: Transforming {input_xml} to JSONL...")
    try:
        xml_to_jsonl(input_xml, intermediate_jsonl)
        print(f"✅ Created intermediate file: {intermediate_jsonl}")
    except Exception as e:
        print(f"❌ Error during XML transformation: {e}")
        return

    # ---------------------------------------------------------
    # 3. LOAD: JSONL -> NEO4J
    # ---------------------------------------------------------
    print("\nStep 2: Loading data into Neo4j...")
    try:
        load_graph_from_jsonl(intermediate_jsonl)
        print("✅ Neo4j database populated successfully.")
    except Exception as e:
        print(f"❌ Error during Neo4j ingestion: {e}")
        return

    # ---------------------------------------------------------
    # 4. QA: EXECUTE BATCH TESTS
    # ---------------------------------------------------------
    print("\nStep 3: Initializing QA Chain and Running Tests...")
    try:
        qa_chain = create_qa_chain(neo4j_uri, neo4j_user, neo4j_password)
        
        # Categorized Test Questions: Easy -> Medium -> Hard
        test_questions = [
            # EASY: Simple Node/Property Lookups
            "Which database contains the table named 'CUSTOMER_RAW'?",
            "List all column names for the table 'CUSTOMER_FINAL'.",
            
            # MEDIUM: 1-2 Hop Relationships
            "Identify any mappings that have a READS relationship with 'CUSTOMER_RAW'.",
            "Which tasks are configured to run the mapping 'm_Load_Customer'?",
            
            # HARD: Complex End-to-End Lineage
            "Which session tasks write data into the 'CUSTOMER_FINAL' table?",
            "Trace the lineage path from the 'Teradata' database node to the 'CUSTOMER_FINAL' table. What mapping connects them?",
            "For the mapping 'm_Load_Customer', list every column name in the tables it writes to."
        ]

        print("\n🚀 Starting Batch QA Test Execution...\n")
        for question in test_questions:
            ask_graph_question(qa_chain, question)
            
        print("\n✨ All tests completed successfully.")
    except Exception as e:
        print(f"❌ Error during QA execution: {e}")

if __name__ == "__main__":
    main()