import os
# Use langchain_neo4j for both the Graph and the QA Chain
from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_core.prompts import PromptTemplate
from langchain_groq import ChatGroq
import api_keys

def create_qa_chain(
    neo4j_uri: str,
    neo4j_username: str,
    neo4j_password: str
):
    """
    Create a Graph QA chain using Groq Llama model + Neo4j.
    We bypass the APOC requirement by providing a manual schema.
    """

    # 1. Connect to Graph
    graph = Neo4jGraph(
        url=neo4j_uri,
        username=neo4j_username,
        password=neo4j_password,
        refresh_schema=False
    )

    # 2. Define your schema manually since APOC is missing
    # Direction and structure are critical here
    manual_schema = """
    Node Properties:
    - Table {id, name, database}
    - Mapping {id, name}
    - Task {id, name, task_type, mapping}
    - Column {id, name, table, datatype}
    - Database {id, name}

    Relationships:
    - (:Mapping)-[:READS]->(:Table)
    - (:Mapping)-[:WRITES]->(:Table)
    - (:Task)-[:RUNS_MAPPING]->(:Mapping)
    - (:Table)-[:HAS_COLUMN]->(:Column)
    - (:Database)-[:HAS_TABLE]->(:Table)
    """

    # 3. Initialize LLM
    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0
    )

    # 4. CUSTOM CYPHER PROMPT
    cypher_template = """
    You are a Neo4j expert. Given an input question, create a syntactically correct Cypher query.
    
    SCHEMA:
    {schema}

    RULES:
    1. Use ONLY the relationship types and property names provided in the schema.
    2. DIRECTION MATTERS: 
       - (Task)-[:RUNS_MAPPING]->(Mapping)
       - (Mapping)-[:WRITES]->(Table)
       - (Mapping)-[:READS]->(Table)
       - (Database)-[:HAS_TABLE]->(Table)
       - (Table)-[:HAS_COLUMN]->(Column)
    3. If asked for "functions", search for Task nodes (specifically task_type 'Session').
    4. Match on node property `id` or `name`. If the user mentions a specific name like 'm_Load_Customer', try to match on `m.id` first then `m.name`.
    5. COLUMNS: Do not look for a 'columns' property. Use the (Table)-[:HAS_COLUMN]->(Column) relationship.
    6. DATABASES: Use (Database)-[:HAS_TABLE]->(Table) to find which DB owns a table.
    7. VERY IMPORTANT: Always RETURN specific properties like `t.name` or `m.name` instead of the whole node.
    8. Output ONLY the raw Cypher query. No markdown, no 'cypher' prefix, no explanations.

    Question: {question}
    Cypher Query:"""

    cypher_prompt = PromptTemplate(
        input_variables=["schema", "question"],
        template=cypher_template
    )

    # 5. CUSTOM QA PROMPT
    qa_template = """
    You are an expert data analyst assistant. 
    You have been provided with a question and the raw results from a Neo4j database.
    
    Question: {question}
    Context (Database Results): {context}

    Instructions:
    1. Look closely at the Context. If a value (like 'Teradata', 's_Load_Customer', or 'm_Load_Customer') is present, it is the correct answer to the question.
    2. Do NOT say the information is missing if there are values in the context.
    3. In Informatica terms, 'Tasks' or 'Sessions' are the 'functions' the user is asking about.
    4. If 't.name' is 's_Load_Customer', then 's_Load_Customer' is the answer.
    5. If the context is strictly empty [], then say you couldn't find that information.
    6. Be concise and direct.

    Answer:"""
    
    qa_prompt = PromptTemplate(
        input_variables=["question", "context"],
        template=qa_template
    )

    # 6. Build the Chain
    chain = GraphCypherQAChain.from_llm(
        llm=llm,
        graph=graph,
        verbose=True,
        cypher_prompt=cypher_prompt.partial(schema=manual_schema),
        qa_prompt=qa_prompt,
        return_intermediate_steps=True,
        allow_dangerous_requests=True
    )

    return chain

def ask_graph_question(
    chain,
    question: str
):
    """
    Executes a single question against the provided chain and prints formatted output.
    """
    try:
        result = chain.invoke({"query": question})
        
        print("\n" + "="*60)
        print(f"❓ QUESTION: {question}")

        if "intermediate_steps" in result:
            steps = result["intermediate_steps"]
            query = "Unknown"
            if len(steps) > 0:
                if isinstance(steps[0], dict):
                    query = steps[0].get("query", "No query key")
                else:
                    query = str(steps[0])
            
            print(f"🔍 CYPHER: {query}")

        print(f"💡 RESULT: {result['result']}")
        print("="*60)
    except Exception as e:
        print(f"\n❌ ERROR for question '{question}': {e}")

if __name__ == "__main__":
    # 1. Initialize variables from environment
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USERNAME", "neo4j")
    pwd = os.environ.get("NEO4J_PASSWORD", "password")

    # 2. Setup Chain once for all questions
    print("🔄 Initializing QA Chain...")
    qa_chain = create_qa_chain(uri, user, pwd)

    # 3. Define Test Questions
    test_questions = [
        "Which functions write to CUSTOMER_FINAL?",
        "What mappings read from CUSTOMER_RAW?",
        "List all tasks that run the mapping m_Load_Customer.",
        "Which database contains the table CUSTOMER_RAW?",
        "What are the columns in the table CUSTOMER_FINAL?"
    ]

    # 4. Loop and run
    print("\n🚀 Starting Batch QA Test Execution...\n")
    for q in test_questions:
        ask_graph_question(qa_chain, q)