import os
from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_core.prompts import PromptTemplate
from langchain_groq import ChatGroq

def create_qa_chain(neo4j_uri, neo4j_username, neo4j_password):
    """
    Creates a generalized Graph QA chain. 
    Uses APOC to dynamically detect the schema of the current database.
    """
    
    # 1. Connect to the Graph
    # refresh_schema=True utilizes APOC to map the DB structure automatically
    graph = Neo4jGraph(
        url=neo4j_uri,
        username=neo4j_username,
        password=neo4j_password,
        refresh_schema=True 
    )

    # 2. Initialize the LLM
    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0
    )

    # 3. DYNAMIC CYPHER GENERATION PROMPT
    cypher_template = """
    You are an expert Neo4j developer. Given a user question, create a syntactically correct Cypher query.
    
    SCHEMA INFORMATION:
    {schema}

    CRITICAL RULES:
    1. Use ONLY the labels, relationships, and properties provided in the SCHEMA above.
    2. STRING MATCHING: Always use case-insensitive matching for user input. 
       Example: WHERE toLower(n.name) CONTAINS toLower('search_term')
    3. RELATIONSHIPS: Ensure your query follows the direction defined in the SCHEMA.
    4. PROJECTIONS: Return specific properties (e.g., n.name, n.id) instead of the raw node 'n'.
    5. LIMIT: Limit the result to 20 unless the user specifies a different count.

    Question: {question}
    Cypher Query:"""

    cypher_prompt = PromptTemplate(
        input_variables=["schema", "question"],
        template=cypher_template
    )

    # 4. NATURAL LANGUAGE RESPONSE PROMPT
    qa_template = """
    You are a professional Data Analyst. Answer the question based on the database results provided.

    Question: {question}
    Context (Graph Data): {context}

    INSTRUCTIONS:
    1. If the context is empty [], state that no information was found in the current graph.
    2. If context contains data, summarize it clearly and directly. 
    3. Do not mention "the database" or "the context" in your final answer.

    Final Answer:"""
    
    qa_prompt = PromptTemplate(
        input_variables=["question", "context"],
        template=qa_template
    )

    # 5. Build the Chain
    # graph.get_schema provides the APOC-detected structure
    chain = GraphCypherQAChain.from_llm(
        llm=llm,
        graph=graph,
        verbose=True,
        cypher_prompt=cypher_prompt.partial(schema=graph.get_schema),
        qa_prompt=qa_prompt,
        return_intermediate_steps=True,
        allow_dangerous_requests=True
    )

    return chain

def ask_graph_question(chain, question):
    """
    Executes a question and prints the result.
    """
    try:
        result = chain.invoke({"query": question})
        print(f"\nQUESTION: {question}")
        
        if "intermediate_steps" in result and result["intermediate_steps"]:
            cypher = result["intermediate_steps"][0]
            if isinstance(cypher, dict): 
                cypher = cypher.get("query")
            print(f"GENERATED CYPHER: {cypher}")

        print(f"ANSWER: {result['result']}")
        print("-" * 60)
    except Exception as e:
        print(f"\nERROR executing '{question}': {e}")

if __name__ == "__main__":
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USERNAME", "neo4j")
    pwd = os.environ.get("NEO4J_PASSWORD", "password")

    qa_chain = create_qa_chain(uri, user, pwd)
    
    test_q = [
        "What are the different types of nodes in this graph?",
        "List all unique labels and their relationships.",
        "Which tables are connected to the database 'Teradata'?"
    ]

    for q in test_q:
        ask_graph_question(qa_chain, q)