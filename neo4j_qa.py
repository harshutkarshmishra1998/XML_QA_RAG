import os
from langchain_community.graphs import Neo4jGraph
from langchain_community.chains.graph_qa.cypher import GraphCypherQAChain
from langchain_core.prompts import PromptTemplate
from langchain_groq import ChatGroq
import api_keys


def create_qa_chain(
    neo4j_uri: str,
    neo4j_username: str,
    neo4j_password: str
):
    """
    Create a Graph QA chain using Groq Llama model + Neo4j
    """

    # -------------------------
    # CONNECT GRAPH
    # -------------------------
    graph = Neo4jGraph(
        url=neo4j_uri,
        username=neo4j_username,
        password=neo4j_password,
        refresh_schema=True
    )

    # -------------------------
    # LLM (Groq - Llama)
    # -------------------------
    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0
    )

    # -------------------------
    # CUSTOM CYPHER PROMPT
    # -------------------------
    cypher_prompt = PromptTemplate(
        input_variables=["schema", "question"],
        template="""
    You are an expert in Neo4j Cypher queries.

    Graph Schema:
    {schema}

    Rules:
    - Use ONLY the given schema
    - Do NOT hallucinate labels or relationships
    - Always return valid Cypher
    - Use node property `id` for matching

    Question:
    {question}
    """
    )


    # -------------------------
    # CREATE QA CHAIN
    # -------------------------
    chain = GraphCypherQAChain.from_llm(
        llm=llm,
        graph=graph,
        verbose=True,
        cypher_prompt=cypher_prompt,
        return_intermediate_steps=True,
        allow_dangerous_requests=True
    )

    return chain


# -------------------------
# MAIN FUNCTION
# -------------------------
def ask_graph_question(
    question: str,
    neo4j_uri: str,
    neo4j_username: str,
    neo4j_password: str
):
    """
    Ask question to graph using LLM
    """

    chain = create_qa_chain(
        neo4j_uri,
        neo4j_username,
        neo4j_password
    )

    result = chain.invoke({"query": question})

    print("\n🧠 QUESTION:")
    print(question)

    print("\n🔍 GENERATED CYPHER:")
    print(result["intermediate_steps"][0]["query"])

    print("\n📊 RESULT:")
    print(result["result"])

    return result


# -------------------------
# CLI TEST
# -------------------------
if __name__ == "__main__":
    ask_graph_question(
        question="Which functions write to CUSTOMER_FINAL?",
        neo4j_uri=os.environ["NEO4J_URI"],
        neo4j_username=os.environ["NEO4J_USERNAME"],
        neo4j_password=os.environ["NEO4J_PASSWORD"]
    )