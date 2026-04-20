import os
from langchain_neo4j import Neo4jGraph
from langchain_groq import ChatGroq
import api_keys


def ask_graph_question(question):

    graph = Neo4jGraph(
        url=os.environ.get("NEO4J_URI"),
        username=os.environ.get("NEO4J_USERNAME"),
        password=os.environ.get("NEO4J_PASSWORD"),
        refresh_schema=True
    )

    llm = ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0
    )

    schema = graph.get_schema

    prompt = f"""
    You are a Neo4j expert.

    Schema:
    {schema}

    Rules:
    - Use ONLY relationships from schema
    - Do NOT invent relationships
    - Use correct property names

    Question: {question}

    Return ONLY Cypher query. No explanation.
    """

    response = llm.invoke(prompt)
    cypher = response.content.strip() #type: ignore

    if cypher.startswith("```"):
        cypher = cypher.replace("```cypher", "").replace("```", "").strip()

    try:
        result = graph.query(cypher)
    except Exception as e:
        print("ERROR:", e)
        return

    answer_prompt = f"""
    Question: {question}
    Data: {result}

    Answer clearly.
    If empty say: No matching metadata found.
    """

    answer = llm.invoke(answer_prompt).content
    print("\nQUESTION:", question)
    print("\nCYPHER:\n", cypher)
    print("\nANSWER:", answer)
    # print("-" * 60)