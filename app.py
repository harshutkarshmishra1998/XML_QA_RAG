import os
import streamlit as st
from xml_to_json import xml_to_jsonl
from json_to_neo4j import load_graph_from_jsonl
from neo4j_qa import ask_graph_question
import api_keys
import io
import sys

st.set_page_config(page_title="XML Graph QA", layout="wide")

if "graph_loaded" not in st.session_state:
    st.session_state.graph_loaded = False

if "current_file" not in st.session_state:
    st.session_state.current_file = None

uploaded_file = st.file_uploader("Upload XML File", type=["xml"])

if uploaded_file is not None:
    if st.session_state.current_file != uploaded_file.name:
        st.session_state.graph_loaded = False
        st.session_state.current_file = uploaded_file.name

if uploaded_file is not None and not st.session_state.graph_loaded:
    file_name = uploaded_file.name.replace(".xml", "")
    input_path = f"files/{uploaded_file.name}"
    output_path = f"files/{file_name}.jsonl"

    with open(input_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    xml_to_jsonl(input_path, output_path)
    load_graph_from_jsonl(output_path)

    st.session_state.graph_loaded = True

if st.session_state.graph_loaded:
    user_question = st.text_input("Enter your question")

    if st.button("Ask") and user_question:
        buffer = io.StringIO()
        sys.stdout = buffer
        ask_graph_question(user_question)
        sys.stdout = sys.__stdout__
        output = buffer.getvalue()
        st.text_area("Result", output, height=300)