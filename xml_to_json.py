import xml.etree.ElementTree as ET
import json
import os


def xml_to_jsonl(input_path: str, output_path: str):
    tree = ET.parse(input_path)
    root = tree.getroot()

    records = []

    # DATABASE
    source = root.find(".//SOURCE")
    database_name = source.get("DATABASETYPE", "UNKNOWN") if source is not None else "UNKNOWN"

    records.append({
        "type": "node",
        "id": database_name,
        "label": "Database",
        "properties": {"name": database_name}
    })

    # SOURCE TABLES + COLUMNS
    for src in root.findall(".//SOURCE"):
        table_name = src.get("NAME")

        # Table node
        records.append({
            "type": "node",
            "id": table_name,
            "label": "Table",
            "properties": {
                "name": table_name,
                "database": database_name
            }
        })

        # Edge: DB → Table
        records.append({
            "type": "edge",
            "from": database_name,
            "to": table_name,
            "label": "HAS_TABLE"
        })

        # Columns
        for col in src.findall(".//SOURCEFIELD"):
            col_name = col.get("NAME")
            col_id = f"{table_name}.{col_name}"

            records.append({
                "type": "node",
                "id": col_id,
                "label": "Column",
                "properties": {
                    "name": col_name,
                    "table": table_name,
                    "datatype": col.get("DATATYPE"),
                    "precision": col.get("PRECISION"),
                    "scale": col.get("SCALE"),
                    "nullable": col.get("NULLABLE"),
                    "keytype": col.get("KEYTYPE")
                }
            })

            records.append({
                "type": "edge",
                "from": table_name,
                "to": col_id,
                "label": "HAS_COLUMN"
            })

    # TARGET TABLES
    for tgt in root.findall(".//TARGET"):
        table_name = tgt.get("NAME")

        records.append({
            "type": "node",
            "id": table_name,
            "label": "Table",
            "properties": {"name": table_name}
        })

        records.append({
            "type": "edge",
            "from": database_name,
            "to": table_name,
            "label": "HAS_TABLE"
        })

        for col in tgt.findall(".//TARGETFIELD"):
            col_name = col.get("NAME")
            col_id = f"{table_name}.{col_name}"

            records.append({
                "type": "node",
                "id": col_id,
                "label": "Column",
                "properties": {
                    "name": col_name,
                    "table": table_name
                }
            })

            records.append({
                "type": "edge",
                "from": table_name,
                "to": col_id,
                "label": "HAS_COLUMN"
            })

    # TASKS
    for task in root.findall(".//TASK"):
        task_name = task.get("NAME")
        task_type = task.get("TYPE")

        props = {
            "name": task_name,
            "task_type": task_type
        }

        if task_type == "Command":
            vp = task.find(".//VALUEPAIR")
            if vp is not None:
                props["command"] = vp.get("VALUE")

        if task_type == "Session":
            for attr in task.findall(".//ATTRIBUTE"):
                if attr.get("NAME") == "Mapping Name":
                    props["mapping"] = attr.get("VALUE")

        records.append({
            "type": "node",
            "id": task_name,
            "label": "Task",
            "properties": props
        })

    # MAPPING
    for mapping in root.findall(".//MAPPING"):
        map_name = mapping.get("NAME")

        records.append({
            "type": "node",
            "id": map_name,
            "label": "Mapping",
            "properties": {"name": map_name}
        })

    # FLOW (WORKFLOWLINK)
    for link in root.findall(".//WORKFLOWLINK"):
        records.append({
            "type": "edge",
            "from": link.get("FROMTASK"),
            "to": link.get("TOTASK"),
            "label": "DEPENDS_ON"
        })

    # SESSION → MAPPING
    for task in root.findall(".//TASK"):
        if task.get("TYPE") == "Session":
            task_name = task.get("NAME")
            for attr in task.findall(".//ATTRIBUTE"):
                if attr.get("NAME") == "Mapping Name":
                    records.append({
                        "type": "edge",
                        "from": task_name,
                        "to": attr.get("VALUE"),
                        "label": "RUNS_MAPPING"
                    })
    
    # MAPPING → TABLE CONNECTION
    for mapping in root.findall(".//MAPPING"):
        map_name = mapping.get("NAME")

        # SOURCE TABLE
        src = mapping.find(".//SOURCE")
        if src is not None:
            records.append({
                "type": "edge",
                "from": map_name,
                "to": src.get("NAME"),
                "label": "READS"
            })

        # TARGET TABLE
        tgt = mapping.find(".//TARGET")
        if tgt is not None:
            records.append({
                "type": "edge",
                "from": map_name,
                "to": tgt.get("NAME"),
                "label": "WRITES"
            })

    # WRITE JSONL
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"JSONL saved at: {output_path}")

if __name__ == "__main__":
    xml_to_jsonl(
    input_path="files/sample.xml",
    output_path="files/sample.jsonl"
)