import json
import os
from pathlib import Path


def get_last_id(source_list, id_column_name="id"):
    return max(source_list, key=lambda x: x[id_column_name], default={id_column_name: 0})[id_column_name]


def read_jsons(dir_path):
    result = []
    json_files = [file for file in Path(dir_path).glob("**/*.json")]
    for json_file in json_files:
        with open(json_file) as f:
            data = json.load(f)
            result += data

    return result


def write_to_file(data, path):
    if not data:
        return
    create_dirs_and_file_paths(path)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def write_to_csv(df, path):
    if len(df) == 0:
        return
    create_dirs_and_file_paths(path)
    df.to_csv(path, index=False)


def write_to_xml(xml_data_string, path):
    if not xml_data_string:
        return
    create_dirs_and_file_paths(path)
    with open(path, "w") as f:
        f.write(xml_data_string)


def create_dirs_and_file_paths(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
