__author__ = 'janos'

import text_aligner
import csv
import json
import os

def logger(string_to_log=""):
    print(string_to_log)


def run_alignment(input_csv_file_name, output_csv_file_name, column_to_align, columns_to_export=[], table_export_name = "", export_json_file_name=None, default_data_type="VarChar(255"):
    with open(input_csv_file_name,'rb') as f:
        csv_reader = csv.DictReader(f)

        fragment_dict, exact_dict = text_aligner.load_alignment_dicts()
        text_aligner_obj = text_aligner.TextAligner(fragment_dict, exact_dict)

        tagging_column_list = []
        for column_to_export in columns_to_export:
            tagging_column_list = [(column_to_export, default_data_type)]

        text_aligner_obj.register_tagging_type(tagging_column_list)

        with open(output_csv_file_name, "wb") as fw:
            i = 0
            for row in csv_reader:
                tagging_values = [cell for cell in row if cell in columns_to_export]
                alignment_result = text_aligner_obj.align_text(row[column_to_align])

                if i == 0:
                    export_header = True
                else:
                    export_header = False

                text_aligner_obj.export_text_alignment_to_csv(alignment_result, fw, export_header, tagging_values)

                i += 1

        if export_json_file_name is not None:
            add_export_to_json_file(export_json_file_name, table_export_name, input_csv_file_name,
                                    text_aligner_obj.alignment_headers_data_type, [])

def add_export_to_json_file(json_file_name, table_name, csv_file_to_load, data_types_dict, fields_to_index=[]):
    full_path_to_json_file = os.path.abspath(json_file_name)
    if os.path.exists(full_path_to_json_file):
        with open(full_path_to_json_file,"r") as f:
            export_dict = json.load(f)
    else:
        export_dict = {}

    export_dict[table_name] = {"data_types_dict": data_types_dict,
                               "fields_to_index": fields_to_index, "csv_file_to_load": csv_file_to_load}

    with open(full_path_to_json_file,"w") as fw:
        json.dump(export_dict, fw)


def write_sql_export_from_json_file(json_file_name, sql_db_script_name):
    pass



def export_alignments_with_supporting_dbs_to_database():
    """Writes a MySQL load script"""