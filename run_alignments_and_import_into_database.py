__author__ = 'janos'

import text_aligner
import csv
import json
import os
import re
import random

def logger(string_to_log=""):
    print(string_to_log)

def run_alignment(input_csv_file_name, output_csv_file_name, column_to_align, columns_to_export=[], table_export_name="",
                  export_json_file_name=None, default_data_type="VarChar(255)", additional_list_of_fields_to_index=[]):
    """Run alignment against a CSV file and export to a database"""

    with open(input_csv_file_name,'rb') as f:
        csv_reader = csv.DictReader(f)

        fragment_dict, exact_dict = text_aligner.load_alignment_dicts()
        text_aligner_obj = text_aligner.TextAligner(fragment_dict, exact_dict)

        tagging_column_list = []
        for column_to_export in columns_to_export:
            tagging_column_list += [(column_to_export, default_data_type)]

        text_aligner_obj.register_tagging_type(tagging_column_list)

        with open(output_csv_file_name, "wb") as fw:
            i = 0
            for row in csv_reader:
                tagging_values = [row[key] for key in row if key in columns_to_export]
                text_to_align = row[column_to_align]

                if i == 0:
                    csv_writer = csv.writer(fw)
                    csv_writer.writerow(text_aligner_obj.column_headers())

                if text_to_align != "" and text_to_align is not None:
                    alignment_result = text_aligner_obj.align_text(text_to_align)
                    text_aligner_obj.export_text_alignment_to_csv(alignment_result, fw, tagging_values)

                i += 1

                if i % 100 == 0:
                    print("Aligned %s" % i)

        list_of_fields_to_index = ["fragment", "exact_sui","sentence_number", "word_number", "fragment_length"]

        complete_list_of_fields_to_index = list_of_fields_to_index + additional_list_of_fields_to_index

        if export_json_file_name is not None:
            add_export_to_json_file(export_json_file_name, table_export_name, output_csv_file_name,
                                    text_aligner_obj.alignment_headers_data_type,
                                    fields_to_index=complete_list_of_fields_to_index)


def export_fragments_to_csv(alignment_dict_json_name, alignment_csv_file_name, field_name_for_id="sui", table_name=None,
                            export_json_file_name=None):
    """Export a CSV file with fragment"""

    with open(alignment_dict_json_name,"r") as f:
        alignment_dict = json.load(f)

    with open(alignment_csv_file_name, "wb") as fw:
        csv_writer = csv.writer(fw)
        csv_writer.writerow(["fragment", field_name_for_id])

        for key in alignment_dict:
            str_ids_to_export = alignment_dict[key]

            for str_id in str_ids_to_export:
                csv_writer.writerow([key, str_id])

    if export_json_file_name is not None:
        add_export_to_json_file(export_json_file_name, table_name, alignment_csv_file_name,
                                data_types_dict={"fragment": "VarChar(255)", field_name_for_id: "VarChar(255)"},
                                fields_to_index=["fragment", field_name_for_id])


def export_sui_info_to_csv(sui_info_json_name, table_export_name, sui_info_csv_file_name, export_json_file_name = None):
    """Pass"""

    column_order = ["sui", "umls_string", "fragment", "umls_str_original"]
    column_data_dict = {"sui": "VarChar(255)", "umls_string": "VarChar(1023)",
                        "umls_str_original": "VarChar(1023)", "fragment": "Varchar(255)"}

    with open(sui_info_json_name, "r") as f:
        sui_info_dict = json.load(f)

    with open(sui_info_csv_file_name,"wb") as fw:
        csv_writer = csv.writer(fw)
        csv_writer.writerow(column_order)

        for key in sui_info_dict:
            row_to_write = [key]
            row = sui_info_dict[key]

            for field_name in column_order[1:]:
                row_to_write += [row[field_name]]
            csv_writer.writerow(row_to_write)

    if export_json_file_name is not None:
        add_export_to_json_file(export_json_file_name, table_export_name, sui_info_csv_file_name, column_data_dict,
                                fields_to_index=["sui","fragment"])


def add_export_to_json_file(json_file_name, table_name, csv_file_to_load, data_types_dict, fields_to_index=[],
                            line_terminator="\\r\\n"):
    full_path_to_json_file = os.path.abspath(json_file_name)

    with open(csv_file_to_load,"r") as f:
        csv_reader = csv.reader(f)

        header = csv_reader.next()

    if os.path.exists(full_path_to_json_file):
        with open(full_path_to_json_file,"r") as f:
            export_dict = json.load(f)
    else:
        export_dict = {}

    export_dict[table_name] = {"data_types_dict": data_types_dict,
                               "fields_to_index": fields_to_index, "csv_file_to_load": csv_file_to_load,
                               "column_order": header,"line_terminator": line_terminator}

    with open(full_path_to_json_file,"w") as fw:
        json.dump(export_dict, fw)


def mysql_export_from_json_file(json_file_name):
    re_extract_field_length = re.compile(r'\(([0-9]+)\)')
    sql_script = ""

    with open(json_file_name, "r") as f:
        export_tables_dict = json.load(f)

    for table in export_tables_dict:
        sql_script += "drop table if exists `%s`;\n\n" % table

        table_information = export_tables_dict[table]
        sql_script += ("create table `%s` (id integer not null auto_increment,\n" % table)

        spacer = "    "

        csv_import_file_name = table_information["csv_file_to_load"]
        full_path_to_csv_import_file_name = os.path.abspath(csv_import_file_name)

        full_path_to_csv_import_file_name = '/'.join(full_path_to_csv_import_file_name.split('\\'))
        columns = table_information["column_order"]
        data_types_dict = table_information["data_types_dict"]
        line_terminator = table_information["line_terminator"]
        i = 0
        for column in columns:
            sql_script += "%s %s %s,\n" % (spacer, column, data_types_dict[column] )

        sql_script += "%sprimary key(id));\n\n" % spacer

        sql_script += "/* Load data into CSV file */\n\n"
        sql_script += "LOAD DATA INFILE '%s' INTO TABLE `%s` \n" % (full_path_to_csv_import_file_name, table)
        sql_script += """FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\0'
        LINES TERMINATED BY '%s' IGNORE 1 LINES\n""" % line_terminator

        set_vars = "("
        for column in columns:
            set_vars += "@%s," % column
        set_vars = set_vars[:-1] + ")"
        sql_script += set_vars + "\n"
        sql_script += "%sset\n" % spacer

        cases_sql = ""
        for column in columns:
            data_type = data_types_dict[column]
            reres = re_extract_field_length.search(data_type)

            if reres is not None:
                left_cut_length = reres.groups()[0]
                var_else = "left(@%s, %s)" % (column, left_cut_length)
            else:
                var_else = "@%s" % column

            case_sql = "%s = case %s when '' then NULL else %s end" % (column, column, var_else)
            cases_sql += "%s %s,\n" % (spacer, case_sql)

        cases_sql = cases_sql[:-2]
        sql_script += cases_sql
        sql_script += ";\n\n"

        indexes_to_create = table_information["fields_to_index"]
        sql_script += ""

        i = 0
        for index_to_create in indexes_to_create:
            index_name = "idx_" + table[0] + str(random.randint(0,999)) + str(i)
            sql_script += "create index %s on %s(%s);\n" % (index_name, table, index_to_create)
            i += 1

    return sql_script


def export_alignments_with_supporting_dbs_to_database():
    """Writes a MySQL load script"""
    pass

if __name__ == "__main__":
    #TODO move this to a separate script, e.g., Run VIVO Alignment

    json_file_dict_name = "E:/data/alignment/analytics_db.json"
    if os.path.exists(json_file_dict_name):
        os.remove(json_file_dict_name)


    print("Exporting Fragments")
    export_fragments_to_csv("./json/no_case_fragment_dict.json", "E:/data/alignment/fragments_sui.csv",table_name="fragment_sui", export_json_file_name=json_file_dict_name)


    print("Exporting SUI info")

    export_sui_info_to_csv("./json/sui_info_dict.json", "sui_info", "E:/data/alignment/sui_info.csv",
                           export_json_file_name=json_file_dict_name)

    print("Aligning abstracts")
    run_alignment("../../workspace/sbu-mi-vivo-tools/reach_abox_2013-08-11.nt.pubmed.csv",
                  "E:/data/alignment/abstracts_tagged.csv","abstract",
                   columns_to_export=['vivoMember', 'pmid', 'vivoMemberURI', 'pub_date', 'articleURI'],
                   export_json_file_name=json_file_dict_name, table_export_name="abstracts_aligned",
                  additional_list_of_fields_to_index=["articleURI", "vivoMemberURI", "articleURI", "vivoMember"])

    print("Aligning titles")
    run_alignment("../../workspace/sbu-mi-vivo-tools/reach_abox_2013-08-11.nt.pubmed.csv",
                  "E:/data/alignment/titles_tagged.csv", "title",
                  columns_to_export=['vivoMember', 'pmid', 'vivoMemberURI', 'pub_date', 'articleURI'],
                  export_json_file_name=json_file_dict_name, table_export_name="titles_aligned",
                  additional_list_of_fields_to_index=["articleURI", "vivoMemberURI", "articleURI", "vivoMember"]
                  )

    sql_script = mysql_export_from_json_file(json_file_dict_name)

    with open("E:/data/alignment/load.sql","w") as f:
        f.write(sql_script)
    print(sql_script)