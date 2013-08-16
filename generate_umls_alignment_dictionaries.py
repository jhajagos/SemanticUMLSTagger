__author__ = 'janos'

import text_chopper
import config
import os
import json

try:
    from umls_vocabulary_to_skos import RRFReader, read_file_layout
except ImportError:
    raise ImportError, "Please add UMLS2SKOS/script to your PYTHONPATH"


def logger(text_to_log=""):
    print(text_to_log)


DEFAULT_EXPORT_SABS = ['DSM4',
                        'FMA',
                        'GO',
                        'ICD9CM'
                        'HGNC',
                        'NCBI',
                        'NCI',
                        'OMIM',
                        'RXNORM']

def generate_json_files(sabs_to_export, export_full_sui_dict):
    """Generates json alignment files which are used for alignment"""
    file_layout = read_file_layout(os.path.join(config.umls_to_skos_path,"script/umls_file_layout.json"))

    mrsty = RRFReader(os.path.join(config.umls_rrf_directory, "MRSTY.RRF"), file_layout["MRSTY.RRF"])
    mrconso = RRFReader(os.path.join(config.umls_rrf_directory, "MRCONSO.RRF"), file_layout["MRCONSO.RRF"])
    mrsab = RRFReader(os.path.join(config.umls_rrf_directory, "MRSAB.RRF"), file_layout["MRSAB.RRF"])

    logger("Reading in source information")
    sab_dict = {}

    for row in mrsab:
        sab = row["RSAB"]
        curver = row["CURVER"]

        if curver == 'Y':
            sab_dict[sab] = row

    logger("Reading in semantic types")
    sty_dict = {}
    for row in mrsty:
        cui = row["CUI"]
        sty_dict[cui] = row

    text_chopper_obj = text_chopper.TextChopperProcessor()
    sui_dict = {}
    sui_info_dict = {}

    exact_case_str_dict = {}
    no_case_str_dict = {}
    exact_case_fragment_dict = {}
    no_case_fragment_dict = {}

    logger("Reading in strings to fragment")
    i = 0
    j = 0
    sab_counts = {}
    for row in mrconso:
        sui = row["SUI"]
        umls_str_raw = row["STR"]
        umls_str = text_chopper_obj.join_fragment(text_chopper_obj.clean_broken_words(text_chopper_obj.break_into_words(umls_str_raw)))
        aui = row["AUI"]
        cui = row["CUI"]
        sab = row["SAB"]
        tty = row["TTY"]

        if sab in sabs_to_export:

            if sab in sab_counts:
                sab_counts[sab] += 1
            else:
                sab_counts[sab] = 1

            if export_full_sui_dict:
                aui_dict = {"CUI": cui, "SAB": sab, "TTY": tty, "STR": umls_str, "STY" : sty_dict[cui], "SAB_NAME":
                            sab_dict[sab]["SON"]}
            else:
                aui_dict = None

            if sui in sui_dict:
                sui_dict[sui][aui] = aui_dict
            else:
                sui_dict[sui] = {aui: aui_dict}

                text_fragments = text_chopper_obj.create_joined_fragments_from_text(umls_str_raw)
                for text_fragment in text_fragments:
                    text_fragment_upper = text_fragment.upper()

                    if text_fragment in exact_case_fragment_dict:
                        sui_list = exact_case_fragment_dict[text_fragment]
                        if sui not in sui_list:
                            exact_case_fragment_dict[text_fragment] += [sui]
                    else:
                        exact_case_fragment_dict[text_fragment] = [sui]

                    if text_fragment_upper in no_case_fragment_dict:
                        sui_list = no_case_fragment_dict[text_fragment_upper]
                        if sui not in sui_list:
                            no_case_fragment_dict[text_fragment_upper] += [sui]
                    else:
                        no_case_fragment_dict[text_fragment_upper] = [sui]

                    sui_info_dict[sui] = {"umls_str_original": umls_str_raw, "umls_string": umls_str, "fragment": umls_str.upper()}

            exact_case_str_dict[umls_str] = sui
            umls_str_upper = umls_str.upper()

            if umls_str_upper in no_case_str_dict:
                sui_list = no_case_str_dict[umls_str_upper]
                if sui not in sui_list:
                    no_case_str_dict[umls_str_upper] += [sui]
            else:
                no_case_str_dict[umls_str_upper] = [sui]

            j += 1
        i += 1

        if i % 10000 == 0:
            logger("Read in %s row" % i)

    logger("Read through at total of %s rows" % i)
    logger("Extracted from a total of %s rows" % j)
    logger(sab_counts)
    logger("Read in %s SUIs" % len(sui_dict.keys()))
    logger("Read in %s exact fragments" % len(exact_case_fragment_dict.keys()))

    exact_case_str_dict_json = os.path.join(config.json_directory,"exact_case_str_dict.json")
    logger("Writing %s" % exact_case_str_dict_json)
    fecsd = open(exact_case_str_dict_json, "w")
    json.dump(exact_case_str_dict, fecsd)
    fecsd.close()

    no_case_str_dict_json = os.path.join(config.json_directory,"no_case_str_dict.json")
    logger("Writing %s" % no_case_str_dict_json)
    fncsd = open(no_case_str_dict_json, "w")
    json.dump(no_case_str_dict, fncsd)
    fncsd.close()

    exact_case_fragment_dict_json = os.path.join(config.json_directory, "exact_case_fragment_dict.json")
    logger("Writing %s" % exact_case_fragment_dict_json)
    fecfd = open(exact_case_fragment_dict_json,"w")
    json.dump(exact_case_fragment_dict, fecfd)
    fecfd.close()

    no_case_fragment_dict_json = os.path.join(config.json_directory, "no_case_fragment_dict.json")
    logger("Writing %s" % no_case_fragment_dict_json)
    fncfd = open(no_case_fragment_dict_json, "w")
    json.dump(no_case_fragment_dict, fncfd)
    fncfd.close()

    sui_info_dict_json = os.path.join(config.json_directory, "sui_info_dict.json")
    logger("Writing %s" % sui_info_dict_json)
    fsid = open(sui_info_dict_json,"w")
    json.dump(sui_info_dict, fsid)
    fsid.close()

    if export_full_sui_dict:
        sui_dict_json = os.path.join(config.json_directory, "sui_dict.json")
        logger("Writing %s" % sui_dict_json)
        fsd = open(sui_dict_json,"w")
        json.dump(sui_dict, fsd)
        fsd.close()


if __name__ == "__main__":
    generate_json_files(DEFAULT_EXPORT_SABS, export_full_sui_dict=False)