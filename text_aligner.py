__author__ = 'janos'
import text_chopper
import json
import csv
import os
import config


def load_alignment_dicts():
    """A helper function for loading text alignment"""
    f = open(os.path.join(config.json_directory, "no_case_fragment_dict.json"), "r")
    no_case_fragment_dict = json.load(f)
    f.close()

    f = open(os.path.join(config.json_directory, "no_case_str_dict.json"), "r")
    no_case_str_dict = json.load(f)
    f.close()
    return no_case_fragment_dict, no_case_str_dict


class TextAligner(object):
    """A class for aligning text against a source vocabulary dictionary"""

    def __init__(self, alignment_dict, exact_alignment_dict={}):
        self.alignment_dict = alignment_dict
        self.text_chopper = text_chopper.TextChopperProcessor()
        self.headers = {"sentence_number": 0, "word_number": 1, "fragment_length": 2, "fragment": 3, "sentence": 4,
                        "n_suis": 5, "exact_sui": 6}
        self.maximum_expansion = 100

        self.alignment_headers_data_type = {"sentence_number": "Int", "word_number": "Int", "fragment_length": "Int",
                                  "fragment": "VarChar(255)", "sentence": "Text",
                        "n_suis": "Int", "exact_sui": "VarChar(255)"}

        self.exact_alignment_dict = exact_alignment_dict

    def align_text(self, text):
        """Main method for text alignment"""
        h = 0
        alignment_results = []
        for sentence in self.text_chopper.break_into_sentences(text):
            fragment_lists = self.text_chopper.create_joined_fragments_from_paragraph(sentence)
            for fragment_list in fragment_lists:
                last_fragment_k = None
                j = 0
                for fragments in fragment_list:
                    k = 0
                    for fragment in fragments:
                        fragment = fragment.upper()
                        if fragment in self.alignment_dict:
                            if fragment in self.exact_alignment_dict:
                                exact_sui = self.exact_alignment_dict[fragment][0]
                            else:
                                exact_sui = None
                            alignment_results += [[h, j, k + 1, fragment, sentence,
                                                   len(self.alignment_dict[fragment]), exact_sui]]
                        k += 1
                    j += 1
            h += 1
        return alignment_results

    def register_tagging_type(self,tagging_types):
        """Consist of [("name","data_type")]"""

        header_next_ith = max([self.headers[h] for h in self.headers]) + 1
        i = 0
        for tagging_type in tagging_types:
            self.headers[tagging_type[0]] = i + header_next_ith
            self.alignment_headers_data_type[tagging_type[0]] = tagging_type[1]
            i += 1

    def column_headers(self):
        "Names for the columns for export"
        numeric_dict = {}
        for key in self.headers:
            numeric_dict[self.headers[key]] = key

        header_row = []
        for i in range(len(numeric_dict.keys())):
            header_row += [numeric_dict[i]]
        return header_row


    def export_text_alignment_to_csv(self, alignment_results, file_pointer, tagging_list=[]):
        """For exporting text alignment to CSV"""
        csv_writer = csv.writer(file_pointer)

        for row in alignment_results:
            row += tagging_list
            csv_writer.writerow(row)