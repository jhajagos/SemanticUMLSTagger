__author__ = 'janos'
import text_chopper
import json
import csv
import os
import config
import re

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

    def get_start_and_end_position(self, original_text, alignment, position_to_start_searching=0,
                                   sentence_number=None, word_number=None):
        """Given an alignment and a string which the alignment was derived from return the string's match"""

        if sentence_number is not None:
            split_sentence = self.text_chopper.break_into_sentences(original_text)
            sentence = split_sentence[sentence_number]
        else:
            sentence = original_text

        split_alignments = alignment.split("||")

        split_text = [x.upper() for x in self.text_chopper.break_into_words(sentence)]
        re_alignments = [re.compile(sa, re.IGNORECASE) for sa in split_alignments]

        re_matched_positions = {}

        for i in range(len(split_alignments)):
            spu = position_to_start_searching
            mp = []
            while True:
                match = re_alignments[i].search(sentence, spu)
                if match:
                    start_position, end_position = match.start(), match.end()
                    mp.append((start_position, end_position))
                    spu = end_position + 1
                else:
                    break

            re_matched_positions[split_alignments[i]] = mp

        list_matched_position = {}
        for i in range(len(split_alignments)):
            alignment = split_alignments[i]
            spu = 0
            mp = []
            while True:
                if alignment in split_text[spu:]:
                    position_matched = split_text.index(alignment, spu)
                    mp.append(position_matched)
                    spu = position_matched + 1
                else:
                    break
            list_matched_position[alignment] = mp

        logic_matched_position = {}
        for i in range(len(split_alignments)):
            alignment = split_alignments[i]
            lmp = []
            for word in split_text:
                if alignment == word:
                    lmp.append(1)
                else:
                    lmp.append(0)
            logic_matched_position[alignment] = lmp

        number_of_words_to_match = len(split_alignments)

        true_alignment_matches = []
        for first_alignment in list_matched_position[split_alignments[0]]:
            is_true_match = True

            position_of_first_alignment = first_alignment
            for i in range(1, number_of_words_to_match):
                logic_matched = logic_matched_position[split_alignments[i]]
                if logic_matched[position_of_first_alignment + i] == 0:
                    is_true_match = False

            start_position, end_position = re_matched_positions[split_alignments[0]].pop(0)
            if is_true_match:
                for i in range(1, number_of_words_to_match):
                    position_to_evaluate = re_matched_positions[split_alignments[i]].pop(0)
                    while position_to_evaluate[0] < end_position:
                        position_to_evaluate = re_matched_positions[split_alignments[i]].pop(0)
                    end_position = position_to_evaluate[1]
                true_alignment_matches += [(start_position, end_position)]

        return true_alignment_matches

    def annotate_string_with_alignments(self, original_string, alignments_with_annotations):
        """
            Alignments with annotations must follow the following form [((5,10),('<<','>>'', ((11,30),('<<<','>>>>>'))]
            also the alignments must not overlap.
        """

        sorted_alignments_with_annotations = sorted(alignments_with_annotations, key=lambda x: x[0][0])
        annotated_string = original_string
        position_offset = 0
        for alignment in sorted_alignments_with_annotations:
            start_position, end_position = alignment[0]
            start_annotation, end_annotation = alignment[1]
            len_start_annotation = len(start_annotation)
            len_end_annotation = len(end_annotation)
            annotated_string = annotated_string[0:(start_position + position_offset)] + start_annotation \
                               + annotated_string[(start_position + position_offset):(end_position + position_offset)] \
                               + end_annotation + annotated_string[(end_position + position_offset):]

            position_offset += (len_start_annotation + len_end_annotation)

        return annotated_string

    def filter_by_largest_aligned_annotation_first(self, alignments):
        """
        The assumption here is that alignments are in the following structure
        [((34,56),('<<','>>'),'HEART|FAILURE')]

        The only requirement is that the first element of the tuple is a tuple which represents
        where the annotation starts.

        """

        alignment_dicts = {}
        starting_alignment_dict = {}
        for alignment in alignments:
            start_position, end_position = alignment[0]
            alignment_dicts[alignment[0]] = list(alignment[1:])

            if start_position in starting_alignment_dict:
                starting_alignment_dict[start_position] += [end_position]
            else:
                starting_alignment_dict[start_position] = [end_position]

        starting_positions = starting_alignment_dict.keys()
        starting_positions.sort()

        filtered_annotation_list = []
        maximum_position = 0
        for starting_position in starting_positions:
            if starting_position > maximum_position:
                maximum_position = max(starting_alignment_dict[starting_position])

                filtered_annotation_item = [(starting_position, maximum_position)]
                for item in alignment_dicts[(starting_position, maximum_position)]:
                    filtered_annotation_item += [item]

                filtered_annotation_list += [tuple(filtered_annotation_item)]

        return filtered_annotation_list

    def register_tagging_type(self, tagging_types):
        """Consists of [("name","data_type")]"""

        header_next_ith = max([self.headers[h] for h in self.headers]) + 1
        i = 0
        for tagging_type in tagging_types:
            self.headers[tagging_type[0]] = i + header_next_ith
            self.alignment_headers_data_type[tagging_type[0]] = tagging_type[1]
            i += 1

    def column_headers(self):
        """"Names for the columns for export"""
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