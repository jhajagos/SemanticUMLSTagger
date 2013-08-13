__author__ = 'janos'

import nltk


class TextChopperProcessor(object):

    def __init__(self):
        self.stop_words = nltk.corpus.stopwords.words("english")
        self.stop_words = [w.upper() for w in self.stop_words]
        self.punctuation = (',', "-", "?", ".", "!", '"', "'", "-", '(', ")", "$", ";", ":", "%", "#", "/", "@","'s","=","+/-","[","]","+","<",">")

    def break_into_sentences(self, text_to_process):
        return nltk.tokenize.sent_tokenize(text_to_process)

    def break_into_words(self, text_to_process):
        return nltk.tokenize.word_tokenize(text_to_process)

    def clean_broken_words(self, list_of_text):
        return [word for word in list_of_text if word not in self.stop_words and word not in self.punctuation]

    def break_into_fragments(self, list_of_text, fragment_length=4):
        word_fragment_list = []
        n = len(list_of_text)
        for i in range(n):
            anchored_word_fragment_list = []

            if i + fragment_length + 1 > n:
                n_frag = n - i
            else:
                n_frag = fragment_length

            for j in range(n_frag):
                    fragment_piece = list_of_text[i: i + j + 1]
                    anchored_word_fragment_list += [fragment_piece]

            word_fragment_list.append(anchored_word_fragment_list)

        return word_fragment_list

    def create_joined_fragments_from_text(self,text_to_process, fragment_length=4):
        words = self.break_into_words(text_to_process)
        cleaned_words = self.clean_broken_words(words)
        cleaned_words_fragmented = self.break_into_fragments(cleaned_words, fragment_length)
        processed_list = []
        for cleaned_words in cleaned_words_fragmented:
            for fragment in cleaned_words:
                joined_fragment = self.join_fragment(fragment)
                if fragment not in processed_list:
                    processed_list.append(joined_fragment)
        return processed_list

    def join_fragment(self, list_of_text):
        return "||".join(list_of_text)

    def create_joined_fragments_from_paragraph(self, text_to_process, is_a_sentence=True, n_fragments=4):
        if is_a_sentence:
            sentences = self.break_into_sentences(text_to_process)
            #print(sentences)
        else:
            sentences = [text_to_process]

        processed_sentences = []

        for sentence in sentences:
            sentence_text = sentence.upper()
            words = self.break_into_words(sentence_text)
            #print(words)
            filtered_words = self.clean_broken_words(words)
            #print(filtered_words)
            fragments = self.break_into_fragments(filtered_words, n_fragments)
            #print(fragments)
            processed_fragments = []
            for starting_fragment in fragments:
                starting_joined_fragment = []
                for continuing_fragment in starting_fragment:
                    starting_joined_fragment.append(self.join_fragment(continuing_fragment))
                processed_fragments += [starting_joined_fragment]

            processed_sentences += [processed_fragments]

        return processed_sentences






