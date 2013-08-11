
import unittest
import sys
import text_chopper
import text_aligner

sys.path.append("../")


class TestTextChopper(unittest.TestCase):
    def setUp(self):
        self.text_to_break_up = """Cell senescence is a process of irreversible arrest of cell proliferation and plays an important role in tumor suppression. Recent studies showed that Wnt inhibition is a trigger of cellular senescence. Using methods of reverse genetics, we recently identified VentX, a human homolog of the vertebrate Xenopus Vent family of homeobox genes, as a novel Wnt repressor and a putative tumor suppressor in lymphocytic leukemia. Here, we show that VentX is a direct transcriptional activator of p53-p21 and p16ink4a-Rb tumor suppression pathways. Ectopic expression of VentX in cancer cells caused an irreversible cell cycle arrest with a typical senescence-like phenotype. Conversely, inhibition of VentX expression by RNA interference ameliorated chemotherapeutic agent-induced senescence in lymphocytic leukemia cells. The results of our study further reveal the mechanisms underlying tumor suppression function of VentX and suggest a role of VentX as a potential target in cancer prevention and treatment."""

    def testText(self):
        tcp = text_chopper.TextChopperProcessor()
        result = tcp.break_into_sentences(self.text_to_break_up)
        self.assertEqual(result.__class__, list)
        self.assertEqual(len(result), 7)

    def testWordExtract(self):
        tcp = text_chopper.TextChopperProcessor()
        sentences = tcp.break_into_sentences(self.text_to_break_up)
        result = tcp.break_into_words(sentences[0])

        self.assertEqual(result.__class__, list)

    def testX(self):
        tcp = text_chopper.TextChopperProcessor()
        result1 = tcp.create_joined_fragments_from_text("Sulfanate")
        self.assertEquals(["Sulfanate"],result1)

        result2 = tcp.create_joined_fragments_from_text("Hydrogen bonded atom")
        self.assertEquals(["Hydrogen","Hydrogen||bonded","Hydrogen||bonded||atom","bonded","bonded||atom","atom"], result2)


class TextAligner(unittest.TestCase):
    def setUp(self):
        self.text_to_align = """Cell senescence is a process of irreversible arrest of cell proliferation and plays an important role in tumor suppression. Recent studies showed that Wnt inhibition is a trigger of cellular senescence. Using methods of reverse genetics, we recently identified VentX, a human homolog of the vertebrate Xenopus Vent family of homeobox genes, as a novel Wnt repressor and a putative tumor suppressor in lymphocytic leukemia. Here, we show that VentX is a direct transcriptional activator of p53-p21 and p16ink4a-Rb tumor suppression pathways. Ectopic expression of VentX in cancer cells caused an irreversible cell cycle arrest with a typical senescence-like phenotype. Conversely, inhibition of VentX expression by RNA interference ameliorated chemotherapeutic agent-induced senescence in lymphocytic leukemia cells. The results of our study further reveal the mechanisms underlying tumor suppression function of VentX and suggest a role of VentX as a potential target in cancer prevention and treatment."""
        self.no_case_fragment_dict, self.no_case_str_dict = text_aligner.load_alignment_dicts()

    def test_alignment(self):
        ta = text_aligner.TextAligner(self.no_case_fragment_dict, self.no_case_str_dict)
        alignment_result = ta.align_text(self.text_to_align)
        import pprint
        pprint.pprint(alignment_result)
