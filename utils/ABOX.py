import os
import csv
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD

class ABOX:
    def __init__(self, tbox_path, output_path):
        tbox = self.loadTBOX(tbox_path)
        abox = self.createABOX(tbox)
        self.load_abox_data(abox)
        self.save_to_turtle(abox, output_path)
    
    def loadTBOX(self, tbox_path):
        tbox = Graph()
        tbox.parse(tbox_path, format="turtle")
        return tbox
    
    def createABOX(self, tbox):
        g = Graph()
        # Define namespaces
        self.SDM = Namespace("http://www.gra.fo/schema/sdm/")
        g.bind("sdm", self.SDM)
        self.OWL = Namespace("http://www.w3.org/2002/07/owl#")
        self.RDF_NS = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        self.XSD = Namespace("http://www.w3.org/2001/XMLSchema#")
        self.RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

        # Define properties based on the TBox
        self.properties = {
            'about': self.SDM.about,
            'cites': self.SDM.cites,
            'corr_author': self.SDM.corr_author,
            'holds': self.SDM.holds,
            'holds_conf': self.SDM.holds_conf,
            'holds_jour': self.SDM.holds_jour,
            'publ_in': self.SDM.publ_in,
            'publ_in_edi': self.SDM.publ_in_edi,
            'publ_in_vol': self.SDM.publ_in_vol,
            'relates_to': self.SDM.relates_to,
            'writes': self.SDM.writes,
            'writes_r': self.SDM.writes_r,
            'abstract': self.SDM.abstract,
            'author_name': self.SDM.author_name,
            'conf_name': self.SDM.conf_name,
            'content': self.SDM.content,
            'decision': self.SDM.decision,
            'doi': self.SDM.doi,
            'edi_name': self.SDM.edi_name,
            'edi_num': self.SDM.edi_num,
            'edi_year': self.SDM.edi_year,
            'jour_name': self.SDM.jour_name,
            'keyword': self.SDM.keyword,
            'title': self.SDM.title,
            'vol_name': self.SDM.vol_name,
            'vol_year': self.SDM.vol_year,
            'reviewer_name': self.SDM.reviewer_name,
            'community_name': self.SDM.community_name,
            'jour_pertains_to': self.SDM.jour_pertains_to,
            'conf_pertains_to': self.SDM.conf_pertains_to
        }
        return g

    def add_data_from_csv(self, g, file_path, triples, literal_fields=[]):
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)
            for row in reader:
                subject = URIRef(self.SDM[row[headers.index(triples[0])]])
                predicate = self.properties[triples[1]]
                obj_value = row[headers.index(triples[2])]
                if triples[2] in literal_fields:
                    obj = Literal(obj_value, datatype=self.XSD.string)
                else:
                    obj = URIRef(self.SDM[obj_value])
                
                g.add((subject, predicate, obj))

                # Handle the special case for main_author
                data_dir = os.path.join(os.path.dirname(__file__), '..', 'clean_datasets/newdata')
                if file_path == os.path.join(data_dir, 'Edge_papers_author.csv') and triples[1] == 'writes' and row[headers.index('main_author')] == 'TRUE':
                    corr_author_predicate = self.properties['corr_author']
                    g.add((subject, corr_author_predicate, obj))
    
    def load_abox_data(self, g):
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'clean_datasets/newdata')
        # Node Paper
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_paper.csv'), ('id_paper', 'title', 'paper_title'), literal_fields=['paper_title'])
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_paper.csv'), ('id_paper', 'doi', 'doi'), literal_fields=['doi'])
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_paper.csv'), ('id_paper', 'abstract', 'abstract'), literal_fields=['abstract'])
        # Node Volume
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_volumes.csv'), ('volume_id', 'vol_name', 'volume'), literal_fields=['volume'])
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_volumes.csv'), ('volume_id', 'vol_year', 'year'), literal_fields=['year'])
        # Node edition
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_edition.csv'), ('ref_edition', 'edi_name', 'edition'), literal_fields=['edition'])
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_edition.csv'), ('ref_edition', 'edi_num', 'edition_num'), literal_fields=['edition_num'])
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_edition.csv'), ('ref_edition', 'edi_year', 'year'), literal_fields=['year'])
        # Node Journal
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_journals.csv'), ('journal_id', 'jour_name', 'x'), literal_fields=['x'])
        # node_keywords.csv
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_keywords.csv'), ('keywords_id', 'keyword', 'Node_keywords'), literal_fields=['Node_keywords'])
        # node_conferences.csv
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_conference.csv'), ('conference_id', 'conf_name', 'conference'), literal_fields=['conference'])
        # node_authors.csv
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_author.csv'), ('author_id', 'author_name', 'author'), literal_fields=['author'])
        # Node Reviewer
        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_paper_author_reviews.csv'), ('author_id', 'reviewer_name', 'author'), literal_fields=['author'])
        # Node Review
        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_paper_author_reviews.csv'), ('rev_id', 'content', 'content'), literal_fields=['content'])
        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_paper_author_reviews.csv'), ('rev_id', 'decision', 'approves'), literal_fields=['approves'])
        # Node Community
        self.add_data_from_csv(g, os.path.join(data_dir, 'Node_community.csv'), ('community', 'community_name', 'community'), literal_fields=['community'])

        # Edges
        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_papers_author.csv'), ('author', 'writes', 'id_paper'), literal_fields=[])

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_paper_author_reviews.csv'), ('author_id', 'writes_r', 'rev_id'))
        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_paper_author_reviews.csv'), ('rev_id', 'about', 'id_paper'))

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_paper_paper.csv'), ('id_paper', 'cites', 'cites_value'), literal_fields=[])

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_paper_keywords.csv'), ('id_paper', 'relates_to', 'keywords'), literal_fields=[])

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_paper_volumes.csv'), ('id_volume', 'publ_in_vol', 'id_paper'), literal_fields=[])

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_papers_edition.csv'), ('ref_edition', 'publ_in_edi', 'id_paper'), literal_fields=[])

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_volumes_journal.csv'), ('journal', 'holds_jour', 'id_volume'), literal_fields=[])

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_edition_conference.csv'), ('conference', 'holds_conf', 'ref_edition'), literal_fields=[])

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_conference_community.csv'), ('conference', 'conf_pertains_to', 'community'), literal_fields=[])

        self.add_data_from_csv(g, os.path.join(data_dir, 'Edge_journal_community.csv'), ('journal', 'jour_pertains_to', 'community'), literal_fields=[])

    def save_to_turtle(self, g, output_path):
        # Serialize the ABox graph to Turtle format and save it to the specified path
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(g.serialize(format='turtle'))

if __name__ == "__main__":
    # Paths for TBOX and output ABOX
    abox_path = os.path.join(os.path.dirname(__file__), '..', 'ABOX.ttl')
    tbox_path = 'TBOX_v2.ttl'    
    abox_instance = ABOX(tbox_path, abox_path)

