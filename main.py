__author__ = 'Peeyush Sahu'
# System import
import logging
import os
import json
import sys
# Local import
from data_loader import gdc_upload
from data_download import gdc_api
# Third party imports
from neo4j import GraphDatabase

# Setup logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.DEBUG)
logging.getLogger('neo4j').setLevel(logging.INFO)
logging.getLogger('graphio').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# Get neo4j configuration
NEO4J_HOST = os.getenv('NEO4J_HOST', 'localhost')
NEO4J_PORT = os.getenv('NEO4J_PORT', 7687)
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'gdcdatabase')
OUTPUT_DIRECTORY = os.getenv('OUTPUT_DIRECTORY', 'C://Users//peeyushsahu//Downloads//ATGC')
driver = GraphDatabase.driver('neo4j://{}:{}'.format(NEO4J_HOST, NEO4J_PORT), auth=('neo4j', NEO4J_PASSWORD))

# Start the GDC data download
meta_file_name = gdc_api.bulk_download(primary_site='Blood',
                                       experiment_strategy=['RNA-Seq', 'miRNA-Seq'],
                                       file_format="TSV",
                                       outdir=OUTPUT_DIRECTORY,
                                       output_size=40)

# Start upload in neo4j database
gdc_upload.get_metadata(datadir=OUTPUT_DIRECTORY, filename=meta_file_name, graph=driver)