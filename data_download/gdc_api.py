# System imports
import requests
import logging
import json
import re
import os
from typing import List, Union
from datetime import datetime
# Third party imports
import pandas as pd

log = logging.getLogger(__name__)


def gdc_meta_filter(primary_site: str='Blood', experiment_strategy: List[str]=['RNA-Seq', 'miRNA-Seq'],
                    file_format: str="TSV", output_dir: str=None, output_size: int=20):
    """
    API call for retrieving file ids with metadata (fields) using filters
    :param primary_site:
    :param experiment_strategy:
    :param file_format:
    :param output_dir:
    :param output_size:
    :return:
    """
    log.info("Downloading {} entries with filters {}, {}, {}".format(
        output_size, primary_site, experiment_strategy, file_format))
    fields = [
        "file_name",
        "experimental_strategy",
        "cases.case_id",
        "cases.submitter_id",
        "cases.samples.sample_type",
        "cases.disease_type",
        "cases.project.project_id"
    ] # An extensive list @https://docs.gdc.cancer.gov/API/Users_Guide/Appendix_A_Available_Fields/
    fields = ",".join(fields)
    files_endpt = "https://api.gdc.cancer.gov/files"
    # This set of filters is nested under an 'and' operator.
    filters = {
        "op": "and",
        "content": [
            {
                "op": "in",
                "content": {
                    "field": "cases.project.primary_site",
                    "value": [primary_site]
                }
            },
            {
                "op": "in",
                "content": {
                    "field": "files.experimental_strategy",
                    "value": experiment_strategy
                }
            },
            {
                "op": "in",
                "content": {
                    "field": "files.data_format",
                    "value": [file_format]
                }
            }
        ]
    }
    # A POST is used, so the filter parameters can be passed directly as a Dict object.
    params = {
        "filters": filters,
        "fields": fields,
        "format": "TSV",
        "size": output_size
    }
    # The parameters are passed to 'json' rather than 'params' in this case
    response = requests.post(files_endpt, headers={"Content-Type": "application/json"}, json=params)
    file_id = '_'.join([primary_site, '_'.join(experiment_strategy), file_format, str(output_size), datetime.now().strftime("%y%m%d%H%M%S")])
    if output_dir:
        file_name = os.path.join(output_dir, file_id+'.tsv')
        log.debug("Saving metadata in {}".format(file_name))
        with open(file_name, "w") as fo:
            fo.write(response.content.decode("utf-8").replace('\r\n', '\n'))
            fo.close()
        return file_id+'.tsv'
    else:
        raise ValueError("Please provide output dir to save the meta data file.")


def gdc_cases(case_id: str, output_dir: str=None):
    """
    Retrieve metadata on a specific case using UUID.
    :param case_id: e.g. cb92f61d-041c-4424-a3e9-891b7545f351
    :param output_dir:
    :return:
    """
    log.info("Downloading cases with case-id {}".format(case_id))
    file_endpt = 'https://api.gdc.cancer.gov/cases/'
    case_uuid = case_id
    response = requests.get(file_endpt + case_uuid)
    # OUTPUT METHOD 1: Write to a file.
    if output_dir:
        file_name = os.path.join(output_dir, case_uuid+'.json')
    else:
        file_name = case_uuid+'.json'
    file = open(file_name, "w")
    file.write(json.dumps(response.json(), indent=2))
    file.close()


def gdc_file(file_id: str, output_dir: str=None):
    """
    Download specific metadata for a file with file-uuid.
    :param file_id: e.g. cb92f61d-041c-4424-a3e9-891b7545f351
    :param output_dir:
    :return:
    """
    log.info("Downloading file with file-id {}".format(file_id))
    file_endpt = 'https://api.gdc.cancer.gov/files/'
    file_uuid = file_id
    response = requests.get(file_endpt + file_uuid)
    # OUTPUT METHOD 1: Write to a file.
    if output_dir:
        file_name = os.path.join(output_dir, file_id+'.json')
    else:
        file_name = file_id+'.json'
    file = open(file_name, "w")
    file.write(json.dumps(response.json(), indent=2))
    file.close()


def gdc_data(file_id: str, output_dir: str=None):
    """
    Download a file with file-id.
    :param file_id: e.g. 827123d3-1191-41d2-a72e-574ef5707a12
    :param output_dir:
    :return:
    """
    log.info("Downloading data file with file-id {}".format(file_id))
    file_id = file_id
    data_endpt = "https://api.gdc.cancer.gov/data/{}".format(file_id)
    response = requests.get(data_endpt, headers = {"Content-Type": "application/json"})

    # The file name can be found in the header within the Content-Disposition key.
    response_head_cd = response.headers["Content-Disposition"]
    file_name = re.findall("filename=(.+)", response_head_cd)[0]
    if output_dir:
        file_name = os.path.join(output_dir, file_name)
    else:
        file_name = file_name
    with open(file_name, "wb") as output_file:
        output_file.write(response.content)
    return file_name


def bulk_download(outdir: str, output_size: int, primary_site: str=None,
                  experiment_strategy: List[str]=[None], file_format: str=None):
    """
    Do a bulk download for data files using specific filters.
    :param outdir:
    :param output_size:
    :param primary_site:
    :param experiment_strategy:
    :param file_format:
    :return:
    """
    log.info("Initiating bulk download...")
    meta_filename = gdc_meta_filter(primary_site=primary_site, experiment_strategy=experiment_strategy,
                                    file_format=file_format, output_dir=outdir, output_size=output_size)

    meta_file = pd.read_csv(os.path.join(outdir, meta_filename), header=0, index_col=None, sep='\t')
    for ind, row in meta_file.iterrows():
        file_name = row['file_name']
        uuid = row['id']
        if file_name.endswith('augmented_star_gene_counts.tsv') | \
                file_name.endswith('mirnaseq.mirnas.quantification.txt'):
            gdc_data(file_id=uuid, output_dir=outdir)
    return meta_filename


#gdc_meta_filter(primary_site='Blood', experiment_strategy=['RNA-Seq', 'miRNA-Seq'],
#                    file_format="TSV", output_dir="C://Users//peeyushsahu//Downloads//ATGC", output_size=200)