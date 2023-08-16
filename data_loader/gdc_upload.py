# System imports
import os
import logging
import neo4j.graph as Graph
from graphio import NodeSet, RelationshipSet
from uuid import uuid4
# Third party imports
import pandas as pd

log = logging.getLogger(__name__)


def get_metadata(datadir: str, filename: str, graph: Graph):
    if os.path.exists(os.path.join(datadir, filename)):
        load_exp_metadata(datadir=datadir, filename=filename, graph=graph)
    else:
        raise FileNotFoundError("File {} not found.".format(filename))


def load_exp_metadata(datadir: str, filename: str, graph:Graph):
    """
    Load the metadata.
    :return:
    """
    log.info("Loading GDC meta data with File {}".format(filename))

    # Read and filter metadata
    exp_metainfo = pd.read_csv(os.path.join(datadir, filename), header=0, index_col=None, sep='\t')
    exp_metainfo = exp_metainfo[exp_metainfo['file_name'].str.contains('augmented_star_gene_counts.tsv') |
                                exp_metainfo['file_name'].str.contains('mirnaseq.mirnas.quantification.txt')]
    # Start loading metadata
    for ind, row in exp_metainfo.iterrows():
        log.debug("File name {}".format(row['file_name']))
        project = NodeSet(['Project'], merge_keys=['id'])
        disease = NodeSet(['Disease'], merge_keys=['name'])
        sample = NodeSet(['Sample'], merge_keys=['id']) # sample_type
        measurement = NodeSet(['Measurement'], merge_keys=['id']) # type

        project_datafrom_disease = RelationshipSet('FROM', ['Project'], ['Disease'], ['id'], ['name'])
        project_has_samples = RelationshipSet('HAS', ['Project'], ['Sample'], ['id'], ['id'])
        samples_resulted_measurements = RelationshipSet('MEASURED_TO', ['Sample'], ['Measurement'], ['id'], ['id'])

        nodeset = [project, disease, sample, measurement]
        relationset = [project_datafrom_disease, project_has_samples, samples_resulted_measurements]

        # Node ids
        project_id = row['cases.0.project.project_id']
        sample_id = row['cases.0.case_id']
        disease_name = row['cases.0.disease_type']
        measurement_id = row['id']

        # Add nodes
        project.add_node({'id': project_id})
        disease.add_node({'name': disease_name})
        sample.add_node({'id': sample_id, 'sample_type': row['cases.0.samples.0.sample_type']})
        measurement.add_node({'id': measurement_id, 'type': row['experimental_strategy']})

        # Add relationships
        project_datafrom_disease.add_relationship({'id': project_id}, {'name': disease_name}, {})
        project_has_samples.add_relationship({'id': project_id}, {'id': sample_id}, {})
        samples_resulted_measurements.add_relationship({'id': sample_id}, {'id': measurement_id}, {})

        for nd in nodeset:
            nd.create_index(graph=graph)
            nd.merge(graph=graph)
        for rs in relationset:
            rs.create_index(graph=graph)
            rs.merge(graph=graph)

        if row['file_name'].endswith('augmented_star_gene_counts.tsv'):
            mRNA_file = os.path.join(datadir, row['file_name'])
            load_mRNA_data(mRNA_file, measurement_id, graph)
        elif row['file_name'].endswith('mirnaseq.mirnas.quantification.txt'):
            miRNA_file = os.path.join(datadir, row['file_name'])
            load_miRNA_data(miRNA_file, measurement_id, graph)


def load_mRNA_data(file: str, measurement_id: str, graph: Graph):
    """
    Load mRNA expression data.
    Parameters
    ----------
    file
    measurement_id
    graph

    Returns
    -------

    """
    log.info("Loading mRNA expression data with {}".format(file))
    expr_df = pd.read_csv(file, header=None, index_col=None, sep='\t', skiprows=6)
    for ind, row in expr_df[:10].iterrows():
        # Create nodes and relationship
        expression = NodeSet(['Expression'], merge_keys=['uid'])
        gene = NodeSet(['Gene'], merge_keys=['id'])
        measurement_resultedto_expression = RelationshipSet('RESULTED_TO', ['Measurement'], ['Expression'], ['id'], ['uid'])
        expression_belongsto_gene = RelationshipSet('BELONGS_TO', ['Expression'], ['Gene'], ['uid'], ['id'])

        nodeset = [expression, gene]
        relationset = [measurement_resultedto_expression, expression_belongsto_gene]

        UID = str(uuid4())
        gene_id = row[0]
        # Add nodes
        expression.add_node({'uid': UID, 'raw': row[3], 'norm': row[6], 'norm_type': 'fpkm'})
        gene.add_node({'id': gene_id, 'name': row[1], 'type': row[2]})
        # Add relationship
        measurement_resultedto_expression.add_relationship({'id': measurement_id}, {'uid': UID}, {})
        expression_belongsto_gene.add_relationship({'uid': UID}, {'id': gene_id}, {})

        for nd in nodeset:
            nd.create_index(graph=graph)
            nd.merge(graph=graph)
        for rs in relationset:
            rs.create_index(graph=graph)
            rs.merge(graph=graph)


def load_miRNA_data(file: str, measurement_id: str, graph: Graph):
    """
    Load miRNA expression data
    Parameters
    ----------
    file
    measurement_id
    graph

    Returns
    -------

    """
    log.info("Loading miRNA expression data with {}".format(file))
    expr_df = pd.read_csv(file, header=0, index_col=None, sep='\t')
    for ind, row in expr_df[:10].iterrows():
        # Create nodes and relationship
        expression = NodeSet(['Expression'], merge_keys=['uid'])
        gene = NodeSet(['Gene'], merge_keys=['id'])
        measurement_resultedto_expression = RelationshipSet('RESULTED_TO', ['Measurement'], ['Expression'], ['id'], ['uid'])
        expression_belongsto_gene = RelationshipSet('BELONGS_TO', ['Expression'], ['Gene'], ['uid'], ['id'])

        nodeset = [expression, gene]
        relationset = [measurement_resultedto_expression, expression_belongsto_gene]

        UID = str(uuid4())
        gene_id = row['miRNA_ID']
        # Add nodes
        expression.add_node({'uid': UID, 'raw': row['read_count'], 'norm': row['reads_per_million_miRNA_mapped'], 'norm_type': 'rpm'})
        gene.add_node({'id': gene_id, 'name': '-', 'type': 'miRNA'})
        # Add relationship
        measurement_resultedto_expression.add_relationship({'id': measurement_id}, {'uid': UID}, {})
        expression_belongsto_gene.add_relationship({'uid': UID}, {'id': gene_id}, {})

        for nd in nodeset:
            nd.create_index(graph=graph)
            nd.merge(graph=graph)
        for rs in relationset:
            rs.create_index(graph=graph)
            rs.merge(graph=graph)
