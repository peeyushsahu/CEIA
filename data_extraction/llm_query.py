# System imports
import os
# Third party imports
from langchain.chat_models import ChatOpenAI
from langchain.chains import GraphCypherQAChain
from langchain.graphs import Neo4jGraph

os.environ['OPENAI_API_KEY'] = open('..//openai_cred.txt').read()

graph = Neo4jGraph(
    url="bolt://localhost:7687",
    username="neo4j",
    password="gdcdatabase")

chain = GraphCypherQAChain.from_llm(
    ChatOpenAI(temperature=0), graph=graph, verbose=True,
)

chain.run("""
Task:
Find the path from sample to gene and identify protein_coding gene with the highest expression.fpkm value. Now get all expression values for identified gene wrt to all sample ids. Return gene id, gene name, expression.fpkm, expression.raw and sample.id?
Instructions:
Use only the provided relationship types and properties.
Do not use any other relationship types or properties that are not provided.
Use only the correct direction of graph.
Connect only nodes which are connected in provided data.
Assign different node variables if used multiple time in a query.
""")

"""
Task:
Find the path from sample to gene and identify gene with the highest expression.fpkm value. Return gene id, gene name, expression.fpkm, expression.raw and sample.id?
Instructions:
Generated Cypher:
MATCH (s:Sample)-[:MEASURED_TO]->(:Measurement)-[:RESULTED_TO]->(e:Expression)-[:BELONGS_TO]->(g:Gene)
RETURN g.id, g.name, e.fpkm, e.raw, s.id
ORDER BY e.fpkm DESC
LIMIT 1
Full Context:
[{'g.id': 'hsa-let-7a-1', 'g.name': '-', 'e.fpkm': None, 'e.raw': 142786, 's.id': 'ea48b158-b34e-5c61-a37e-ed9c5440061e'}]
"""