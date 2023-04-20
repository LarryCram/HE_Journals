import os

import pandas as pd
import numpy as np
import igraph as ig
import leidenalg as la

from utils.dbUtils import dbUtil
from utils.time_run import time_run
from utils.profile_run import profile_run

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class CitationSummary:

    def __init__(self, journal=None):
        self.citation_table = None
        self.articles = None
        self.cited = None
        self.citers = None
        self.journal = journal
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir = } {os.getcwd() = }')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def load_citers(self):
        self.articles = self.db.read_db(table_name='articles')
        citers = self.db.read_db(table_name='citers_referenced_works')
        citers = citers[citers.referenced_works.isin(self.articles.works_id)]
        self.citers = citers\
            .dropna()\
            .reset_index(drop=True)\
            .drop(columns='cited_id')\
            .rename(columns={'referenced_works': 'journal_id', 'works_id': 'cite_id'})

    def load_cited(self):
        cited = self.db.read_db(table_name='referenced_works')
        cited = cited[cited.works_id.isin(self.articles.works_id)]
        self.cited = cited\
            .dropna()\
            .reset_index(drop=True)\
            .rename(columns={'works_id': 'journal_id', 'referenced_works': 'cite_id'})

    def construct_citation_table(self):
        self.citation_table = pd.concat([self.citers, self.cited], axis=0)\
            .drop_duplicates()\
            .replace("https://openalex.org/", "", regex=True)
            # .sample(frac=0.1)

    def make_citation_graph(self):
        print(f'{self.citation_table.shape = }')
        g = ig.Graph.TupleList(self.citation_table.itertuples(index=False), directed=False, weights=False)
        df = pd.DataFrame([[g.vs[edge.source]['name'], g.vs[edge.target]['name']]
                          for edge in g.es], columns=['source', 'target'])
        print(f'{df.shape = }\n{df.head()}')
        self.graph_info(g)
        self.components_of_citation_graph(g)

    def components_of_citation_graph(self, g):
        # [print(j, len(g.vs[cc])) for j, cc in enumerate(sorted(g.connected_components(mode='weak'), key=len, reverse=True))]
        self.clusters_of_citation_graph(g.connected_components(mode='strong').giant())

    def clusters_of_citation_graph(self, h):
        print(h.summary())
        for j, edge in enumerate(h.es):
            print(j, edge.source, edge.target, edge)
            if j > 4:
                break
        df = pd.DataFrame([[h.vs[edge.source]['name'], h.vs[edge.target]['name']]
                          for edge in h.es], columns=['source', 'target'])
        print(f'{df.shape = } {df.drop_duplicates().shape = }\n{df.head()}')

        # n_comms = 50
        # partition = la.ModularityVertexPartition(h)
        #                                   # initial_membership=np.random.choice(n_comms, len(h.vs)))
        #                                   # resolution_parameter=0.5)

        partition = la.find_partition(h, la.ModularityVertexPartition)
        p_dict = {k: j for j, p in enumerate(partition) for k in p if isinstance(k, int)}
        df['partition'] = p_dict
        print(f'{df.shape = } {len(p_dict) = }\n{df.head()}')
        [print(p, len(group)) for p, group in df.groupby('partition')]
        exit(44)

        h.write_graphml(rf'{self.data_dir}\leiden_communities.graphml')
        ig.plot(partition,
                vertex_size=2,
                palette=None,
                # vertex_color=['blue', 'red', 'green', 'yellow'],
                # vertex_label=['first', 'second', 'third', 'fourth'],
                # vertex_label=[h.vs(n)['name'] for p in list(partition) for n in p],
                # edge_width=[1, 4],
                # edge_color=['black', 'grey'],
                layout='kamada_kawai',
                target=rf'{self.data_dir}\leiden_communities.pdf',
                )

    def graph_info(self, g):
        print("Number of vertices:", g.vcount())
        print("Number of edges:", g.ecount())
        print("Density of the graph:", 2 * g.ecount() / (g.vcount() * (g.vcount() - 1)))
        n_vertices = g.vcount()
        degrees = []
        total = 0
        for n in range(n_vertices):
            neighbours = g.neighbors(n, mode='ALL')
            total += len(neighbours)
            degrees.append(len(neighbours))
        print("Average degree:", total / n_vertices)
        print("Maximum degree:", max(degrees))
        indx_max = degrees.index(max(degrees))
        print(f"Vertex ID with the maximum degree: {indx_max = } {g.vs[indx_max]['name'] = }")
        cc = g.connected_components(mode='strong')
        print(f"Number of connected components:", len(cc))
        print(f"Size of largest connected component: {cc.giant().vcount() = } {cc.giant().ecount() = }")

    def citation_summary_runner(self):
        self.load_citers()
        self.load_cited()
        self.construct_citation_table()
        self.make_citation_graph()


@time_run
# @profile_run
def main():

    cs = CitationSummary(journal='HERD')
    cs.citation_summary_runner()


if __name__ == '__main__':
    main()
