import os

import pandas as pd
import igraph as ig
import leidenalg as la

from utils.dbUtils import dbUtil
from utils.time_run import time_run
from utils.profile_run import profile_run

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', 800)


class CitationSummary:

    def __init__(self, journal=None):
        self.partition_df = None
        self.citation_table = None
        self.articles = None
        self.cited = None
        self.citers = None
        self.journal = journal
        self.data_dir = r'./data'
        if not os.path.exists(self.data_dir):
            raise SystemExit(f'data directory does not exist {self.data_dir = } {os.getcwd() = }')
        self.db = dbUtil(db_name=f'{self.data_dir}/.db/{journal}')

    def load_articles(self):
        self.articles = self.db.read_db(table_name='articles').drop(columns='index')
        print(f'ARTICLES: {self.articles.works_id.nunique() = } {self.articles.shape = }')
        referenced_works = self.db.read_db(table_name='referenced_works')
        referenced_works = referenced_works[[w in self.articles.works_id.values for w in referenced_works.works_id]]
        print(f'REFERENCED WORKS: {referenced_works.works_id.nunique() = } '
              f'{referenced_works.referenced_works.nunique() = }')
        for j, works_id in enumerate(self.articles.works_id):
            if works_id not in referenced_works.works_id.values:
                print(f'NO REFERENCES: {j = } {works_id = }')

    def load_citers(self):
        citers = self.db.read_db(table_name='citers_works')
        citers = citers[citers.cited_id.isin(self.articles.works_id.values)]
        citers = citers\
            .dropna()\
            .reset_index(drop=True)\
            .rename(columns={'cited_id': 'journal_id', 'works_id': 'cite_id'})
        print(f'CITERS: {citers.shape = } {citers.cite_id.nunique() = } {citers.journal_id.nunique() = }')
        self.citers = citers

    def load_cited(self):
        cited = self.db.read_db(table_name='cited_works')
        cited = cited[cited.citing_work_id.isin(self.articles.works_id.values)]
        cited = cited\
            .reset_index(drop=True)\
            .rename(columns={'citing_work_id': 'journal_id', 'works_id': 'cite_id'})
        print(f'CITED: {cited.shape = } {cited.cite_id.nunique() = } {cited.journal_id.nunique() = }')
        for j, works_id in enumerate(self.articles.works_id.values):
            if works_id not in cited.journal_id.values:
                print(f'NOT IN ARTICLES WITH CITED WORKS: {j = } {works_id = }')
        self.cited = cited

    def construct_citation_table(self):
        print(self.cited.sort_values('journal_id').head())
        print(self.citers.sort_values('journal_id').head())
        self.citation_table = pd.concat([self.citers, self.cited], axis=0)\
            .drop_duplicates()\
            .sort_values('journal_id')\
            .replace("https://openalex.org/", "", regex=True)
        print(self.citation_table.head())
        print(f'{self.cited.shape = } '
              f'{self.citers.shape = } '
              f'{self.citation_table.shape = } '
              f'{self.citation_table.journal_id.nunique() = }')

    def make_citation_graph(self):
        print(f'{self.citation_table.shape = }\n{self.citation_table.sort_values("journal_id").head()}')
        g = ig.Graph.TupleList(self.citation_table[['cite_id', 'journal_id']].itertuples(index=False), directed=True, weights=False)
        df = pd.DataFrame([[g.vs[edge.source]['name'], g.vs[edge.target]['name']]
                          for edge in g.es], columns=['cite_id', 'journal_id'])
        print(f'{df.shape = }\n{df.head()}')
        self.graph_info(g)
        # self.components_of_citation_graph(g)
        exit(11)

    def components_of_citation_graph(self, g):
        self.clusters_of_citation_graph(g.connected_components(mode='strong').giant())

    def clusters_of_citation_graph(self, h):
        print(h.summary())
        for j, edge in enumerate(h.es):
            print(j, edge.source, edge.target, edge)
            if j > 4:
                break
        df = pd.DataFrame([[edge.source, h.vs[edge.source]['name'],
                            edge.target, h.vs[edge.target]['name']]
                          for edge in h.es], columns=['source', 'source_id', 'target', 'target_id'])
        print(f'{df.shape = } {df.drop_duplicates().dropna().shape = }\n{df.head()}')
        s1 = set(df.source.to_list())
        s2 = set(df.target.to_list())
        print(f'distinct sources: {len(s1) = }')
        print(f'distinct targets: {len(s2) = }')
        s1.update(s2)
        print(f'distinct sources or targets: {len(s1) = }')

        # n_comms = 50
        # partition = la.ModularityVertexPartition(h)
        #                                   # initial_membership=np.random.choice(n_comms, len(h.vs)))
        #                                   # resolution_parameter=0.5)

        partition = la.find_partition(h, la.ModularityVertexPartition)
        # partition = la.find_partition(h, la.RBConfigurationVertexPartition)
        diff = la.Optimiser().optimise_partition(partition, n_iterations=50)
        print(f'{diff = }')
        # partition = la.ModularityVertexPartition(h).renumber_communities()
        p_dict = {k: j for j, p in enumerate(partition) for k in p if isinstance(k, int)}
        df['partition'] = [p_dict[v1] if p_dict.get(v1, False) else p_dict.get(v2, False)
                           for v1, v2 in zip(df.source, df.target)]
        print(f'{df.shape = } {len(p_dict) = }\n{df.head()}')
        fd = df.value_counts("partition").to_frame().reset_index().astype({"partition": int})
        fd = fd.reset_index(drop=False).drop(columns='partition').rename(columns={'index': 'partition'})
        print(f'frequency distribution of partitions:\n{fd}')
        print(f'{df.value_counts("partition").to_frame()["count"].sum() = }')
        self.db.to_db(df=df, table_name='communities')
        self.partition_df = df.reset_index()
        self.cluster_labels()

    def graph_info(self, g):
        print("Number of vertices:", g.vcount())
        print("Number of edges:", g.ecount())
        print("Density of the graph:", 2 * g.ecount() / (g.vcount() * (g.vcount() - 1)))
        n_vertices = g.vcount()
        degrees = {'in': [], 'out': []}
        total = 0
        for n in range(n_vertices):
            for mode in ['in', 'out']:
                neighbours = g.neighbors(n, mode=mode)
                total += len(neighbours)
                degrees[mode].append(len(neighbours))
        print("Average degree:", total / n_vertices)
        for mode in ['in', 'out']:
            dm = degrees[mode]
            print(f"Maximum degree {mode}: {max(dm) = }")
            indx_max = dm.index(max(dm))
            print(f"Vertex ID with the maximum degree: {indx_max = } {g.vs[indx_max]['name'] = }")
        cc = g.connected_components(mode='strong')
        print(f"Number of connected components:", len(cc))
        print(f"Size of largest connected component: {cc.giant().vcount() = } {cc.giant().ecount() = }")

    def cluster_labels(self):
        for partition in [0, 1, 2, 3, 4, 5]:
            print(f'{partition = } {len(self.partition_df[partition == self.partition_df.partition]) = }')
            sources = set(self.partition_df.loc[partition == self.partition_df.partition].source_id.values)
            articles = self.citation_table[self.citation_table.journal_id.isin(sources)].sort_values('publication_year').copy()
            print(f'{articles.shape = }')
            print(articles.head())
            self.articles['works_id'] = [w.replace('https://openalex.org/', '') for w in self.articles.works_id]
            cite_dict = dict(zip(self.articles.works_id, self.articles.display_name))
            year_dict = dict(zip(self.articles.works_id, self.articles.publication_year))
            articles['cited_title'] = articles.journal_id.map(cite_dict)
            articles['journal_year'] = articles.journal_id.map(year_dict)
            cols = ['journal_id', 'cited_title', 'journal_year', 'cite_id', 'display_name', 'publication_year', 'cited_by_count']
            articles = articles[cols]
            articles['cited_title'] = [' '.join(t.split(' ')[:8]) for t in articles.cited_title]
            articles['display_name'] = [' '.join(t.split(' ')[:8]) for t in articles.display_name]
            articles = articles.sort_values(['cited_title', 'publication_year'])
            print(f'{articles.shape = }')
            print(articles.head())
            print(articles.tail())
            self.db.to_db(df=articles, table_name=f'partition_{partition}')

    def citation_summary_runner(self):
        self.load_articles()
        self.load_citers()
        self.load_cited()
        self.construct_citation_table()
        # self.make_citation_graph()


@time_run
# @profile_run
def main():

    cs = CitationSummary(journal='HERD')
    cs.citation_summary_runner()


if __name__ == '__main__':
    main()
