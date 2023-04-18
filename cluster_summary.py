import os
import networkx as nx
from collections import defaultdict

from utils.dbUtils import dbUtil
from utils.time_run import time_run

import pandas as pd
import matplotlib.pyplot as plt
import scienceplots

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class ClusterSummary:

    def __init__(self, journal=None):
        self.final_year = None
        self.authorships = None
        self.articles = None
        self.journal = journal
        self.cluster_time_series = []
        db_dir = rf'{os.getcwd()}\data\.db'
        self.db_core = dbUtil(db_name=rf'{db_dir}\core')
        self.db_journal = dbUtil(db_name=rf'{db_dir}\{journal}')
        self.db_clusters = dbUtil(db_name=rf'{db_dir}\{journal}_clusters')

    def loader(self):
        self.load_articles()
        self.load_authorships()

    def load_articles(self):
        self.articles = self.db_journal.read_db(table_name='articles') \
                            .loc[:, ['works_id', 'doi', 'publication_year', 'cited_by_count', 'abstract']]

    def load_authorships(self):
        self.authorships = self.db_journal.read_db(table_name='authorships') \
                               .loc[:, ['works_id', 'author_id', 'author_display_name']]
        self.authorships = self.authorships[
            [works_id in self.articles.works_id.to_list() for works_id in self.authorships.works_id]]
        self.authorships['publication_year'] = self.authorships.works_id\
            .map(dict(zip(self.articles.works_id, self.articles.publication_year.astype(int))))
        self.authorships = self.authorships.drop_duplicates().dropna(subset='author_id')

    def extract_clusters(self, final_year=None):
        self.final_year = final_year
        print(self.authorships.head())
        authorships = self.authorships[self.authorships.publication_year <= final_year].copy()
        print(f'for {self.journal = } and {final_year = } -> {authorships.shape = }')
        # construct bipartite graph of article/author
        B = nx.Graph()
        article_nodes = authorships.works_id.unique()
        B.add_nodes_from(article_nodes, bipartite=0)
        author_nodes = authorships.author_id.unique()
        B.add_nodes_from(author_nodes, bipartite=1)
        edges = list(zip(authorships.works_id, authorships.author_id))
        B.add_edges_from(edges)
        print(f'{len(article_nodes) = } {len(author_nodes) = } {nx.number_connected_components(B) = }')
        new_authorship = self.summarise_clusters(graph=B, authorship=authorships)
        self.db_clusters.to_db(df=new_authorship, table_name=f'clusters_{final_year}')

    def summarise_clusters(self, graph=None, authorship=None):
        # decompose bipartite graph into connected components
        cluster_dict = defaultdict(lambda: defaultdict(int))
        S = [s.nodes(data=True) for s in [graph.subgraph(c).copy() for c in nx.connected_components(graph)]]
        for j, s in enumerate(sorted(S, key=lambda x: len(x), reverse=True)):
            n_author = sum(x[1]['bipartite'] for x in s)
            n_article = len(s) - n_author
            cluster_dict[n_author][n_article] += 1
            cluster = f'C_{j:04}'
            for jj, n in enumerate(s):
                author_id = n[0]
                if r'org/A' in author_id:
                    authorship.loc[authorship.author_id == author_id, 'cluster'] = cluster
                    authorship.loc[authorship.author_id == author_id, 'cluster_size'] = len(s)
                    authorship.loc[authorship.author_id == author_id, 'n_author'] = n_author
                    authorship.loc[authorship.author_id == author_id, 'n_article'] = n_article
            if j == 0:
                self.cluster_time_series.append([self.final_year, cluster, len(s), n_author, n_article])
        self.cluster_table_maker(cluster_dict=cluster_dict)
        return authorship

    def cluster_table_maker(self, cluster_dict=None):
        # first get actual values of articles (index values) and authors (column values)
        row_index = set()
        column_index = set()
        for k2, d2 in sorted(cluster_dict.items()):
            row_index.add(k2)
            for k1, v1 in sorted(d2.items()):
                column_index.add(k1)
        row_index = sorted(list(row_index))
        column_index = sorted(list(column_index))
        # fill out dataframe
        df = pd.DataFrame(index=row_index, columns=column_index)
        for row in df.index:
            for column in df.columns:
                df.at[row, column] = cluster_dict[row][column]
        # compute row and column totals and proportions
        for row in df.itertuples(name=None):
            df.at[row[0], 'total'] = int(sum(column_index[j] * v for j, v in enumerate(row[1:])))
        df = df.T
        for row in df[:-1].itertuples(name=None):
            df.at[row[0], 'total'] = int(sum(row_index[j] * int(v) for j, v in enumerate(row[1:])))
        df = df.T
        df.at[::-1, '%'] = [t*100 / sum(df.iloc[:-1].total) for t in df.iloc[::-1].total]
        df.loc['%', :] = [t*100 / df.loc['total', :].sum() for t in df.loc['total', :]]
        df = df.fillna(0).round(2)
        print(df)

    def plot_cluster_time_series(self, cluster_time_series=None):
        plt.style.use('science')
        plt.subplots(figsize=(9, 9))
        cluster_time_series.rename(columns={'final_year': 'Final year',
                                            'n_authors': 'Number of authors',
                                            'n_articles': 'Number of articles'})
        cluster_time_series.plot('final_year', ['n_articles', 'n_authors'], kind='line')
        plt.xlabel('Final year')
        plt.tight_layout()
        plt.show()

    def cluster_summary_runner(self):
        self.loader()
        for final_year in range(2023, 1984, -1):
            self.extract_clusters(final_year=final_year)

        cols = ['final_year', 'cluster', 'total nodes', 'n_authors', 'n_articles']
        cluster_time_series = pd.DataFrame(self.cluster_time_series, columns=cols)
        print(cluster_time_series)
        self.plot_cluster_time_series(cluster_time_series=cluster_time_series)


@time_run
# @profile_run
def main():

    cs = ClusterSummary(journal='HERD')
    cs.cluster_summary_runner()


if __name__ == '__main__':
    main()