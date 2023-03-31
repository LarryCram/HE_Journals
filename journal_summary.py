import networkx as nx
from collections import defaultdict

from utils.dbUtils import dbUtil
from utils.time_run import time_run
from utils.dataFileManager import DataFileManager

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


class JournalSummary:

    def __init__(self, journal=None):
        self.full = None
        self.altmetric = None
        self.reference = None
        self.article_list = None
        self.authorship = None
        self.article = None
        self.journal = journal
        self.dfm = DataFileManager()
        project_data_folder = self.dfm.data_dir
        self.db_folder = rf'{project_data_folder}\.db'
        self.db_core = dbUtil(rf'{self.db_folder}\core')
        self.db_journal = dbUtil(db_name=rf'{self.db_folder}\{journal}')
        print(f'opened core.db db in {self.db_core}')

    def journal_summary_runner(self):
        self.load_journal_from_db()
        self.prepare_time_series()

    def load_journal_from_db(self):
        self.load_article_from_journal()
        self.load_authorships_from_journal()
        self.load_references_from_journal()
        self.load_altmetrics_from_journal()
        self.full = self.article.merge(self.authorship, on='works_id').\
            merge(self.reference, on='works_id').merge(self.altmetric, on='doi')

    def load_article_from_journal(self):
        article = self.db_journal.read_db(table_name='articles').loc[:,
                  ['works_id', 'doi', 'publication_year', 'cited_by_count', 'display_name',
                   'biblio_first_page', 'biblio_last_page', 'abstract']]
        article['doi'] = [doi.rstrip().replace(r"https://doi.org/", "") if isinstance(doi, (str,)) else pd.NA
                          for doi in article.doi]
        print(f'for {self.journal = }: with articles {article.shape = }\n{article.head()}')
        self.article = article

    def load_authorships_from_journal(self):
        authorship = self.db_journal.read_db(table_name='authorships').loc[:,
                     ['works_id', 'author_id', 'institutions_id', 'country_code']]
        self.article_list = self.article.works_id.to_list()
        authorship = authorship[[works_id in self.article_list for works_id in authorship.works_id]]
        print(f'for {self.journal = }: number of authorships {authorship.shape[0] = }\n{authorship.head()}')
        self.authorship = authorship

    def load_references_from_journal(self):
        reference = self.db_journal.read_db(table_name='referenced_works')
        reference = reference[[works_id in self.article_list for works_id in reference.works_id]]
        reference = reference.groupby('works_id').count().reset_index().\
            rename(columns={'referenced_works': 'reference_count'})
        print(f'for {self.journal = } {reference.shape = }\n{reference.head()}')
        self.reference = reference

    def load_altmetrics_from_journal(self):
        altmetric = self.db_journal.read_db(table_name='altmetrics').loc[:, ['doi', 'score']]
        altmetric = altmetric.rename(columns={'score': 'alt_score'})
        altmetric['alt_score'] = [float(score) if isinstance(score, (float, str,)) else pd.NA for score in
                                  altmetric.alt_score]
        print(f'for {self.journal = } {altmetric.shape = }\n{altmetric.head()}')
        self.altmetric = altmetric

    def prepare_time_series(self, journal=None):
        """
        plot time series of key data for journal
        """
        print(f'merged journal data {self.full.shape = }\n{self.full.head()}')
        # aggregate to per-article counts
        agg_dict = {
            'author_count': pd.NamedAgg(column='author_id', aggfunc='nunique'),
            'institution_count': pd.NamedAgg(column='institutions_id', aggfunc='nunique'),
            'country_count': pd.NamedAgg(column='country_code', aggfunc='nunique')
        }
        agg_df = self.full.groupby('works_id').agg(**agg_dict).reset_index()
        self.full = self.full[['publication_year', 'works_id', 'cited_by_count', 'reference_count', 'alt_score']].\
            drop_duplicates().merge(agg_df, on='works_id').rename(columns={'publication_year': 'year'})
        self.full['cite_rate'] = [cites / (2024 - year) for year, cites in zip(self.full.year, self.full.cited_by_count)]
        print(f'full array {self.full.shape = }\n{self.full.head()}')
        annual = self.annual_trend(df=self.full)
        self.plot_annual(journal=journal, df=annual)

    def annual_trend(self, df=None):

        df = df.rename(columns={'publication_year': 'year'})
        df = df.sort_values('year')
        print(df.head(32))
        df.drop_duplicates(subset=['works_id'], inplace=True, ignore_index=True)
        print(df.head())
        agg_dict = {
            "Article_count": pd.NamedAgg(column="works_id", aggfunc='count'),
            "Author_count_mean": pd.NamedAgg(column="author_count", aggfunc='mean'),
            "Reference_count_mean": pd.NamedAgg(column="reference_count", aggfunc='mean'),
            "Citation_rate_mean": pd.NamedAgg(column="cite_rate", aggfunc='mean'),
            "Altmetric_score_mean": pd.NamedAgg(column="alt_score", aggfunc='mean'),
        }

        df0 = df.groupby('year').agg(**agg_dict)

        # lamda functions don't work in a dict of NamedAgg (Pandas bug)
        df1 = df.groupby('year')['cited_by_count'].agg(Prop_uncited=lambda x: 100 * (x == 0).sum() / len(x))
        df2 = df.groupby('year')['author_count'].agg(Prop_multiauthor=lambda x: 100 * (x > 1).sum() / len(x))
        df3 = df.groupby('year')['country_count'].agg(Prop_multicountry=lambda x: 100 * (x > 1).sum() / len(x))
        df4 = df.groupby('year')['institution_count'].agg(Prop_multiinst=lambda x: 100 * (x > 1).sum() / len(x))
        df = pd.concat([df0, df1, df2, df3, df4], axis=1).reset_index()

        print(df.head(128))

        return df.fillna(0)

    def plot_annual(self, journal=None, df=None):

        print(f'plot {journal}\n{df.head()}')

        fig, axes = plt.subplots(9, 1, sharex=True, figsize=(7, 7))

        print(axes)

        # axes[0].set_title('Number of articles per year', fontsize='medium', pad=2.0)
        axes[0].set_title(f'Number of articles per year ({self.journal})', fontsize='medium', pad=2.0)
        axes[1].set_title('Mean number of authors', fontsize='medium', pad=2.0)
        axes[2].set_title('Mean number of references', fontsize='medium', pad=2.0)
        axes[3].set_title('Mean citation rate', fontsize='medium', pad=2.0)
        axes[4].set_title('Mean Altmetric score', fontsize='medium', pad=2.0)
        axes[5].set_title('Proportion (%) of uncited articles', fontsize='medium', pad=2.0)
        axes[6].set_title('Proportion (%) of multi-author articles', fontsize='medium', pad=2.0)
        axes[7].set_title('Proportion (%) of multi-institution articles', fontsize='medium', pad=2.0)
        axes[8].set_title('Proportion (%) of multi-country articles', fontsize='medium', pad=2.0)

        axes[0].bar(df.year, df.Article_count, color='gray', edgecolor='black')
        axes[1].bar(df.year, df.Author_count_mean, color='gray', edgecolor='black')
        axes[2].bar(df.year, df.Reference_count_mean, color='gray', edgecolor='black')
        axes[3].bar(df.year, df.Citation_rate_mean, color='gray', edgecolor='black')
        axes[4].bar(df.year, df.Altmetric_score_mean, color='gray', edgecolor='black')
        axes[5].bar(df.year, df.Prop_uncited, color='gray', edgecolor='black')
        axes[6].bar(df.year, df.Prop_multiauthor, color='gray', edgecolor='black')
        axes[7].bar(df.year, df.Prop_multiinst, color='gray', edgecolor='black')
        axes[8].bar(df.year, df.Prop_multicountry, color='gray', edgecolor='black')

        for jj in range(8):
            axes[jj].xaxis.get_major_locator().set_params(integer=True)

        plt.xlabel('Year')
        for j in range(1):
            axes[j].tick_params(bottom=False)
            axes[j].grid(axis='y')
        plt.subplots_adjust(hspace=0.4)

        save_fig = rf".\plots\annual_{self.journal}.png"
        plt.savefig(save_fig)
        plt.show()
        plt.close(fig)

        return

    def extract_clusters(self, journal=None):

        if not journal:
            print(f'in extract_cluster {journal = }')
            return None

        db_journal = dbUtil(db_name=rf'{self.db_folder}\{journal}')

        # extract and transform articles
        article = db_journal.read_db(table_name='articles'). \
                      loc[:, ['artId', 'doi', 'publication_year', 'cited_by_count', 'abstract']]
        # article = article.loc[article['abstract'].map(len) > 1].drop(columns=['abstract']).copy()
        article_list = article.artId.to_list()

        # extract and transform authorship
        authorship = db_journal.read_db(table_name='authorships'). \
                         loc[:, ['artId', 'authorId']]
        total_items = authorship.shape[0]
        authorship = authorship[[artId in article_list for artId in authorship.artId]]
        authorship = authorship.drop_duplicates()
        print(f'for {journal}: number of authorships {authorship.shape[0]} from {total_items} total items')
        # print(authorship.head())

        # construct bipartite graph of article/author
        B = nx.Graph()
        article_nodes = authorship.artId.unique()
        print(f'number of articles = {len(article_nodes)}')
        B.add_nodes_from(article_nodes, bipartite=0)
        author_nodes = authorship.authorId.unique()
        print(f'number of authors = {len(author_nodes)}')
        B.add_nodes_from(author_nodes, bipartite=1)
        edges = list(zip(authorship.artId, authorship.authorId))
        B.add_edges_from(edges)
        print(f'number of connected components = {nx.number_connected_components(B)}')

        # decompose bipartite graph into connected components
        cluster_dict = defaultdict(lambda: defaultdict(int))
        S = [s.nodes(data=True) for s in [B.subgraph(c).copy() for c in nx.connected_components(B)]]
        for j, s in enumerate(sorted(S, key=lambda x: len(x), reverse=True)):
            n_author = sum(x[1]['bipartite'] for x in s)
            n_article = len(s) - n_author
            cluster_dict[n_author][n_article] += 1
            if j == 0:
                print(f'largest connected component: n_author = {n_author} n_article = {n_article}')

        # create table of (author_count, article_count) -> number of components

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
            # print(row)
            df.at[row[0], 'total'] = sum(column_index[j] * v for j, v in enumerate(row[1:]))
        df = df.T
        for row in df[:-1].itertuples(name=None):
            # print(row)
            df.at[row[0], 'total'] = sum(row_index[j] * int(v) for j, v in enumerate(row[1:]))
        df = df.T
        df.at[:-1, '%'] = [t / sum(df.iloc[:-1].total) for t in df.iloc[:-1].total]
        df.loc['%', :] = [t / df.loc['total', :].sum() for t in df.loc['total', :]]

        print(df)


@time_run
# @profile_run
def main():

    js = JournalSummary(journal='HERD')
    js.journal_summary_runner()


if __name__ == '__main__':
    main()
