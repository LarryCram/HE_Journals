import networkx as nx
from collections import defaultdict

from utils.dbUtils import dbUtil
from utils.time_run import time_run
from utils.dataFileManager import DataFileManager

import pandas as pd
import matplotlib.pyplot as plt
import scienceplots


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

    def load_journal_from_db(self):
        self.load_article_from_journal()
        self.load_authorships_from_journal()
        self.load_references_from_journal()
        self.load_altmetrics_from_journal()
        self.full = self.article.merge(self.authorship, on='works_id').\
            merge(self.reference, on='works_id').merge(self.altmetric, on='doi')

    def load_article_from_journal(self):
        article = self.db_journal.read_db(table_name='articles') \
            .loc[:, ['works_id', 'doi', 'publication_year', 'cited_by_count',
                     'display_name', 'biblio_first_page', 'biblio_last_page', 'abstract']]
        article['doi'] = [doi.rstrip().replace(r"https://doi.org/", "") if isinstance(doi, (str,)) else pd.NA
                          for doi in article.doi]
        print(f'for {self.journal = }: with articles {article.shape = }\n{article.head()}')
        self.article = article


    def load_authorships_from_journal(self):
        authorship = self.db_journal.read_db(table_name='authorships') \
                .loc[:, ['works_id', 'author_id', 'author_display_name', 'institutions_id', 'country_code']]
        self.db_journal.to_db(df=authorship, table_name='authorships_b4_replace_synonyms')
        self.article_list = self.article.works_id.to_list()
        authorship = authorship[[works_id in self.article_list for works_id in authorship.works_id]]
        print(f'for {self.journal = }: number of authorships {authorship.shape[0] = }\n{authorship.head()}')
        synonyms = self.db_journal.read_db(table_name='synonyms')
        cols = [f'author_id_{i}' for i in range(1, 4)]
        print(cols)
        syn_dict = {} | {i: row.author_id_0 for row in synonyms.itertuples() for i in row[3:]}
        print(f'{authorship.author_id.nunique() = }')
        authorship['author_id'] = authorship['author_id'].map(syn_dict)
        print(f'for {self.journal = }: with articles {authorship.shape = }\n{authorship.head()}')
        print(f'{authorship.author_id.nunique() = }')
        self.db_journal.to_db(df=authorship, table_name='authorships')
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

    def time_series_runner(self, journal=None):
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
        self.full['cite_rate'] = [cites / (2024 - year)
                                  for year, cites in zip(self.full.year, self.full.cited_by_count)]
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

        plt.style.use('science')
        fig, axes = plt.subplots(9, 1, sharex='all', figsize=(6, 8))
        axes[0].set_title(f'Number of articles per year ({self.journal})', fontsize='medium', pad=2.0)
        axes[1].set_title('Mean number of authors', fontsize='medium', pad=2.0)
        axes[2].set_title('Mean number of references', fontsize='medium', pad=2.0)
        axes[3].set_title('Mean citation rate', fontsize='medium', pad=2.0)
        axes[4].set_title('Mean Altmetric score', fontsize='medium', pad=2.0)
        axes[5].set_title('Proportion (\%) of uncited articles', fontsize='medium', pad=2.0)
        axes[6].set_title('Proportion (\%) of multi-author articles', fontsize='medium', pad=2.0)
        axes[7].set_title('Proportion (\%) of multi-institution articles', fontsize='medium', pad=2.0)
        axes[8].set_title('Proportion (\%) of multi-country articles', fontsize='medium', pad=2.0)

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
        plt.tight_layout()
        plt.show()
        plt.close(fig)
        return

    def journal_synonyms(self):
        # get authors who appear more than once
        a = self.authorship
        a['counts'] = a.groupby('author_display_name').author_id.transform('nunique')
        a = a[a.counts > 1].drop_duplicates(subset=['author_id', 'author_display_name'])
        d = {author_name: author_id for author_name, author_id in zip(a.author_display_name, a.author_id)}
        a['author_id'] = a.author_display_name.map(d)
        a = a.sort_values('author_display_name')
        print(f'SYNONYMS: {a.shape = } {a.author_display_name.nunique() = }\n{a.head(64)}')
        self.authorship = a

    def journal_homonyms(self):
        # get authors who appear more than once with different institutions
        a = self.authorship.dropna(subset=['institutions_id']).copy()
        print(a.head())
        a['counts'] = a.drop_duplicates(subset=['author_id', 'institutions_id'])\
            .groupby('author_display_name').institutions_id.transform('nunique')
        print(a.head())
        a = a[a.counts > 1].sort_values('author_display_name')
        print(f'HOMONYMS: {a.shape = } {a.author_display_name.nunique() = }\n{a.head(128)}')
        # a['number_of_articles'] = a.groupby('author_display_name').author_id.transform('count')
        # a = a[a.number_of_articles > 1].sort_values('author_display_name')
        # print(f'{a.shape = } {a.author_display_name.nunique() = }\n{a.head(32)}')

        # of these get ones that have different ror



    def journal_summary_runner(self):
        self.load_journal_from_db()
        self.journal_synonyms()
        self.journal_homonyms()
        self.time_series_runner()


@time_run
# @profile_run
def main():

    js = JournalSummary(journal='HERD')
    js.journal_summary_runner()


if __name__ == '__main__':
    main()
