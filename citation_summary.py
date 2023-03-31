from utils.time_run import time_run
from utils.profile_run import profile_run

class CitationSummary:

    def __init__(self, journal=None):
        self.journal = journal


    def citation_summary_runner(self):
        self.load_citers()
        self.load_cited()

    def load_citers(self):
        pass

    def load_cited(self):
        pass

@time_run
# @profile_run
def main():

    cs = CitationSummary(journal='HERD')
    cs.citation_summary_runner()


if __name__ == '__main__':
    main()