__author__ = 'rodrigo'
import pywikibot
import networkx as nx
from collections import defaultdict
import queue
import re
import asyncio
from datetime import datetime
import threading
from string import punctuation

from statscounter import StatsCounter
from mwparserfromhell import parse as mwparse
from nltk.tokenize import wordpunct_tokenize
from nltk.corpus import stopwords

from wikicrawl.tools.divtools import jsd2
from wikicrawl.tools.containers import SortedKeyDict

__all__ = ["MultiThreadedWikiCrawler"]

STOPWORDS = set(stopwords.words('english'))
PUNCTUATION = set(punctuation)

WIKILINK_RE = re.compile(r"\[\[(.*?)\]\]")

WIKINAMESPACE_RE = re.compile(r"^File:|^Image:|^Category:|^simple:|^..:|^#.*")

RLOCK = threading.RLock()

class MultiThreadedWikiCrawler:
    """
    TODO: finish implementing proper caching update.
    """
    def __init__(self, *args, **kwargs):
        """"""
        self._wiki_site = pywikibot.Site(u"en", fam=u"wikipedia")

        self.graphs = []#nx.DiGraph()
        self.page_title = kwargs.get('page_title', None)
        self.timestamp = kwargs.get('timestamp', None)
        self.node_count = kwargs.get('node_count', None)
        # aliases (titles) to canonical titles table
        self.title_to_ctitle_table = {}
        # canonical title to canonical wiki page (not redirection page) table
        self.ctitle_to_cpage_table = {}
        # canonical title to revisions
        #self.ctitle_to_revisions = {}
        self.ctitle_to_revids_to_revisions = defaultdict(dict)
        # canonical title to revision ids to article text
        self.ctitle_to_revids_to_texts = defaultdict(dict)
        # canonical title to revision ids to article text word distribution
        self.ctitle_to_revids_to_word_dist = defaultdict(dict)
        # ctitle to ctitle divergence scores
        self.ctitle_and_ctitle_to_div_scores = SortedKeyDict()
        # canonical title to revision ids to forward links
        self.ctitle_to_revids_to_forwardlinks = defaultdict(dict)
        # canonical title to timestamp to revid
        self.ctitle_to_timestamps_to_revid = defaultdict(dict)

        #self.q = queue.LifoQueue()
        self.q = queue.Queue()


    def _get_wikipage(self, page_title):
        """"""
        if page_title in self.title_to_ctitle_table:
            page_title = self.title_to_ctitle_table[page_title]

        if page_title in self.ctitle_to_cpage_table:
            return self.ctitle_to_cpage_table[page_title]

        page = pywikibot.Page(self._wiki_site, page_title)

        # if not canonical page
        if page.isRedirectPage():
            page = page.getRedirectTarget()

        # canonical title
        ctitle = page.title()

        # set canonical title to canonical wiki page
        self.ctitle_to_cpage_table[ctitle] = page

        # set title (alias) to canonical title
        self.title_to_ctitle_table[page_title] = ctitle

        return page

    def _get_canonical_page_title(self, page_title):
        """"""
        # check alias to canonical title table
        if page_title in self.title_to_ctitle_table:
            return self.title_to_ctitle_table[page_title]

        return self._get_wikipage(page_title).title()


    def _get_revisions(self, page_title, debug=False):
        RLOCK.acquire()
        revisions = []
        try:
            page_title = self._get_canonical_page_title(page_title)

            # check canonical title to revisions table
            #if page_title in self.ctitle_to_revisions:
            #    return self.ctitle_to_revisions[page_title]
            if page_title in self.ctitle_to_revids_to_revisions:
                return self.ctitle_to_revids_to_revisions[page_title].values()

            page = self._get_wikipage(page_title)
            # sorted property/invariant
            revisions = sorted(list(page.revisions()), key=lambda r: r['timestamp'])
        except Exception as e:
            if debug:
                print(e)
            return []
        else:
            if debug:
                print("{0} - {1} revisions".format(page_title, len(revisions)))

            #self.ctitle_to_revisions[page_title] = revisions
            for rev in revisions:
                self.ctitle_to_revids_to_revisions[page_title][rev['revid']] = rev

            #return revisions
        finally:
            RLOCK.release()
            return revisions
    #
    # def _get_revisions(self, page_title, debug=False):
    #     """"""
    #     RLOCK.acquire()
    #     revisions = []
    #     try:
    #         if debug:
    #             print("Getting revisions for {0}".format(page_title))
    #
    #         page_title = self._get_canonical_page_title(page_title)
    #
    #         # check canonical title to revisions table
    #         if page_title in self.ctitle_to_revisions:
    #             return self.ctitle_to_revisions[page_title]
    #
    #         page = self._get_wikipage(page_title)
    #
    #         #try:
    #             # page.revisions() returns a generator
    #             # sorted property/invariant
    #         revisions = sorted(list(page.revisions()), key=lambda r: r['timestamp'])
    #     except Exception as e:
    #         if debug:
    #             print(e)
    #     else:
    #         self.ctitle_to_revisions[page_title] = revisions
    #         #return revisions
    #     finally:
    #         RLOCK.release()
    #         return revisions

    def _get_revisionid_before_date(self, page_title, timestamp, debug=False):
        """"""
        page_title = self._get_canonical_page_title(page_title)

        timestamps_to_revid = self.ctitle_to_timestamps_to_revid[page_title]

        if timestamp in timestamps_to_revid:
            return timestamps_to_revid[timestamp]

        revisions = self._get_revisions(page_title, debug)

        if debug:
            print("Total of {0} revisions for {1}".format(len(revisions), page_title))

        revisions = [rev for rev in revisions if rev['timestamp'] < timestamp]

        if debug:
            print("{0} revisions before {1}".format(len(revisions), timestamp))

        if revisions:
            revid = revisions[-1]['revid']

            timestamps_to_revid[timestamp] = revid

            return revid
        else:
            timestamps_to_revid[timestamp] = None
            return None

    def _get_revisionid_nearest_to_date(self, page_title, timestamp):
        """"""
        page_title = self._get_canonical_page_title(page_title)

        timestamps_to_revid = self.ctitle_to_timestamps_to_revid[page_title]

        if timestamp in timestamps_to_revid:
            return timestamps_to_revid[timestamp]

        revisions = self._get_revisions(page_title)

        if revisions:
            revisions = min(revisions, key=lambda d: abs(timestamp-d['timestamp']))

            revid = revisions['revid']

            timestamps_to_revid[timestamp] = revid

            return revid
        else:
            timestamps_to_revid[timestamp] = None
            return None

    def _get_first_revisionid(self, page_title):
        """"""
        page_title = self._get_canonical_page_title(page_title)

        timestamps_to_revid = self.ctitle_to_timestamps_to_revid[page_title]

        revisions = self._get_revisions(page_title)

        if revisions:
            revid = revisions[0]['revid']

            timestamps_to_revid['timestamp'] = revid

            return revid
        else:
            #timestamps_to_revid[timestamp] = None
            return None

    # def _get_revision_text(self, page_title, revid):
    #     """"""
    #     page_title = self._get_canonical_page_title(page_title)
    #
    #     revids_to_articletext = self.ctitle_to_revids_to_articletext[page_title]
    #
    #     if revid in revids_to_articletext:
    #         return revids_to_articletext[revid]
    #
    #     page = self._get_wikipage(page_title)
    #
    #     articletext = page.getOldVersion(revid, get_redirect=True)
    #
    #     revids_to_articletext[revid] = articletext
    #
    #     return articletext

    def _get_revision_text(self, page_title, revid):

        revids_to_texts = self.ctitle_to_revids_to_texts[page_title]

        if revid in revids_to_texts:
            return revids_to_texts[revid]

        wikitext = self._get_revision_wikitext(page_title, revid)

        text = mwparse(wikitext).strip_code()

        revids_to_texts[revid] = text

        return text

    def _get_revision_word_dist(self, page_title, revid):
        """"""
        revids_to_word_dist = self.ctitle_to_revids_to_word_dist[page_title]

        if revid in revids_to_word_dist:
            return revids_to_word_dist[revid]

        text = self._get_revision_text(page_title, revid)

        text = [word.lower() for word in wordpunct_tokenize(text)
                if word.lower() not in STOPWORDS and word.lower() not in PUNCTUATION]

        pdist = StatsCounter(text).normalize()

        revids_to_word_dist[revid] = pdist

        return pdist

    def _get_revision_forwardlinks(self, page_title, revid):
        """"""
        page_title = self._get_canonical_page_title(page_title)

        revids_to_forwardlinks = self.ctitle_to_revids_to_forwardlinks[page_title]

        if revid in revids_to_forwardlinks:
            return revids_to_forwardlinks[revid]

        wikitext = self._get_revision_text(page_title, revid)

        forwardlinks = self._extract_links_from_wikitext(wikitext)

        revids_to_forwardlinks[revid] = forwardlinks

        return forwardlinks

    def _extract_links_from_wikitext(self, wikitext):
        """
        TODO: this should be better implemented
        """
        return [link for link in WIKILINK_RE.findall(wikitext)
                if not WIKINAMESPACE_RE.match(link)]

    def crawl(self, *args, **kwargs):
        """"""
        seed_page_title = kwargs.get('seed_page_title', self.seed_page_title)
        seed_date = kwargs.get('seed_date', self.seed_date)
        seed_count = kwargs.get('seed_count', self.seed_count)
        debug = kwargs.get('debug', False)
        q = self.q

        graph = nx.Graph()

        if debug:
            print("WikipediaCrawler.crawl_wiki starting")

        revid = self._get_revisionid_before_date(seed_page_title, seed_date, debug)

        if not revid:
            raise Exception("No wikipages for article {0} before date {1}".format(seed_page_title, seed_date))

        q.put((seed_page_title, revid))

        for page_title, revid in iter(q.get, None):
            if len(graph) > seed_count:
                break
            if debug:
                print()
                print("Number of nodes in graph: {0}".format(len(graph)))

            outlinks = self._get_revision_forwardlinks(page_title, revid)

            outlinks = [self._get_canonical_page_title(link)
                             for link in outlinks]

            for outlink in outlinks:
                if debug:
                    print("Outlink: {0}".format(outlink))

                if outlink:
                    revid = self._get_revisionid_before_date(outlink, seed_date, debug)

                    if revid:
                        if debug:
                            print("Crawled: {0}".format(outlink))
                            if len(graph)%10 == 0:
                                print("Number of nodes in graph: {0}".format(len(graph)))

                        q.put((outlink, revid))

                    graph.add_edge(page_title, outlink)

        self.graphs.append(graph)

        return graph.edges()

    def crawl_heuristic_bfs(self, *args, **kwargs):
        """Heuristic breadth-first search crawl"""
        page_title = kwargs.get('page_title', self.page_title)
        timestamp = kwargs.get('timestamp', self.timestamp)
        node_count = kwargs.get('node_count', self.node_count)
        debug = kwargs.get('debug', False)

        div_from_root = kwargs.get('div_from_root', False)
        div_func = kwargs.get('div_func', lambda x, y: jsd2(x,y))

        date_from_revision = kwargs.get('date_from_revision', False)

        graph = nx.Graph()

        self.graphs.append(graph)

        if debug: print("MultiThreadedWikiCrawler.crawl_wiki starting")

        q = queue.PriorityQueue()

        visited = set()

        q.put((0, page_title))

        root_pdist = None
        root_title = None

        for score, page_title in iter(q.get, None):
            if page_title in visited:
                continue

            if len(graph) > node_count:
                break

            visited.add(page_title)

            if debug: print("{0}".format(page_title))

            revid = self._get_revisionid_before_date(page_title, timestamp, debug)

            if not revid:
                continue

            # Crawl by prioritizing article links with smaller JS divergence
            if not root_pdist and div_from_root:
                root_pdist = self._get_revision_word_dist(page_title, revid)

                root_title = page_title

                if debug:
                    print("{0}: root_pdist's top 3: {1}".format(page_title,
                                                                root_pdist.most_common(3)))

            current_pdist = self._get_revision_word_dist(page_title, revid)

            forward_links = self._get_revision_forward_links(page_title, revid)

            if debug:
                print("{0}: {1} nodes".format(page_title, len(graph)))
                print("{0}: {1} forward links ".format(page_title, len(forward_links)))

            forward_links = [self._get_canonical_page_title(link)
                             for link in forward_links]

            for forward_link in forward_links:
                if forward_link:
                    flink_revid = self._get_revisionid_before_date(forward_link,
                                                                   timestamp)

                    if not flink_revid:
                        continue

                    if div_from_root:
                        if tuple(sorted([root_title, forward_link])) in self.ctitle_and_ctitle_to_div_scores:
                            div = self.ctitle_and_ctitle_to_div_scores[(root_title, forward_link)]
                        else:
                            flink_pdist = self._get_revision_word_dist(forward_link, flink_revid)
                            div = div_func(root_pdist, flink_pdist)
                            self.ctitle_and_ctitle_to_div_scores[(root_title, forward_link)] = div

                        #div = div_func(root_pdist, flink_pdist)
                        q.put((div, forward_link))

                    if tuple(sorted([page_title, forward_link])) in self.ctitle_and_ctitle_to_div_scores:
                        div = self.ctitle_and_ctitle_to_div_scores[(page_title, forward_link)]
                    else:
                        flink_pdist = self._get_revision_word_dist(forward_link, flink_revid)
                        div = div_func(current_pdist, flink_pdist)
                        self.ctitle_and_ctitle_to_div_scores[(page_title, forward_link)] = div

                    #div = div_func(current_pdist, flink_pdist)

                    if not div_from_root:
                        q.put((div, forward_link))

                    if debug:
                        print("{0} -> {1} - divergence: {2}".format(page_title, forward_link, div))

                    graph.add_edge(page_title, forward_link, div=div, revid=flink_revid)

            if debug: print()

        return

    def start_crawl(self, articles, *args, **kwargs):
        #seed_page_title = kwargs.get('seed_page_title', self.seed_page_title)
        seed_date = kwargs.get('timestamp', self.timestamp)
        seed_count = kwargs.get('node_count', self.node_count)
        #num_of_threads = kwargs.get('num_of_threads', 2)

        debug = kwargs.get('debug', False)

        # Set up some threads to fetch the enclosures
        #for i in range(num_of_threads):
        for e, article in enumerate(articles):
            kwargs['page_title'] = article
            if debug: print("Starting thread {}".format(e))
            worker = threading.Thread(target=self.crawl_heuristic_bfs, kwargs=kwargs)
            #worker.setDaemon(True)
            worker.start()

        if debug: print("Waiting for threads")

        self.q.join()

        if debug: print("Threads done")

    def crawl_iter(self, *args, **kwargs):
        """"""
        page_title = kwargs.get('page_title', self.page_title)
        seed_date = kwargs.get('timestamp', self.timestamp)
        seed_count = kwargs.get('node_count', self.node_count)
        debug = kwargs.get('debug', False)

        graph = nx.Graph()

        visited = set()

        #q = queue.Queue()
        q = queue.LifoQueue()

        revid = self._get_revisionid_before_date(page_title, seed_date)

        revid = revid if revid else self._get_revisionid_nearest_to_date(page_title, seed_date)

        if not revid:
            raise Exception("No wikipages for article {0} before date {1}".format(page_title, seed_date))

        q.put((page_title, revid))

        for page_title, revid in iter(q.get, None):
            if debug:
                print("Number of nodes in graph: {0}".format(len(graph)))

            if len(graph) > seed_count:
                break

            outlinks = self._get_revision_forwardlinks(page_title, revid)

            outlinks = [self._get_canonical_page_title(link)
                             for link in outlinks]

            for next_title in outlinks:
                if debug:
                    print("Number of nodes in graph: {0}".format(len(graph)))

                if next_title != None:
                    revid = self._get_revisionid_before_date(next_title, seed_date)

                    if revid:
                        if debug:
                            print("Crawled: {0}".format(next_title))
                            if len(graph)%10 == 0:
                                print("Number of nodes in graph: {0}".format(len(graph)))

                        q.put((next_title, revid))

                    if (page_title, next_title) not in visited:
                        visited.add((page_title, next_title))
                        yield [page_title, next_title]

            self.graphs.append(self.graphs[-1])\

    def crawl_time_series(self, *args, **kwargs):
        seed_page_title = self.seed_page_title = kwargs.get('seed_page_title',
                                                            self.seed_page_title)

        seed_date = self.seed_date = kwargs.get('seed_date', self.seed_date)

        end_date = kwargs.get('end_date', None)

        seed_count = self.seed_count = kwargs.get('seed_count', self.seed_count)

        time_series_interval = kwargs.get('time_series_interval', 'monthly')
        debug = kwargs.get('debug', False)

        time_series_payload = []

        if time_series_interval == 'monthly':
            if seed_date:
                start_year = seed_date.year
                start_month = seed_date.month
            else:
                #
                pass
            for year in range(start_year, 2017):
                for month in range(1, 13):
                    timestamp = datetime(year, month, 1)

                    payload = self.crawl(seed_page_title=seed_page_title,
                                         seed_date=timestamp,
                                         seed_count=seed_count,
                                         debug=debug)

                    time_series_payload.append((timestamp, payload))

                if end_date < timestamp:
                    break

        return time_series_payload

    # async def crawl_async(self, *args, **kwargs):
    #     loop = kwargs.get('loop', asyncio.get_event_loop())
    #
    #     try:
    #         del kwargs['loop']  # assuming searching doesn't take loop as an arg
    #     except KeyError:
    #         pass
    #
    #     # Passing None tells asyncio to use the default ThreadPoolExecutor
    #     r = await loop.run_in_executor(None, self.crawl_wiki_iter, *args, **kwargs)
    #     return r

#def worker(crawler):
