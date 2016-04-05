from collections import defaultdict
import queue
import re
from datetime import datetime
from timeit import default_timer as timer
from string import punctuation

import pywikibot
import networkx as nx
from mwparserfromhell import parse as mwparse
from statscounter import StatsCounter
from nltk.tokenize import wordpunct_tokenize
from nltk.corpus import stopwords

from wikicrawl.tools.divtools import jsd2
from wikicrawl.tools.containers import SortedKeyDict

STOPWORDS = set(stopwords.words('english'))
PUNCTUATION = set(punctuation)

WIKILINK_RE = re.compile(r"\[\[(.*?)\]\]")

WIKINAMESPACE_RE = re.compile(r"^File:|^Image:|^Category:|^simple:|^..:|^#.*")

class WikiCrawler:
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
        self.bad_pages = []
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

    def __getitem__(self, key):
        if not self.graphs:
            return
        if len(self.graphs) == 1:
            return self.graphs[0][key]
        else:
            return [graph[key] for graph in self.graphs]

    def _get_wikipage(self, page_title):
        """"""
        if page_title in self.title_to_ctitle_table:
            page_title = self.title_to_ctitle_table[page_title]

        #assert(page_title != '', "Page title is empty string")

        if page_title in self.ctitle_to_cpage_table:
            return self.ctitle_to_cpage_table[page_title]

        page = pywikibot.Page(self._wiki_site, page_title)

        try:
            # if not canonical page
            if page.isRedirectPage():
                page = page.getRedirectTarget()
        except Exception as e:
            print(e, "page_title:", page_title)
            self.bad_pages.append(page_title)
            #raise
            return None

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

        page = self._get_wikipage(page_title)
        if page:
            return page.title()
        return None

    def _get_revisions(self, page_title, starttime, debug=False):
        """"""
        if debug: print("{0} - _get_revisions starttime {1}".format(page_title, starttime))
        page_title = self._get_canonical_page_title(page_title)

        # check canonical title to revisions table
        #if page_title in self.ctitle_to_revids_to_revisions:
        #    return sorted(self.ctitle_to_revids_to_revisions[page_title].values(), key=lambda r: r['timestamp'])

        page = self._get_wikipage(page_title)

        try:
            self._wiki_site.loadrevisions(page, getText=True, total=1, starttime=starttime)
            # page.revisions() returns a generator
            # sorted property/invariant
            # sorted may be too inefficient
            revisions = sorted(page._revisions.values(), key=lambda r: r['timestamp'])
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

            return revisions

    def _get_revision_data(self, page_title, revid):
        """"""
        return self.ctitle_to_revids_to_revisions[page_title].get(revid, None)

    def _get_revisionid_before_date(self, page_title, timestamp, debug=False):
        """"""
        start = timer()

        page_title = self._get_canonical_page_title(page_title)

        timestamps_to_revid = self.ctitle_to_timestamps_to_revid[page_title]

        if timestamp in timestamps_to_revid:
            end = timer()
            if debug:
                print("_get_revisionid_before_date: {}".format(end - start))
            return timestamps_to_revid[timestamp]

        revisions = self._get_revisions(page_title, starttime=timestamp, debug=debug)

        if debug:
            print("{0} - {1} revisions before {2}".format(page_title, len(revisions), timestamp))

        if revisions:
            revid = revisions[-1]['revid']

            timestamps_to_revid[timestamp] = revid

            end = timer()
            if debug:
                print("Revision id: {0} - _get_revisionid_before_date: {1}".format(revid, (end - start)))

            return revid
        else:
            timestamps_to_revid[timestamp] = None
            return None

    def _get_revision_wikitext(self, page_title, revid):
        """"""
        page_title = self._get_canonical_page_title(page_title)

        revids_to_revisions = self.ctitle_to_revids_to_revisions[page_title]

        if revid in revids_to_revisions:
            if revids_to_revisions[revid].text:
                return revids_to_revisions[revid].text

        page = self._get_wikipage(page_title)

        wikitext = page.getOldVersion(revid, get_redirect=True)

        revids_to_revisions[revid].text = wikitext

        return wikitext

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

    def _get_revision_forward_links(self, page_title, revid, debug=False):
        """"""
        start = timer()

        page_title = self._get_canonical_page_title(page_title)

        revids_to_forwardlinks = self.ctitle_to_revids_to_forwardlinks[page_title]

        if revid in revids_to_forwardlinks:
            end = timer()
            if debug: print("_get_revision_forward_links: {}".format(end - start))
            return revids_to_forwardlinks[revid]

        wikitext = self._get_revision_wikitext(page_title, revid)

        forwardlinks = self._extract_links_from_wikitext(wikitext)

        revids_to_forwardlinks[revid] = forwardlinks

        end = timer()
        if debug: print("_get_revision_forward_links: {}".format(end - start))

        return forwardlinks

    def _extract_links_from_wikitext(self, wikitext):
        """
        TODO: this should be better implemented
        """
        return [link for link in WIKILINK_RE.findall(wikitext)
                if not WIKINAMESPACE_RE.match(link)]

    def _get_div_score(self, title_A, revid_A, title_B, revid_B, div_func):
        """"""
        if tuple(sorted([title_A, title_B])) in self.ctitle_and_ctitle_to_div_scores:
            return self.ctitle_and_ctitle_to_div_scores[(title_A, title_B)]
        else:
            pdist_A = self._get_revision_word_dist(title_A, revid_A)
            pdist_B = self._get_revision_word_dist(title_B, revid_B)

            div = div_func(pdist_A, pdist_B)
            self.ctitle_and_ctitle_to_div_scores[(title_A, title_B)] = div
            return div

    def crawl_heuristic_bfs(self, *args, **kwargs):
        """Heuristic breadth-first search crawl"""
        page_title = kwargs.get('page_title', self.page_title)
        timestamp = kwargs.get('timestamp', self.timestamp)
        node_count = kwargs.get('node_count', self.node_count)
        #first_layer_only = kwargs.get('first_layer_only', False)
        div_from_root = kwargs.get('div_from_root', False)
        div_func = kwargs.get('div_func', lambda x, y: jsd2(x,y))

        debug = kwargs.get('debug', False)

        date_from_revision = kwargs.get('date_from_revision', False)

        graph = nx.Graph(name="{0}&&{1}".format(page_title, timestamp.date()),
                         starting_node=page_title, starting_date=timestamp, year=timestamp.year)

        self.graphs.append(graph)

        if debug: print("WikiCrawler.crawl_wiki starting")

        q = queue.PriorityQueue()

        visited = set()

        q.put((0, page_title))

        root_pdist = None
        root_title = None
        root_revid = None

        #for score, page_title in iter(q.get, None):
        while not q.empty():
            score, page_title = q.get()

            if page_title in visited:
                continue

            if len(graph) > node_count:
                break

            visited.add(page_title)

            if debug: print("{0}".format(page_title))

            revid = self._get_revisionid_before_date(page_title, timestamp, debug)

            if not revid:
                if debug: print("{0} - no revid found".format(page_title))
                continue

            # Crawl by prioritizing article links with smaller JS divergence
            if not root_pdist and div_from_root:
                root_pdist = self._get_revision_word_dist(page_title, revid)

                root_title = page_title

                root_revid = revid

                if debug:
                    print("{0}: root_pdist's top 3: {1}".format(page_title,
                                                                root_pdist.most_common(3)))

            #current_pdist = self._get_revision_word_dist(page_title, revid)

            forward_links = self._get_revision_forward_links(page_title, revid, debug)

            if debug:
                print("{0}: {1} nodes".format(page_title, len(graph)))
                print("{0}: {1} forward links ".format(page_title, len(forward_links)))

            forward_links = [self._get_canonical_page_title(link)
                             for link in forward_links]

            forward_links = [flink for flink in forward_links if flink]

            for forward_link in forward_links:
                if forward_link:
                    flink_revid = self._get_revisionid_before_date(forward_link, timestamp)

                    if not flink_revid:
                        continue

                    if div_from_root:
                        div = self._get_div_score(root_title, root_revid, forward_link, flink_revid, div_func=div_func)

                        #div = div_func(root_pdist, flink_pdist)
                        q.put((div, forward_link))

                    #div = div_func(current_pdist, flink_pdist)

                    if not div_from_root:
                        div = self._get_div_score(page_title, revid, forward_link, flink_revid, div_func=div_func)
                        q.put((div, forward_link))

                    if debug:
                        print("{0} -> {1} - divergence: {2}".format(page_title, forward_link, div))

                    graph.add_edge(page_title, forward_link, div=div, revid=flink_revid)

            if debug: print()
        return

    def crawl_time_series(self, *args, **kwargs):
        timestamp = self.timestamp = kwargs.get('timestamp', self.timestamp)

        time_series_interval = kwargs.get('time_series_interval', 'monthly')
        crawl_type = kwargs.get('crawl_type', 'heuristic')
        debug = kwargs.get('debug', False)

        time_series_payload = []

        if time_series_interval == 'monthly':
            intervals = [datetime(year, month, 1)
                         for year in range(timestamp.year, 2017)
                         for month in range(1, 13)]
        elif time_series_interval == 'yearly':
            intervals = [datetime(year, 1, 1)
                         for year in range(timestamp.year, 2017)]

        for interval in intervals:
            kwargs['timestamp'] = interval

            if debug: print("Date: {}".format(interval))
            if crawl_type == 'heuristic':

                self.crawl_heuristic_bfs(**kwargs)
            else:
                self.crawl(**kwargs)

        return

    def crawl(self, *args, **kwargs):
        """Breadth-first search crawl"""
        page_title = kwargs.get('page_title', self.page_title)
        timestamp = kwargs.get('timestamp', self.timestamp)
        node_count = kwargs.get('node_count', self.node_count)
        debug = kwargs.get('debug', False)

        if not page_title:
            raise Exception("No page title given.")

        graph = nx.Graph()

        self.graphs.append(graph)

        if debug: print("WikiCrawler.crawl_wiki starting")

        q = queue.Queue()

        visited = set()

        q.put(page_title)

        for page_title in iter(q.get, None):
            if page_title in visited:
                continue

            if len(graph) > node_count:
                break

            visited.add(page_title)

            revid = self._get_revisionid_before_date(page_title, timestamp, debug)

            if not revid:
                continue

            forward_links = self._get_revision_forward_links(page_title, revid)

            forward_links  = [self._get_canonical_page_title(link)
                              for link in forward_links]

            if debug:
                print("{0}: {1} nodes".format(page_title, len(graph)))
                print("{0}: {1} forward links ".format(page_title, len(forward_links)))

            for forward_link in forward_links:
                if debug:
                    print("{0} -> {1}".format(page_title, forward_link))

                if forward_link:
                    q.put(forward_link)
                    graph.add_edge(page_title, forward_link)

            if debug: print()

        return #graph.edges()

    def crawl_iter(self, *args, **kwargs):
        """"""
        page_title = kwargs.get('page_title', self.page_title)
        timestamp = kwargs.get('timestamp', self.timestamp)
        node_count = kwargs.get('node_count', self.node_count)
        debug = kwargs.get('debug', False)

        graph = nx.Graph()

        visited = set()

        q = queue.Queue()
        #q = queue.LifoQueue()

        revid = self._get_revisionid_before_date(page_title, timestamp)

        revid = revid if revid else self._get_revisionid_nearest_to_date(page_title, timestamp)

        if not revid:
            raise Exception("No wikipages for article {0} before date {1}".format(page_title, timestamp))

        q.put((page_title, revid))

        for page_title, revid in iter(q.get, None):
            if debug:
                print("Number of nodes in graph: {0}".format(len(graph)))

            if len(graph) > node_count:
                return

            outlinks = self._get_revision_forward_links(page_title, revid)

            outlinks = [self._get_canonical_page_title(link)
                             for link in outlinks]

            for outlink in outlinks:
                if len(graph) > node_count:
                    return

                if debug:
                    print("Number of nodes in graph: {0}".format(len(graph)))

                if outlink != None:
                    revid = self._get_revisionid_before_date(outlink, timestamp)

                    if revid:
                        if debug:
                            print("Crawled: {0}".format(outlink))
                            if len(graph)%10 == 0:
                                print("Number of nodes in graph: {0}".format(len(graph)))

                        q.put((outlink, revid))

                    if (page_title, outlink) not in visited:
                        visited.add((page_title, outlink))
                        yield [page_title, outlink]

        self.graphs.append(self.graphs[-1])

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
    #
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
