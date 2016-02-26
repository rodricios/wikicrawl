import pywikibot
import networkx as nx
from collections import defaultdict
import queue
import re
import asyncio
from datetime import datetime
from timeit import default_timer as timer

__all__ = ["WikiCrawler"]

WIKILINK_RE = re.compile(r"\[\[(.*?)\]\]")

WIKINAMESPACE_RE = re.compile(r"^File:|^Image:|^Category:|^simple:|^..:")

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
        # aliases (titles) to canonical titles table
        self.title_to_ctitle_table = {}
        # canonical title to canonical wiki page (not redirection page) table
        self.ctitle_to_cpage_table = {}
        # canonical title to revisions
        #self.ctitle_to_revisions = {}
        self.ctitle_to_revids_to_revisions = defaultdict(dict)
        # canonical title to revision ids to wiki article text
        self.ctitle_to_revids_to_articletext = defaultdict(dict)
        # canonical title to revision ids to forward links
        self.ctitle_to_revids_to_forwardlinks = defaultdict(dict)
        # canonical title to timestamp to revid
        self.ctitle_to_timestamps_to_revid = defaultdict(dict)

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
        """"""
        page_title = self._get_canonical_page_title(page_title)

        # check canonical title to revisions table
        #if page_title in self.ctitle_to_revisions:
        #    return self.ctitle_to_revisions[page_title]
        if page_title in self.ctitle_to_revids_to_revisions:
            return self.ctitle_to_revids_to_revisions[page_title].values()

        page = self._get_wikipage(page_title)

        try:
            # page.revisions() returns a generator
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
            print("_get_revisionid_before_date: {}".format(end - start))
            return timestamps_to_revid[timestamp]

        revisions = self._get_revisions(page_title, debug)

        revisions = [rev for rev in revisions if rev['timestamp'] < timestamp]

        if debug:
            print("{0} - {1} revisions before {2}".format(page_title, len(revisions), timestamp))

        if revisions:
            revid = revisions[-1]['revid']

            timestamps_to_revid[timestamp] = revid

            end = timer()
            print("_get_revisionid_before_date: {}".format(end - start))

            return revid
        else:
            timestamps_to_revid[timestamp] = None
            return None

    def _get_revision_text(self, page_title, revid):
        """"""
        page_title = self._get_canonical_page_title(page_title)

        revids_to_articletext = self.ctitle_to_revids_to_articletext[page_title]

        if revid in revids_to_articletext:
            return revids_to_articletext[revid]

        page = self._get_wikipage(page_title)

        articletext = page.getOldVersion(revid, get_redirect=True)

        revids_to_articletext[revid] = articletext

        return articletext

    def _get_revision_forward_links(self, page_title, revid):
        """"""
        start = timer()

        page_title = self._get_canonical_page_title(page_title)

        revids_to_forwardlinks = self.ctitle_to_revids_to_forwardlinks[page_title]

        if revid in revids_to_forwardlinks:
            end = timer()
            print("_get_revision_forward_links: {}".format(end - start))
            return revids_to_forwardlinks[revid]

        wikitext = self._get_revision_text(page_title, revid)

        forwardlinks = self._extract_links_from_wikitext(wikitext)

        revids_to_forwardlinks[revid] = forwardlinks

        end = timer()
        print("_get_revision_forward_links: {}".format(end - start))

        return forwardlinks

    def _extract_links_from_wikitext(self, wikitext):
        """
        TODO: this should be better implemented
        """
        return [link for link in WIKILINK_RE.findall(wikitext)
                if not WIKINAMESPACE_RE.match(link)]

    def crawl(self, *args, **kwargs):
        """Breadth-first search crawl"""
        page_title = kwargs.get('page_title', self.page_title)
        timestamp = kwargs.get('timestamp', self.timestamp)
        node_count = kwargs.get('node_count', self.node_count)
        debug = kwargs.get('debug', False)

        graph = nx.Graph()

        if debug: print("WikiCrawler.crawl_wiki starting")

        q = queue.Queue()

        visited = set()

        q.put((page_title, timestamp))

        for page_title, date in iter(q.get, None):
            if page_title in visited:
                continue

            if len(graph) > node_count:
                break

            visited.add(page_title)

            revid = self._get_revisionid_before_date(page_title, date, debug)

            if not revid:
                continue

            revdate = self._get_revision_data(page_title, revid)['timestamp']

            forward_links = self._get_revision_forward_links(page_title, revid)

            forward_links  = [self._get_canonical_page_title(link)
                              for link in forward_links]

            if debug:
                print("{0}: {1} nodes".format(page_title, len(graph)))
                print("{0}: {1} forward links ".format(page_title, len(forward_links )))

            for forward_link in forward_links:
                if debug:
                    print("{0} -> {1}".format(page_title, forward_link))

                if forward_link:
                    q.put((forward_link, revdate))
                    graph.add_edge(page_title, forward_link)

            if debug: print()

        self.graphs.append(graph)

        return #graph.edges()
    
    def crawl(self, *args, **kwargs):
        """Breadth-first search crawl"""
        page_title = kwargs.get('page_title', self.page_title)
        timestamp = kwargs.get('timestamp', self.timestamp)
        node_count = kwargs.get('node_count', self.node_count)
        debug = kwargs.get('debug', False)

        graph = nx.Graph()

        if debug: print("WikiCrawler.crawl_wiki starting")

        q = queue.Queue()

        visited = set()

        q.put((page_title, timestamp))

        for page_title, date in iter(q.get, None):
            if page_title in visited:
                continue

            if len(graph) > node_count:
                break

            visited.add(page_title)

            revid = self._get_revisionid_before_date(page_title, date, debug)

            if not revid:
                continue

            revdate = self._get_revision_data(page_title, revid)['timestamp']

            forward_links = self._get_revision_forward_links(page_title, revid)

            forward_links  = [self._get_canonical_page_title(link)
                              for link in forward_links]

            if debug:
                print("{0}: {1} nodes".format(page_title, len(graph)))
                print("{0}: {1} forward links ".format(page_title, len(forward_links )))

            for forward_link in forward_links:
                if debug:
                    print("{0} -> {1}".format(page_title, forward_link))

                if forward_link:
                    q.put((forward_link, revdate))
                    graph.add_edge(page_title, forward_link)

            if debug: print()

        self.graphs.append(graph)

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

    def crawl_time_series(self, *args, **kwargs):
        page_title = self.page_title = kwargs.get('page_title',
                                                  self.page_title)

        timestamp = self.timestamp = kwargs.get('timestamp', self.timestamp)

        end_date = kwargs.get('end_date', None)

        node_count = self.node_count = kwargs.get('node_count', self.node_count)

        time_series_interval = kwargs.get('time_series_interval', 'monthly')

        debug = kwargs.get('debug', False)

        time_series_payload = []

        if time_series_interval == 'monthly':
            if timestamp:
                start_year = timestamp.year
                start_month = timestamp.month
            else:
                #
                pass
            for year in range(start_year, 2017):
                if end_date < timestamp:
                    break

                for month in range(1, 13):
                    timestamp = datetime(year, month, 1)

                    payload = self.crawl(page_title=page_title,
                                         timestamp=timestamp,
                                         node_count=node_count,
                                         debug=debug)

                    time_series_payload.append((timestamp, payload))

        return time_series_payload

    def get(self, key):
        if not self.graphs:
            return
        if len(self.graphs) == 1:
            return self.graphs[0][key]
        else:
            return [graph[key] for graph in self.graphs]

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

    async def crawl_async(self, *args, **kwargs):
        loop = kwargs.get('loop', asyncio.get_event_loop())

        try:
            del kwargs['loop']  # assuming searching doesn't take loop as an arg
        except KeyError:
            pass

        # Passing None tells asyncio to use the default ThreadPoolExecutor
        r = await loop.run_in_executor(None, self.crawl_wiki_iter, *args, **kwargs)
        return r
