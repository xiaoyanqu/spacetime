import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from io import StringIO

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = 0 if not os.path.exists("successful_urls.txt") else (len(open("successful_urls.txt").readlines()) - 1)
if url_count < 0:
    url_count = 0
MAX_LINKS_TO_DOWNLOAD = 20

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "54848408_36326408_64923885"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 Grad 36326408"

        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if url_count >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks = process_url_group(g, self.UserAgentString)
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if url_count >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        print "downloaded ", url_count, " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    url_count += len(urls)
    with open("successful_urls.txt", "a") as surls:
        surls.write("\n".join(urls) + "\n")

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas)

#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def extract_next_links(rawDatas):
    outputLinks = []
    '''
    rawDatas is a list of tuples -> [(url1, raw_content1), (url2, raw_content2), ....]
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''
    # assert isinstance(rawDatas, list) and len(rawDatas) > 0, "ERROR: wrong input."

    for combo in rawDatas:
        base, string_content = combo[0], combo[1] # assume the content is in String format

        namespace = extract_namespace(string_content)
        relative_urls = parse_String_content(string_content, namespace)
        absolute_urls = []
        for url in relative_urls:
            if url.startswith("http"):
                absolute_urls.append(url)
            else:
                print "******* RELATIVE URL:", url
                absolute_urls.append(relative2absolute(base, url))
        outputLinks.extend(absolute_urls)
        # outputLinks.extend(relative_urls)
    return outputLinks

def extract_namespace(content):
    ns = ""
    # TO DO
    return ns

def relative2absolute(base, relative):
    # base: http://www.ics.uci.edu/community
    # relative: ../something.html
    # abs: http://www.ics.uci.edu/something.html
    if base[-1] == '/':
         base = base[:-1]

    if relative[0] == '/':
        edu = base.find("edu")
        print "********** RECONSTRUCT ABSOLUTE URL FROM '/'':", base[:edu + 3] + relative
        return base[:edu + 3] + relative

    while relative.startswith("../"):
        relative = relative[3:]
        base = trucate_the_last_dir(base)

    print "********** RECONSTRUCT ABSOLUTE URL FROM '../'':", base + '/' + relative
    return base + '/' + relative

def trucate_the_last_dir(base):
    # base: http://www.ics.uci.edu/community
    for i in range(len(base) - 1, -1, -1):
        if base[i] == '/':
            return base[: i]

    # print "base2:", base
    raise Exception("ERROR in trucate_the_last_dir(): base url format worng.")

def parse_String_content(content, xmlns):
    print "==================== Start a new page"
    relative_urls = []
    parser = etree.HTMLParser()
    uni_content = content.decode('unicode-escape')
    tree = etree.parse(StringIO(uni_content), parser)
    iterable = tree.getiterator(tag = xmlns + "a")
    for elm in iterable:
        if 'href' in elm.attrib: # there are <a> elements that don't contain "href" is a hyperlink placeholder.
            print "======== ADD: ", elm.attrib['href']
            relative_urls.append(elm.attrib['href'])
    return relative_urls

def contain_duplications(url):
    """
    this function checks if the given url contains duplicated "elements" that make it more likely a trap.
    such element includes: duplicated php, duplicated http, duplicated same word.
    """

    return contain_duplicated_stopElements(url) or contain_duplicated_addressElements(url)

def contain_duplicated_stopElements(url):
    stop_elements = ["php", "http", "www", ".edu", ".com", "index"]
    for word in stop_elements:
        if url.count(word) > 1:
            print "URL CONTAINS DUPLICATED STOP_ELEMENTS."
            return True
    return False

def contain_duplicated_addressElements(url):
    hm = {}
    elements = url.split('/')

    for elem in elements:
        if elem in hm:
            hm[elem] += 1
        else:
            hm[elem] = 1
    for k in hm:
        # if there're any duplicated elements, return True
        if hm[k] > 1:
            print "URL CONTAINS DUPLICATED ADDRESS ELEMENTS."
            return True
    return False

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    # check url absolute form
    if ".." in url:
        print "url is not in absolute form, 1"
        return False

    if contain_duplications(url):
        return False


    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        print "url is not in absolute form, 2"
        return False

    if "calendar" in url:
        print "encounter calendar."
        return False

    if "archive" in url:
        print "encounter archive."
        return False

    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
