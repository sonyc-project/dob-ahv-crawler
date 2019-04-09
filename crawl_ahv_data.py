# crawl_ahv_data.py
# Author: Fabio Miranda
# https://github.com/sonyc-project/dob-ahv-crawler

from scrapy.crawler import CrawlerProcess
from w3lib.html import remove_tags, remove_tags_with_content

import scrapy
import argparse
import re
import os
import urllib.parse
import time

# 1) first step, using borough, block, lot
# http://a810-bisweb.nyc.gov/bisweb/PropertyBrowseByBBLServlet?allborough=1&allblock=00500&alllot=7506&go5=+GO+&requestid=0
# - get bin from that page
#
# 2) use bin to get following page:
# http://a810-bisweb.nyc.gov/bisweb/AHVPermitsQueryByNumberServlet?requestid=2&allkey=1028827&fillerdata=A
# http://a810-bisweb.nyc.gov/bisweb/AHVPermitsQueryByNumberServlet?requestid=2&allkey=1025450&fillerdata=A&allcount=0001
# http://a810-bisweb.nyc.gov/bisweb/AHVPermitsQueryByNumberServlet?next.x=41&next.y=19&allcount=0071&boroughkey=I&allinquirytype=BXS2AVBR&allfileid=null&allkey=1025450&readsw=D&passjobnumber=&requestid=3&fillerdata=A
# allcount takes to next page
# - get reference number
#
# 3) use reference number to get following page:
# http://a810-bisweb.nyc.gov/bisweb/AHVPermitDetailsServlet?requestid=3&allkey=00744246
# - parse this page
#
# remove requestid?
# allcount apperently is the start index

propertyBrowseUrl = "http://a810-bisweb.nyc.gov/bisweb/PropertyBrowseByBBLServlet?"
ahvPermitsListUrl = "http://a810-bisweb.nyc.gov/bisweb/AHVPermitsQueryByNumberServlet?requestid=2&fillerdata=A&"
ahvPermitDetailsUrl = "http://a810-bisweb.nyc.gov/bisweb/AHVPermitDetailsServlet?requestid=3&"

class BISSpider(scrapy.Spider):
    name = 'bisspider'
    custom_settings = {
        'CONCURRENT_REQUESTS': 4,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'DOWNLOAD_DELAY': 1,
        'LOG_FILE': './dob-ahv.log',
        'LOG_ENABLED': True,
        # 'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        'FAKEUSERAGENT_FALLBACK': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 100,
        'RETRY_HTTP_CODES': [500, 502, 504, 408, 403, 400, 429, 470],
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_DIR': './httpcache',
        'DOWNLOADER_MIDDLEWARES' : {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
        }
    }
    handle_httpstatus_list = [503]
    start_urls = []
    outputPath = None
    count = 0

    def __init__(self, urls, outputPath):
        self.start_urls = urls
        self.outputPath = outputPath

        if not os.path.exists(self.outputPath):
            os.makedirs(self.outputPath)

    # parse result from PropertyBrowseByBBLServlet
    def parse(self, response):

        print(response.request.headers['User-Agent'])
        print("bbl %d of %d"%(self.count,len(self.start_urls)))
        self.count+=1

        if response.status == 503:
            # print("Retrying parse 503:", response.url)
            time.sleep(1)

            if 'num' in response.meta:
                response.meta['num'] = int(response.meta['num']) + 1
            else:
                response.meta['num'] = 1

            # add this so that cache is ignored
            nextPage = response.url + "&ignoreCache="+str(response.meta['num'])
            request = scrapy.Request(url=nextPage, callback=self.parse, meta=response.meta, dont_filter=True)
            yield request

            return

        par = urllib.parse.parse_qs(urllib.parse.urlparse(response.url).query)
        bbl = par['allborough'][0]+par['allblock'][0]+par['alllot'][0]

        # save html
        path = self.outputPath + '/' + bbl
        if not os.path.exists(path):
            os.makedirs(path)

        f = open(path+'/bin-list.html', 'wb')
        f.write(response.body)
        f.close()

        # parse html
        error1 = response.xpath('//p[starts-with(text(),"Just a moment")]/text()')
        error2 = response.xpath('//td[starts-with(text(),"Building Information System Error")]/text()')
        bins = response.xpath('//a[starts-with(@href,"PropertyProfileOverviewServlet?")]/text()')

        # print("===================================")
        # print("Error parse:", len(error1), len(error2))
        # print("===================================")

        # retry if error found
        if len(error1) > 0 or len(error2) > 0:
            if 'num' in response.meta:
                response.meta['num'] = int(response.meta['num']) + 1
            else:
                response.meta['num'] = 1

            time.sleep(5)
            # add this so that cache is ignored
            nextPage = response.url + "&ignoreCache="+str(response.meta['num'])
            request = scrapy.Request(url=nextPage, callback=self.parse, meta=response.meta, dont_filter=True)
            yield request
            
        else:
            for b in bins:
                b = b.extract()
                b = b[:b.find(u'\xa0')]
                nextPage = ahvPermitsListUrl+"allkey="+b+"&allcount=1"
                request = scrapy.Request(url=nextPage, callback=self.parseAhvList, meta={'bbl': bbl, 'bin': b, 'start': 1})
                yield request

    # parse result from AHVPermitsQueryByNumberServlet
    def parseAhvList(self, response):

        if response.status == 503:
            # print("Retrying parseAhvList 503:", response.url)
            time.sleep(1)

            if 'num' in response.meta:
                response.meta['num'] = int(response.meta['num']) + 1
            else:
                response.meta['num'] = 1

            # add this so that cache is ignored
            nextPage = response.url + "&ignoreCache="+str(response.meta['num'])
            request = scrapy.Request(url=nextPage, callback=self.parseAhvList, meta=response.meta, dont_filter=True)
            yield request

            return

        bbl = response.meta['bbl']
        b = response.meta['bin']
        start = response.meta['start'] 

        # save html
        path = self.outputPath + '/' + bbl
        if not os.path.exists(path):
            os.makedirs(path)

        f = open(path+'/ahv-list-'+str(b)+'-'+str(start)+'.html', 'wb')
        f.write(response.body)
        f.close()

        # parse html
        error1 = response.xpath('//p[starts-with(text(),"Just a moment")]/text()')
        error2 = response.xpath('//td[starts-with(text(),"Building Information System Error")]/text()')
        references = response.xpath('//a[starts-with(@href,"AHVPermitDetailsServlet?")]/text()')

        # print("===================================")
        # print("Error parseAhvList:", len(error1), len(error2))
        # print("Start index:", start)
        # print(response.url)
        # print("===================================")

        # request next page
        if len(references) >= 70:
            newStart = start + 70
            nextPage = response.url[:response.url.find("allcount=")] + "allcount="+str(newStart)
            request = scrapy.Request(url=nextPage, callback=self.parseAhvList, meta={'bbl': bbl, 'bin': b, 'start': newStart, 'num': 1}, dont_filter=True)
            yield request

        # retry if error found
        if len(error1) > 0 or len(error2) > 0:
            if 'num' in response.meta:
                response.meta['num'] = int(response.meta['num']) + 1
            else:
                response.meta['num'] = 1
            time.sleep(5)
            # add this so that cache is ignored
            nextPage = response.url + "&ignoreCache="+str(response.meta['num'])
            request = scrapy.Request(url=nextPage, callback=self.parseAhvList, meta=response.meta, dont_filter=True)
            yield request
            
        else:
            for r in references:
                r = r.extract()
                nextPage = ahvPermitDetailsUrl+"allkey="+r
                request = scrapy.Request(url=nextPage, callback=self.parseAhvDetails, meta={'bbl': bbl, 'bin': b, 'reference': r, 'num': 1})
                yield request


    # parse result from AHVPermitDetailsServlet 
    def parseAhvDetails(self, response):

        if response.status == 503:
            # print("Retrying parseAhvDetails 503:", response.url)
            time.sleep(1)

            if 'num' in response.meta:
                response.meta['num'] = int(response.meta['num']) + 1
            else:
                response.meta['num'] = 1

            # add this so that cache is ignored
            nextPage = response.url + "&ignoreCache="+str(response.meta['num'])
            request = scrapy.Request(url=nextPage, callback=self.parseAhvDetails, meta=response.meta, dont_filter=True)
            yield request

            return

        bbl = response.meta['bbl']
        b = response.meta['bin']
        r = response.meta['reference']

        # save html
        path = self.outputPath + '/' + bbl
        if not os.path.exists(path):
            os.makedirs(path)

        f = open(path+'/ahv-'+str(b)+'-'+str(r)+'.html', 'wb')
        f.write(response.body)
        f.close()

        # parse html
        error1 = response.xpath('//p[starts-with(text(),"Just a moment")]/text()')
        error2 = response.xpath('//td[starts-with(text(),"Building Information System Error")]/text()')

        # print("===================================")
        # print("Error parseAhvDetails:", len(error1), len(error2))
        # print("===================================")

        # retry if error found
        if len(error1) > 0 or len(error2) > 0:
            if 'num' in response.meta:
                response.meta['num'] = int(response.meta['num']) + 1
            else:
                response.meta['num'] = 1
            time.sleep(5)
            # add this so that cache is ignored
            nextPage = response.url + "&ignoreCache="+str(response.meta['num'])
            request = scrapy.Request(url=nextPage, callback=self.parseAhvDetails, meta=response.meta, dont_filter=True)
            yield request
            
def getbbl(line):

    borough = line[0]
    block = line[1:6]
    lot = line[6:]

    return borough, block, lot

def crawl(inputPath, outputPath, boroughs):

    infile = open(inputPath)
    permitUrls = []
    for line in infile:
        tks = re.split(', | ', line.rstrip())
        borough, block, lot = getbbl(tks[0])
        if borough in boroughs:
            url = propertyBrowseUrl+"allborough="+str(borough)+"&allblock="+str(block)+"&alllot="+str(lot)
            permitUrls.append(url)


    process = CrawlerProcess()
    process.crawl(BISSpider, permitUrls, outputPath)
    process.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Crawl DOB website to get AHV permit htmls.')

    parser.add_argument('-i', '--input', nargs='?', required=True,
                        help='BBLs input csv.')

    parser.add_argument('-b', '--boroughs', nargs='*', required=False, default=[1, 2, 3, 4, 5],
                        help='List of boroughs to consider. E.g. 1 2 3 4 5 to crawl for Manhattan, Bronx, Brooklyn, Queens and Staten Island permits.')

    parser.add_argument('-o', '--output', nargs='?', required=True,
                        help='Output file.')

    args = parser.parse_args()

    crawl(args.input, args.output, args.boroughs)

