# crawl_ahv_data.py
# Author: Fabio Miranda
# https://github.com/sonyc-project/dob-ahv-crawler

from scrapy.selector import HtmlXPathSelector
from scrapy.http import TextResponse

import argparse
import re
import os
import glob
import time
import scrapy
import csv

# output csv:
# bbl, borough, block, lot, bin, premises, jobno, referenceno, permitno, jobtype, efiled, feeexempt, filingtype, daysbilled, status, entrydate, decisiondate, totalfee
# houseno, streetname, floors, aptno, cbno
# contratorname, contractorbusinessname, contractorbusinessaddr, contractoremail, contractorlicensetype, contractorphone, contractorlicense,
# residencewithin200feet, workwithinenclosedbuilding, fullorpatialdemolition, craneuse
# daysrequested, daysapproved, approvedfor, erenew
# applyreason, approved, erenewauthorized
# description

HEADER = 'bbl,borough,block,lot,bin,premises,jobno,referenceno,permitno,jobtype,efiled,feeexempt,filingtype,daysbilled,status,totalfee,houseno,streetname,floors,cbno,' \
       + 'contractorname,contractorbusinessname,contractorbusinessaddr,contractorlicensetype,contractorphone,contractorlicense,' \
       + 'residencewithin200feet,workwithinenclosedbuilding,fullorpatialdemolition,craneuse,daysrequested,daysapproved,approvedfor,erenew,' \
       + 'applyreason,approved,erenewalauthorized,description'

boroughDict = {'bronx': 2, 'brooklyn': 3, 'manhattan': 1, 'queens': 4, 'staten island': 5}

def parse(inputPath, outputPath):

    outfile = open(outputPath, 'w')
    outfile.write(HEADER+'\n')

    files = glob.glob(inputPath)
    count = 1
    for fname in files:
        print "%d of %d: %s"%(count, len(files), fname)
        count+=1
        f = csv.writer(open(fname,'w'),delimeter=',',quoting=csv.QUOTE_NONNUMERIC)
        response = TextResponse(url = '', body = f.read(), encoding = 'utf-8')

        ahvtitle = response.xpath('//td[starts-with(text(),"After Hours Variance Permit Data")]/text()')

        if len(ahvtitle) > 0:
            borough = response.xpath('//td[starts-with(text(),"Borough:")]/following-sibling::td[1]/text()').extract()[0]
            boroughid = boroughDict[borough.lower()]
            
            #########################
            block = response.xpath('//td[starts-with(text(),"Block:")]/following-sibling::td[1]/text()').extract()[0].zfill(5)
            lot = response.xpath('//td[starts-with(text(),"Lot:")]/following-sibling::td[1]/text()').extract()[0].zfill(4)
            bbl = str(boroughid)+block+lot
            bin = response.xpath('//td[starts-with(text(),"BIN:")]/a[1]/text()').extract()[0]
            premises = response.xpath('//td[starts-with(text(),"Premises:")]/text()').extract()[0]
            premises = premises[len("Premises:")+1:]
            jobno = response.xpath('//td[starts-with(text(),"Job No:")]/a[1]/text()').extract()[0]
            referenceno = response.xpath('//td[starts-with(text(),"Reference Number:")]/text()').extract()[0]
            referenceno = referenceno[len("Reference Number:")+1:]
            permitno = response.xpath('//td[starts-with(text(),"Work Permit No:")]/following-sibling::td[1]/a[1]/text()').extract()[0]
            jobtype = response.xpath('//td[starts-with(text(),"Job Type:")]/following-sibling::td[1]/text()').extract()[0]
            efiled = response.xpath('//td[starts-with(text(),"eFiled:")]/following-sibling::td[1]/text()').extract()
            if len(efiled) > 0:
                efiled = efiled[0]
            else:
                efiled = ''
            feeexempt = response.xpath('//td[starts-with(text(),"Fee Exempt:")]/following-sibling::td[1]/text()').extract()[0]
            filingtype = response.xpath('//td[starts-with(text(),"Filing Type:")]/following-sibling::td[1]/text()').extract()[0]
            daysbilled = int(response.xpath('//td[starts-with(text(),"Number of Days Billed")]/following-sibling::td[1]/text()').extract()[0])
            status = response.xpath('//td[starts-with(text(),"Status:")]/following-sibling::td[1]/text()').extract()[0]
            # entrydate = response.xpath('//td[starts-with(text(),"Entry Date:")]/following-sibling::td[1]/text()').extract()[0]
            # decisiondate = response.xpath('//td[starts-with(text(),"Decision Date:")]/following-sibling::td[1]/text()').extract()[0]
            totalfee = response.xpath('//td[starts-with(text(),"Total Fee:")]/following-sibling::td[1]/text()').extract()
            if len(totalfee) > 0:
                totalfee = totalfee[0]
            else:
                totalfee = ''

            #########################
            houseno = response.xpath('//td[starts-with(text(),"House No(s):")]/following-sibling::td[1]/text()').extract()[0]
            floors = response.xpath('//td[starts-with(text(),"Work on Floor(s):")]/following-sibling::td[1]/text()').extract()
            if len(floors) > 0:
                floors = floors[0]
            else:
                floors = ''
            streetname = response.xpath('//td[starts-with(text(),"Street Name:")]/following-sibling::td[1]/text()').extract()[0]
            # aptnos = response.xpath('//td[starts-with(text(),"Apt/Condo No(s):")]/following-sibling::td[1]/text()').extract()
            cbno = response.xpath('//td[starts-with(text(),"CB No:")]/following-sibling::td[1]/text()').extract()
            if len(cbno) > 0:
                cbno = cbno[0]
            else:
                cbno = ''

            #########################
            contractorname = response.xpath('//td[starts-with(text(),"Name:")]/following-sibling::td[1]/text()').extract()[0]
            contractorbusinessname = response.xpath('//td[starts-with(text(),"Business Name:")]/following-sibling::td[1]/text()').extract()[0]
            contractorbusinessaddr = response.xpath('//td[starts-with(text(),"Business Address:")]/following-sibling::td[1]/text()').extract()[0]
            # contractoremail = response.xpath('//td[starts-with(text(),"E-Mail:")]/following-sibling::td[1]/text()').extract()[0]
            contractorlicensetype = response.xpath('//td[starts-with(text(),"License Type:")]/following-sibling::td[1]/text()').extract()[0]
            contractorphone = response.xpath('//td[starts-with(text(),"Business Phone:")]/following-sibling::td[1]/text()').extract()[0]
            contractorlicense = response.xpath('//td[starts-with(text(),"License Number:")]/following-sibling::td[1]/a[1]/text()').extract()[0]

            #########################
            residencewithin200feet = response.xpath('//td[starts-with(text(),"Is a residence within 200 feet of the site?")]/following-sibling::td[1]/img[1]/@src').extract()[0]
            residencewithin200feet = True if residencewithin200feet=='images/box_check.gif' else False

            workwithineclosedbuilding = response.xpath('//td[starts-with(text(),"Is all work being done within an enclosed building?")]/following-sibling::td[1]/img[1]/@src').extract()[0]
            workwithineclosedbuilding = True if workwithineclosedbuilding=='images/box_check.gif' else False

            fullorpatialdemolition = response.xpath('//td[starts-with(text(),"Does any of the work involve full or partial demolition?")]/following-sibling::td[1]/img[1]/@src').extract()[0]
            fullorpatialdemolition = True if fullorpatialdemolition=='images/box_check.gif' else False

            craneuse = response.xpath('//td[starts-with(text(),"Does any of the work involve crane use?")]/following-sibling::td[1]/img[1]/@src').extract()[0]
            craneuse = True if craneuse=='images/box_check.gif' else False

            #########################
            daysrequested = int(response.xpath('//b[starts-with(text(),"Total Days Requested:")]/../text()').extract()[0])
            daysapproved = int(response.xpath('//b[starts-with(text(),"Total Days Approved:")]/../text()').extract()[0])
            approvedfor = ""
            for i in range(0, daysapproved):
                sibling = i + 1 # skip header
                startday = response.xpath('//td[contains(text(),"Start Day:")]/../following-sibling::tr['+str(sibling)+']/td[1]/text()').extract()
                days = response.xpath('//td[contains(text(),"Start Day:")]/../following-sibling::tr['+str(sibling)+']/td[2]/text()').extract()
                hoursfrom = response.xpath('//td[contains(text(),"Start Day:")]/../following-sibling::tr['+str(sibling)+']/td[3]/text()').extract()
                hoursto = response.xpath('//td[contains(text(),"Start Day:")]/../following-sibling::tr['+str(sibling)+']/td[4]/text()').extract()
                if len(startday) > 0 and len(days) > 0 and len(hoursfrom) > 0 and len(hoursto) > 0:
                    approvedfor += ("%s,%s,%s,%s;")%(startday[0],days[0],hoursfrom[0],hoursto[0])

            #########################
            applyreason = response.xpath('//b[starts-with(text(),"Apply Reason:")]/../text()').extract()[0].replace(u'\xa0', '').replace('\"', '\'')
            erenew = response.xpath('//b[starts-with(text(),"eRenew?")]/../text()').extract()[0].replace(u'\xa0', '').replace('\r','').replace('\n','').replace('\t','').replace('\"', '\'')
            approved = ''
            if daysapproved > 0:
                approved = response.xpath('//b[starts-with(text(),"Approved:")]/../text()').extract()[1].replace(u'\xa0', '').replace('\r','').replace('\n','').replace('\t','').replace('\"', '\'')
            erenewalauthorized = response.xpath('//b[starts-with(text(),"eRenewal Authorized Day(s)/Time(s):")]/../text()').extract()
            if len(erenewalauthorized) > 0:
                erenewalauthorized = erenewalauthorized[0].replace(u'\xa0', '').replace('\r','').replace('\n','').replace('\t','').replace('\"', '\'')
            else:
                erenewalauthorized = ''
            description = response.xpath('//td[starts-with(text(),"Description of Work:")]/following-sibling::td[1]/text()').extract()
            if len(description) > 0:
                description = description[0].replace(u'\xa0', '').replace('\r','').replace('\n','').replace('\t','').replace('\"', '\'')
            else:
                description = ''

            # Write to file
            outfile.write(("\"%s\",\"%s\",\"%s\",%s,%s,\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,\"%s\",\"%s\",\"%s\",%s,\"%s\",%s,%s,%s,%s,%s,%s,%s,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n") \
                            %(bbl,borough,block,lot,bin,premises,jobno,referenceno,permitno,jobtype,efiled,feeexempt,filingtype,daysbilled,status,totalfee,houseno,streetname,floors,cbno,\
                              contractorname, contractorbusinessname, contractorbusinessaddr, contractorlicensetype, contractorphone, contractorlicense, \
                              residencewithin200feet,workwithineclosedbuilding,fullorpatialdemolition,craneuse,daysrequested,daysapproved,approvedfor, erenew, \
                              applyreason,approved,erenewalauthorized,description))

    outfile.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse DOB AHV permits into a csv file.')

    parser.add_argument('-i', '--input', nargs='?', required=True,
                        help='Input html files. If using path with wildcards, make sure to use quotes.')

    parser.add_argument('-o', '--output', nargs='?', required=True,
                        help='Output file.')

    args = parser.parse_args()

    parse(args.input, args.output)

