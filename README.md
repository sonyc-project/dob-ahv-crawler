# dob-ahv-crawler

Crawler to extract and parse NYC Department of Buildings After Hour Variances (AHV).

After Hour Variances are permits issued by New York City Department of Buildings allowing construction activity before 7:00 AM, after 6:00 PM, or on the weekend. A list of issued permits are only available through [DOB Buildings Information System](http://a810-bisweb.nyc.gov/bisweb/bsqpm01.jsp) (BIS), and not through any open data portal. The **dob-ahv-crawler** crawls through the BIS, parsing the HTML files and outputting the AHV in a CSV file.

## Dependencies

Before running the crawler, you need to install the following libraries for python3:

```
python -m pip install scrapy
```

## Steps

1. Create a CSV file with a lists of BBL's (building-block-lot) to crawl for AHV's. Here are the first few lines of an example file:

   ```
   1004730029
   1015230011
   1005260075
   1017250040
   1014937506
   1015280014
   1022300011
   1020110001
   ```

   You can also use this CSV file with the list of all BBL's in NYC. Another option is to use [this](http://chriswhong.github.io/plutoplus/) tool to filter for specific BBL's.

2. Run the actual crawler:

   ```
   python crawl_ahv_data.py --input bbls.csv --output html_files/
   ```

   Where `--input` is the CSV  file from the previous step and `--output` is the folder that will store the crawled HTML files. You can also specify a list of borough numbers to consider with `--boroughs`, i.e. `--boroughs 1 3` will only consider AHV in Manhattan and Brooklyn.

   The borough numbers are:

   ```
   1: Manhattan
   2: Bronx
   3: Brooklyn
   4: Queens
   5: Staten Island
   ```

3. Once the html files are downloaded, run the following script to parse them and output a CSV file:

   ```
   python get_ahv_data.py --input "/home/fmiranda/scrapy/html-files-all/*/ahv-*.html" --output ../../data/ahv/ahv-manhattan2.csv
   ```

   Where `--input` is a wildcard containing all the HTML files to parse, and `--output` is the final parsed csv file.

## Result

The final result should be a CSV file with the after hour variances for the BBL's specified in the input CSV. Here are the first few lines as an example:

```
bbl,borough,block,lot,bin,premises,jobno,referenceno,permitno,jobtype,efiled,feeexempt,filingtype,daysbilled,status,totalfee,houseno,streetname,floors,cbno,contractorname,contractorbusinessname,contractorbusinessaddr,contractorlicensetype,contractorphone,contractorlicense,residencewithin200feet,workwithinenclosedbuilding,fullorpatialdemolition,craneuse,daysrequested,daysapproved,approvedfor,erenew,applyreason,approved,erenewalauthorized,description
1013690030,MANHATTAN,01369,0030,1040546,"440 EAST   58 STREET MANHATTAN",122011287,00592905,122011287,A2,Yes,No,Initial,2,AHV SUCCESSFULLY ISSUED,"$260.00","440","EAST 58TH STREET","OSP",106,"ALAM  MOHAMMED","DAFFODIL GENERAL CONTRACT","1039A PACIFIC STREET  BROOKLYN NY 11238",GC,"718-398-7399",010244,True,False,False,False,2,2,"06/20/2015,Saturday,9:00 AM,5:00 PM;06/21/2015,Sunday,9:00 AM,5:00 PM;",Yes,"CONSTRUCTION ACTIVITIES WITH MINIMAL NOISE IMPACT",E-FILED AHV: AUTO-APPROVAL,"Saturday 9:00 AM to 5:00 PM, Sunday 9:00 AM to 5:00 PM","BROWNSTONE FACADE RESTORATION OF FRONT FACADE."
```

