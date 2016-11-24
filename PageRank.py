#SREELAKSHMI SETTURU
#800934503
#ssetturu@uncc.edu
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkContext, SparkConf
import re
os.environ["SPARK_HOME"]= "/usr/lib/spark"
sc=SparkContext()
#path of input file given through command line arguments
file=sc.textFile(sys.argv[1])
#creating a list to store all titles to get the count of number of documents
doccount=[]
def titles(x):
    titlle=""
    title=re.search('<title>(.*?)</title>',x)
#condition to eliminate empty lines in the document
    if title:
        titlle=title.group(1)
        doccount.append(titlle)
    else:
        titlle="800934503"
        doccount.append(titlle)
    return doccount
lines=file.flatMap(lambda x:titles(x))
#filtering the empty lines. if a line is empty my 800 number is appended to the doccount list,
# so that they can be filtered and actual count of documents cn be obtained.
lines=lines.filter(lambda s:s!="800934503")
count=lines.distinct().count()
print "lines count ",count
#creating a link graph 
def linkggraph(z):
    titlle1=""
    title1=re.search('<title>(.*?)</title>',z)
    inipgr=1.0/count
    value=""
    text=re.search("<text xml(.*?)</text>",z)
#extracting pagerank and outlinks information only when title is present(eliminating empty lines)
    if title1:
#outlinks are present only inside <text xml> tag, other lables are ignored as mentioned in the assignment document.
        if text:
            titlle1=title1.group(1)
            txt=text.group(1)
            outlinks1 = re.findall("\\[\\[(.*?)\\]\\]", txt)
            for out1 in outlinks1:
                value=value+out1+"@#$@#$"
            if value=="":
               value = repr(inipgr) + "######" + "\t"
            else:
               value=repr(inipgr)+"######"+value
        else:
            value=repr(inipgr)+"######"+"\t"
    else:
        titlle1="800934503"
        value="800934503"
#returning title and initial pagerank+ outlinks information (outlinks are seperted by @#$@#$ )
    return titlle1,value
#filtering empty lines from link graph
linkgr=file.map(lambda z:linkggraph(z))
linkgr=linkgr.filter(lambda r:r[0]!="800934503")

#in this function i am seperating title , pagerank, oulinks from the output of linkgraph function.
def processing(r):
    proceskey=[]
    count=0
    procesvalue=[]
    strs=""
    key1=r[0]
    value1=r[1]
    val1=value1.split('######')[1]
    pgr=float(value1.split('######')[0])
    contribution=value1.count('@#$@#$')
    if contribution is not 0:
       pgrk = pgr /contribution
    else:
       pgrk=pgr
    outlink2=val1.split('@#$@#$')
    for o in outlink2:
        count+=1
        if count<=contribution:
            proceskey.append(o)
            procesvalue.append(pgrk)
            strs=strs+o+"#$%#$%"
    proceskey.append(key1)
    #if there are no utlinks, passing tab instead of null value
    if strs=="":
        procesvalue.append("\t")
    else:
        procesvalue.append(strs)
#zip pairs keys and values from list of keys and list of values
    ppp=zip(proceskey,procesvalue)
    return ppp

#all the values of a key are grouped together and are passed as argument of this function
#in this function, all the page ranks contributed from its inlinks are summed up and outlinks information is appended to the pagerank to recreate the link graph
#which can be used for 10 iterations.
def sumrank(x):
    key2=x[0]
    oulnks=""
    pg=0.0
    i=0
    iterater=x[1]
    for it in iterater:
        if type(it)==float:
           pg = pg + it
        else:
             outl=it.split('#$%#$%')
             con = it.count('#$%#$%')
             for l in outl:
                 i+=1
                 if i<=con:
                   oulnks=oulnks+l+"@#$@#$"
    pg=(1 - 0.85) + 0.85 * pg
    if oulnks=="":
       val=str(pg)+"######"+"\t"
       val=val.encode('ascii', 'ignore').decode('ascii')
    else:
        val = str(pg) + "######" + oulnks
        val = val.encode('ascii', 'ignore').decode('ascii')
    return  key2,val
#seperating title and pagerank, returning them for sorting based on pagerank.
def ascsort(x):
    ttl=x[0]
    pgrank=x[1].split("######")
    pgg=float(pgrank[0])
    return ttl,pgg

#####----------------------------
#10 iterations to compute the pagerank
for i in range(10):
    print "iteration", i
    linkgr=linkgr.flatMap(lambda r:processing(r)).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x: sumrank(x))
#filtering the rdd inorder to output pagerank of titles instead of pagerank of outlinks.
linkgr=linkgr.filter(lambda b:not b[1].endswith("######"))
sort=linkgr.map(lambda x:ascsort(x))
#sorting in descending order based on pagerank
sort=sort.sortBy(ascending=False,keyfunc=lambda y:y[1])
#writing the output to a text file, path taken from command line arguments.
sort.saveAsTextFile(sys.argv[2])


