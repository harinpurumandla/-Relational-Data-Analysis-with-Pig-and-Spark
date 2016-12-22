import sys
from pyspark.sql import *
spark = SparkSession\
 .builder\
 .appName("customername")\
 .getOrCreate() # creating the spark session
 
lines = spark.read.text("purchase").rdd.map(lambda r: r[0])
parts = lines.map(lambda l: l.split("\t"))
purchase = parts.map(lambda p:
Row(year=int(p[0]),cid=p[1],isbn=p[2],seller=p[3],price=int(p[4])))
purchaseTable = spark.createDataFrame(purchase)
purchaseTable.createOrReplaceTempView("purchase") 

lines = spark.read.text("book").rdd.map(lambda r: r[0])
parts = lines.map(lambda l: l.split("\t"))
book = parts.map(lambda p: Row(isbn=p[0],name=p[1]))
bookTable = spark.createDataFrame(book)
bookTable.createOrReplaceTempView("book")

lines = spark.read.text("customer").rdd.map(lambda r: r[0])
parts = lines.map(lambda l: l.split("\t"))
customer = parts.map(lambda p:
Row(cid=p[0],name=p[1],age=int(p[2]),address=p[3],sex=p[4]))
customerTable = spark.createDataFrame(customer)
customerTable.createOrReplaceTempView("customer")

customername = spark.sql("select name from customer where cid IN (select distinct(purch.cid) as cid from purchase as purch INNER JOIN (select pur.cid as cid,pur.isbn as isbn from purchase as pur INNER JOIN (select cid from customer where name like '%Harry%') as harry ON pur.cid=harry.cid) as common ON purch.isbn=common.isbn and purch.cid != common.cid)")
Names = customername.rdd.map(lambda p: p.name).collect()
f = open('customernames.txt','w') # Creating a ouput file named customernames in the current working directory
for name in Names:
	f.write(name+'\n') # writing the names to the file
f.close()
spark.stop()
