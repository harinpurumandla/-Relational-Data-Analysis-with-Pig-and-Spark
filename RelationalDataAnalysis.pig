PURCHASE = load './purchase' using PigStorage('\t') as ( year:int, cid:chararray, isbn:chararray, seller:chararray, price:int ); -- Loading the purchase table with appropriate datatypes.
TOTAL = FOREACH (GROUP PURCHASE BY seller) GENERATE group,SUM(PURCHASE.price); -- selecting the complete amount earned by each seller from purchase table.
store TOTAL into './total'; -- Storing the output into a file.

book = load './book' using PigStorage('\t') as ( isbn:chararray, name:chararray ); --Loading the book table with appropriate datatypes.
minprice = FOREACH (GROUP purchase BY isbn) GENERATE group,MIN(purchase.price) as price; -- Selecting the minimum price of each distint ISBN by using group by method on purchase
joined = join purchase by isbn,minprice by group; -- joining the above output to the purchase table by grouping it based on ISBN of both tables.
filtered = FOREACH (FILTER joined by purchase::seller == 'Amazon' and purchase::price == minprice::price) generate purchase::isbn; -- Filtering the above joined table based on the seller name = Amazon and price of minimum ISBN = price of purchase table and returing the corresponding ISBN
booknames = FOREACH (JOIN book BY isbn,filtered by purchase::isbn) GENERATE book::name; -- Finding the name of the ISBN's returned by the above operation
store booknames into './booknames'; -- Storing the output into a file.

customer = load './customer' using PigStorage('\t') as ( cid:chararray, name:chararray, age:int, address:chararray, sex:chararray ); -- Loading the customer table with appropriate datatypes.
harryid= FOREACH (FILTER customer BY name MATCHES '.*Harry.*') GENERATE cid; -- Selecting the CID of the customer whose name has Harry
isbn = FOREACH ( FILTER (JOIN purchase by cid, harryid by cid) by purchase::cid == harryid::cid) GENERATE harryid::cid as cid,purchase::isbn as isbn; -- Joining the above output with the purchase table by comparing the CID's from purchase table and the above output and returning the ISBN and Harrie's CID.
customerid = DISTINCT(FOREACH( FILTER(JOIN purchase by isbn, isbn by isbn) by purchase::cid != isbn::cid) GENERATE purchase::cid as cid); -- Finding the distinct customer Id who has bought all the books that harry has bought, this is done by using inner join over purchase table and the above output.
customername = FOREACH( FILTER( JOIN customer by cid, customerid by cid) by customer::cid == customerid::cid) GENERATE customer::name; -- Finding the customer name from the above result.
store customername into './customername'; -- Storing the output into a file