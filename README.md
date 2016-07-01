# MapReduceJoin
This serves to explain reducer and mapper side joins.


The project is a maven based project.

It uses the new Hadoop API.



The project can be run in eclipse.

In a new Run Configuration, mention the main class which in our case is hadoopjoinexample.Driver
In arguments tab, specify the program arguments as below
SalesOrderDetail.csv Product.csv ProductSubCategory.csv output


To run the program in a cluster.
Copy the csv files to hdfs and then run the program as below from the command line
hadoop jar nameofjarfile.jar hadoopjoinexample.Driver /pathto/SalesOrderDetail.csv /pathto/Product.csv  /pathto/ProductSubCategory.csv  /pathto/outputdirectoryname
