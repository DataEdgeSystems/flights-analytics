#Realtime analytics with Aerospike

##Problem
You want to perform analytics on your BigData stored in Aerospike, and Hadoop is not fast enough to meet the business requirements
##Solution
Use an Aerospike Aggregation on data streaming from the output of a query. Aggregations are the union of a query on a secondary index and one or more StreamUDFs.

###Getting the code
The source code for this solution is available on GitHub, at https://github.com/aerospike/flights-analytics.git

Clone the GitHub repository using the following command:
```
git clone https://github.com/aerospike/flights-analytics.git
```

###Build instructions
This example requires a working Java development environment (Java 6 and above) including Maven (Maven 2). The Aerospike Java client will be downloaded from Maven Central as part of the build.

After cloning the repository, use maven to build the jar files. From the root directory of the project, issue the following command:
```
mvn clean package
```
A JAR file will be produced in the directory 'target', ```flights-analytics-1.0.0-full.jar```

###Creating a secondary index
Create a secondary index on the Bin ```FL_DATE_BIN``` using the AQL utility. At the prompt type the following command:
```sql
CREATE INDEX FL_DATE on test.flights (FL_DATE_BIN) NUMERIC
```
The AQL is also in the file ```aql/CreateIndex.aql``` 
###Test data
The data is flight records of commercial airline flights in the USA, for the month of January 2012. There are approximately 1,050,000 records in the data set. This data set is divided into several files that are 10,000 records each.

You load the test data by running the jar with the ```-f``` option, specifying the 'data' directory. This will load the data from the CSV files supplied in the 'data' directory.
```
java -jar flights-analytics-1.0.0-full.jar -h <host name> -f <project root>/data
```
Data can also be loaded using the [Aerospike Loader](https://github.com/aerospike/aerospike-loader). This a multi-threaded CSV loader that takes full advantage of the hardware capabilities to load data rapidly.

[Aerospike Loader](https://github.com/aerospike/aerospike-loader) uses a JSON formated configuration file to map each column to a Bin name and type. The configuration file ```flights_from.json``` in the project root directory.

To load the data using [Aerospike Loader](https://github.com/aerospike/aerospike-loader), use this command:
```bash
./run_loader -h localhost -c <project root>/flights_from.json <project root>/data
``` 
###Running the solution
This is a runnable jar complete with all the dependencies packaged.
You can run this jar with the following command:
```
java -jar flights-analytics-1.0.0-full.jar -h <host name>
```
This program will load a User Defined Function (UDF) module when it starts. It will look for the UDF module at this location ```udf/simple_aggregation.lua``. Be sure you place it there.

####Options
```
-f,--file <arg>       Data file (default: data), only use to load data
-h,--host <arg>       Server hostname (default: localhost)
-n,--namespace <arg>  Namespace (default: test)
-p,--port <arg>       Server port (default: 3000)
-u,--usage            Print usage.
```

####Output

The output is controlled by a Log4j profile files located at ```src/log4j.properties```.

The first few lines are simple diagnostics that let you know whats happening.
```
2632 INFO  FlightsAnalytics  - registered UDF
2639 INFO  FlightsAnalytics  - built query
2650 INFO  FlightsAnalytics  - executed aggregation
2650 INFO  FlightsAnalytics  - Airlines with late flights:
```
This is the actual output of the analytics. There are 4 columns:
* Airline code
* Total number of flights
* Number of late flights
* Percentage of late flights

```
7510 INFO  FlightsAnalytics  - AS:  33084   6009  18%
7510 INFO  FlightsAnalytics  - US:  66354  14390  21%
7510 INFO  FlightsAnalytics  - B6:  51987   8850  17%
7510 INFO  FlightsAnalytics  - HA:  11352    220   1%
7511 INFO  FlightsAnalytics  - F9:  12556   5744  45%
7511 INFO  FlightsAnalytics  - EV: 111534  24324  21%
7511 INFO  FlightsAnalytics  - MQ:  71418  16492  23%
7511 INFO  FlightsAnalytics  - OO:  94414  19662  20%
7511 INFO  FlightsAnalytics  - WN: 175788  41660  23%
7511 INFO  FlightsAnalytics  - DL: 110654  29018  26%
7512 INFO  FlightsAnalytics  - UA:  80836  29430  36%
7512 INFO  FlightsAnalytics  - AA: 126926  40902  32%
7512 INFO  FlightsAnalytics  - YV:  23148   4900  21%
7512 INFO  FlightsAnalytics  - FL:  37186   6630  17%
7512 INFO  FlightsAnalytics  - VX:   7948   3070  38%
7512 INFO  FlightsAnalytics  - Time: 485
```

##Discussion 

<ToDO>

###Record Structure
The Key consists of the ```namespace``` from the ```-n``` option (default: test), the set is ```flights```, and the actual key is a String containing a unique number.

The following table describes each Bin:

Bin name | Type
-------- | ----
YEAR | Integer	
DAY\_OF\_MONTH | Integer
FL\_DATE\_BIN | TimeStamp
AIRLINE_ID | Integer	
CARRIER | String
FL_NUM | Integer
ORI\_AIRPORT\_ID | Integer
ORIGIN | String
ORI\_CITY\_NAME | String
ORI\_STATE\_ABR | String
DEST | String
DEST\_CITY\_NAME | String
DEST\_STATE\_ABR | String
DEP_TIME | Integer							
ARR_TIME | Integer
ELAPSED_TIME | Integer
AIR_TIME | Integer
DISTANCE | Integer

####Conversion to time stamp
Aerospike has no native TimeStamp type, but it is easily implemented. In this example, the TimeStamp value is ```Long``` containing the number of seconds since January 1, 1970, 00:00:00 GMT.

The Java [SimpleDateFormat](http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html) class is used to convert the string "2012/01/15" to a [Date](http://docs.oracle.com/javase/6/docs/api/java/util/Date.html) object. The method [getTime()](http://docs.oracle.com/javase/6/docs/api/java/util/Date.html#getTime() is called to get the number of milliseconds and this is divided by 1000.

```java
/*
 * create time stamps for query from
 * the start date and end date
 */
SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
Date startDate = sdf.parse("2012-01-01");
long startTimeStamp = startDate.getTime() / 1000;
Date endDate = sdf.parse("2012-01-31");
long endTimeStamp = endDate.getTime() / 1000;
```
This is a slight variation from the algorithm used by [Aerospike Loader](https://github.com/aerospike/aerospike-loader) in that the time zone offset is not factored into the conversion from Date to Seconds.

###Range Query
The foundation building block of this aggregation is a secondary index query. This a range query on the TimeStamp in FL\_DATE\_BIN.
```java
/*
 * build the query
 */
Statement stmt = new Statement();
stmt.setNamespace(this.namespace);
stmt.setSetName(FLIGHTS);
stmt.setFilters(Filter.range(FL_DATE_BIN, startTimeStamp, endTimeStamp));
log.info("built query");
```
In the ```Statement``` object, we prepare the query by setting the Namespace, the Set and the range filter ```Filter.range(FL_DATE_BIN, <start date>, <end date>)```. This statement is then prepared and can be used in a query or aggregation.

The aggregation is executed with the following code. The output of the aggregation is returned as a Map with the line ```map = (Map) rs.getObject();```. This is a Java [Map](http://docs.oracle.com/javase/7/docs/api/java/util/Map.html) containing the list of Airlines with late flights, keyed by Airline ID. 
```java
// Execute the query
ResultSet rs = client.queryAggregate(null, stmt, "simple_aggregation", "late_flights_by_airline");
log.info("executed aggregation");
if (rs != null){
	Map map = null;
	log.info("Airlines with late flights:");
	long  start = System.nanoTime();
	while (rs.next()){
		map = (Map) rs.getObject();
	}
	long stop = System.nanoTime();
	printMap(map);
	log.info("Time: " + (stop-start)/1000000);
} else {
	log.info("Nothing returned");
}
```

###Aggregation using StreamUDF
We use the StreamUDF capability of Aerospike to process the stream of records that are the output of the range query. This may seem a little daunting, but not really, consider this diagram:

![Stream Processing](query_stream_filter.png)

Why StreamUDFs at all, you could do all this processing in your application? 

Beside the obvious opportunity for function re-use StreamUDF functions in other aggregations, UDFs allow us to push the processing to the same node in the cluster that stores the data. The processing is close to the data and done in parallel. The cost of shipping ~1,000,000 records to the application is reduced to shipping just the results. 


StreamUDFs are written in Lua and located in the file ```udf/simple_aggregation.lua```. So why Lua? It is a very fast interpreter and has a very small footprint. 

The ```aggregation``` function is executed on each node in the cluster, once for each record produced by the query. In our example, the aggregation function will be invoked over 1,000,000 times. If the cluster has 2 nodes, about 500,000 invocations on each node, if the cluster had 5 nodes, about 100,000 invocations on each node. The more nodes, the more parallelism. You get the idea.

The ```reduce``` function is run on the calling client. It collates the results from each node.

####Aggregation function: add_values()
This function is where the meat of the work is done. The accumulator structure, in this case a map, is the first parameter, and the record is the second parameter.

The ```CARRIER``` Bin value is used as a key to the map ```airlineMap```. If an entry for the carrier does not exist, it is added to the map, the value of ```flights``` for the carrier is incremented by 1, if the flight is late, the value of ```late``` is incremented by 1.

The map ```airlineMap``` is the return value, and will be used either by the next aggregation function invocation OR the reduce function if there are no more records in the query output.

```lua
local function add_values(airlineMap, nextFlight)
  local carrier = nextFlight["CARRIER"]
  local airline = airlineMap[carrier] 
  if airline == null then
    airline = map {flights = 0, late = 0}
  end
  airline.flights = airline.flights + 1
  -- if this flight is late, increment the late count in airline
  if toMinutes(nextFlight.ELAPSED_TIME) > (toMinutes(nextFlight.ARR_TIME) - toMinutes(nextFlight.DEP_TIME)) then
    airline.late = airline.late + 1
  end
  -- put the airline into the airlineMap
  airlineMap[carrier] = airline
  return airlineMap
end

```


####Reduce function: reduce_values()
This function runs in a Lua interpreter on the client. It merges the maps produced by each node on the cluster. 
```lua
local function reduce_values(a, b)
  return map.merge(a, b, flightsMerge)
  --return a
end
```

The ```flightsMerge``` function is a custom function to merge the maps produced on each node. It also calculates the percentage of late flights.
```lua
local function flightsMerge(a, b)
  a.flights = a.flights + b.flights
  a.late = a.late + b.late
  a.percent = a.late / a.flights * 100 
  return a
end
```
 
####Configuration function: late\_flights\_by\_airline()
This function configures the stream processing.
```lua
function late_flights_by_airline(stream)

  return stream : aggregate(map(), add_values) 
    : reduce(reduce_values)

end
```
the ```aggregate``` function is passed a new ```map``` and a function reference to ```add_values```.

The ```reduce``` function is passed a function reference to ```reduce_values```.

The stream is processed from left to right in the code

```lua
  return stream : aggregate(map(), add_values) 
    : reduce(reduce_values)
```
