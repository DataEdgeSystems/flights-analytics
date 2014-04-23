#Realtime analytics with Aerospike

##Problem
You want to perform analytics on your BigData stored in Aerospike, and Hadoop is too slow to meet the business requirements
##Solution
Use an Aerospike Aggregation on data streaming from the output of a query. Aggregations are a union of a query on a secondary index and one or more StreamUDFs.

###Getting the code
The source code for this solution is available on GitHub, at https://github.com/helipilot50/flights-analytics.git

Clone the GitHub repository using the following command:
```
git clone https://github.com/helipilot50/flights-analytics.git
```

###Build instructions
The utility requires a working Java development environment (Java 6 and above) including Maven (Maven 2). The Aerospike Java client will be downloaded from Maven Central as part of the build.

After cloning the repository, use maven to build the jar files. From the root directory of the project, issue the following command:
```
mvn clean package
```
A JAR file will be produced in the directory 'target', ```flights-analytics-1.0.0-jar-with-dependencies.jar```

###Test data
The data is flight records of commercial airline flights in the USA, for the month of January 2012. There are approximately 1,050,000 records in the data set.

You load the test data by running the jar with the ```-f``` option, specifying the data directory. This will load the data from the CSV files supplied in the 'data' directory.
```
java -jar flights-analytics-1.0.0-jar-with-dependencies.jar -h <host name> -f data
```
###Running the solution
This is a runnable jar complete with all the dependencies packaged.
You can run this jar with the following command:
```
java -jar flights-analytics-1.0.0-jar-with-dependencies.jar -h <host name>
```
####Options
```
-f,--file <arg>       Data file (default: data/flights_from.csv)
-h,--host <arg>       Server hostname (default: localhost)
-n,--namespace <arg>  Namespace (default: test)
-p,--port <arg>       Server port (default: 3000)
-u,--usage            Print usage.
```

####Output


##Discussion 

###Record Structure

The Key
namespace, FLIGHTS,String

Bin name | Type
-------- | ----
YEAR | Integer	
DAY_OF_MONTH | Integer
FL_DATE_BIN | TimeStamp
AIRLINE_ID | Integer	
CARRIER | String
FL_NUM | Integer
ORI\_AIRPORT\_ID | Integer
ORIGIN | String
ORI_CITY_NAME | String
ORI_STATE_ABR | String
DEST | String
DEST_CITY_NAME | String
DEST_STATE_ABR | String
DEP_TIME | Integer							new Bin("ARR_TIME", Integer.parseInt(flight[15].trim())),
							new Bin("ELAPSED_TIME", Integer.parseInt(flight[16].trim())),
							new Bin("AIR_TIME", Integer.parseInt(flight[17].trim())),
							new Bin("DISTANCE", Integer.parseInt(flight[18].trim()))

####Conversion to time stamp

###Secondary index

###Range Query

###Aggregation using StreamUDF
We use the StreamUDF capability of Aerospike to process the stream of records that are the output of the range query. This may seem a little daunting, but not really, consider this diagram:

![Stream Processing](query_stream_filter.png)

Why StreamUDFs at all, you could do all this processing in your application? Beside the obvious opportunity for function re-use UDFs, and in our example, StreamUDFs allow us to push the processing to the same node in the cluster that stores the data. The processing is close to the data and done in parallel. The cost of shipping ~1,000,000 records to the application is reduced to shipping just the results. 


StreamUDFs are written in Lua. So why Lua? It is a very fast interpreter and has a very small footprint. 

The aggregation function is executed on each node in the cluster, once for each record produced by the query. In our example, the aggregation function will be invoked over 1,000,000 times. If the cluster has 2 nodes, about 500,000 invocations on each node, if the cluster had 5 nodes, about 100,000 invocations on each node. The more nodes, the more parallelism. You get the idea.

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

```lua
local function reduce_values(a, b)
  return map.merge(a, b, flightsMerge)
  --return a
end

```
 
####Configuration function: late\_flights\_by\_airline()
This function configures the stream processing.

