# Spark+DynamoDB
Plug-and-play implementation of an Apache Spark custom data source for AWS DynamoDB.

We published a small article about the project, check it out here:
https://www.audienceproject.com/blog/tech/sparkdynamodb-using-aws-dynamodb-data-source-apache-spark/

## Features

- Distributed, parallel scan with lazy evaluation
- Throughput control by rate limiting on target fraction of provisioned table/index capacity
- Schema discovery to suit your needs
  - Dynamic inference
  - Static analysis of case class
- Column and filter pushdown
- Global secondary index support
- Write support

## Quick Start Guide

### Scala
```scala
import com.audienceproject.spark.dynamodb.implicits._

// Load a DataFrame from a Dynamo table. Only incurs the cost of a single scan for schema inference.
val dynamoDf = spark.read.dynamodb("SomeTableName") // <-- DataFrame of Row objects with inferred schema.

// Scan the table for the first 100 items (the order is arbitrary) and print them.
dynamoDf.show(100)

// write to some other table overwriting existing item with same keys
dynamoDf.write.dynamodb("SomeOtherTable")

// Case class representing the items in our table.
import com.audienceproject.spark.dynamodb.attribute
case class Vegetable (name: String, color: String, @attribute("weight_kg") weightKg: Double)

// Load a Dataset[Vegetable]. Notice the @attribute annotation on the case class - we imagine the weight attribute is named with an underscore in DynamoDB.
import org.apache.spark.sql.functions._
import spark.implicits._
val vegetableDs = spark.read.dynamodbAs[Vegetable]("VegeTable")
val avgWeightByColor = vegetableDs.agg($"color", avg($"weightKg")) // The column is called 'weightKg' in the Dataset.
```

### Python
```python
# Load a DataFrame from a Dynamo table. Only incurs the cost of a single scan for schema inference.
dynamoDf = spark.read.format("com.audienceproject.spark.dynamodb") \
                     .option("tableName", "SomeTableName") \ 
                     .load() # <-- DataFrame of Row objects with inferred schema.

# Scan the table for the first 100 items (the order is arbitrary) and print them.
dynamoDf.show(100)

# write to some other table overwriting existing item with same keys
dynamoDf.write.format("com.audienceproject.spark.dynamodb") \
              .option("tableName", "SomeOtherTable")
```

*Note:* When running from `pyspark` shell, you can add the library as:
```bash
pyspark --packages com.audienceproject:spark-dynamodb_<spark-scala-version>:<version>
```


## Getting The Dependency

The library is available from [Maven Central](https://mvnrepository.com/artifact/com.audienceproject/spark-dynamodb). Add the dependency in SBT as ```"com.audienceproject" %% "spark-dynamodb" % "latest"```

Spark is used in the library as a "provided" dependency, which means Spark has to be installed separately on the container where the application is running, such as is the case on AWS EMR.

## Parameters
The following parameters can be set as options on the Spark reader and writer object before loading/saving.
- `region` sets the region where the dynamodb table. Default is environment specific.
- `roleArn` sets an IAM role to assume. This allows for access to a DynamoDB in a different account than the Spark cluster. Defaults to the standard role configuration.


The following parameters can be set as options on the Spark reader object before loading.

- `readPartitions` number of partitions to split the initial RDD when loading the data into Spark. Corresponds 1-to-1 with total number of segments in the DynamoDB parallel scan used to load the data. Defaults to `sparkContext.defaultParallelism`
- `targetCapacity` fraction of provisioned read capacity on the table (or index) to consume for reading. Default 1 (i.e. 100% capacity).
- `stronglyConsistentReads` whether or not to use strongly consistent reads. Default false.
- `bytesPerRCU` number of bytes that can be read per second with a single Read Capacity Unit. Default 4000 (4 KB). This value is multiplied by two when `stronglyConsistentReads=false`
- `filterPushdown` whether or not to use filter pushdown to DynamoDB on scan requests. Default true.
- `throughput` the desired read throughput to use. It overwrites any calculation used by the package. It is intended to be used with tables that are on-demand. Defaults to 100 for on-demand.

The following parameters can be set as options on the Spark writer object before saving.

- `writeBatchSize` number of items to send per call to DynamoDB BatchWriteItem. Default 25.
- `targetCapacity` fraction of provisioned write capacity on the table to consume for writing or updating. Default 1 (i.e. 100% capacity).
- `update` if true items will be written using UpdateItem on keys rather than BatchWriteItem. Default false.
- `throughput` the desired write throughput to use. It overwrites any calculation used by the package. It is intended to be used with tables that are on-demand. Defaults to 100 for on-demand.

## Running Unit Tests
The unit tests are dependent on the AWS DynamoDBLocal client, which in turn is dependent on [sqlite4java](https://bitbucket.org/almworks/sqlite4java/src/master/). I had some problems running this on OSX, so I had to put the library directly in the /lib folder, as graciously explained in [this Stack Overflow answer](https://stackoverflow.com/questions/34137043/amazon-dynamodb-local-unknown-error-exception-or-failure/35353377#35353377).

In order to run the tests, make sure to put the following as additional VM parameters:

```-Djava.library.path=./lib/sqlite4java -Daws.dynamodb.endpoint=http://localhost:8000```

## Acknowledgements
Usage of parallel scan and rate limiter inspired by work in https://github.com/traviscrawford/spark-dynamodb
