%scala
// Read from elasticsearch into data frame
val esURL = "https://ebd-practice-module.es.ap-southeast-1.aws.found.io:9243/"
val username = ""
val password = ""
val reader = spark.read
  .format("org.elasticsearch.spark.sql")
  .option("es.nodes.wan.only","true")
  .option("es.port","443")
  .option("es.net.ssl","true")
  .option("es.net.http.auth.user",username)
  .option("es.net.http.auth.pass",password)
  .option("es.nodes", esURL)

val df = reader.load("carparkinformation*")
// display(df)
df.cache()
df.count()


%scala
// Read from elasticsearch (By DSL query) into data frame, and index dataframe back to elasticsearch in another index
val query = """{
  "query": {
    "range": {
      "update_datetime": {
        "gte": "now-1h"
      }
    }
  }
}"""

val reader2 = spark.read
  .format("org.elasticsearch.spark.sql")
  .option("es.nodes.wan.only","true")
  .option("es.port","443")
  .option("es.net.ssl","true")
  .option("es.net.http.auth.user",username)
  .option("es.net.http.auth.pass",password)
  .option("es.nodes", esURL)
  .option("es.resource", "carparkinformation*")
  .option("es.query", query)

val df2 = reader2.load("carparkinformation*")
// display(df)
// df2.cache()
df2.count()

df2.write
  .format("org.elasticsearch.spark.sql")
  .option("es.nodes.wan.only","true")
  .option("es.port","443")
  .option("es.net.ssl","true")
  .option("es.net.http.auth.user",username)
  .option("es.net.http.auth.pass",password)
  .option("es.nodes", esURL)
  .mode("Overwrite")
  .save("test-carparkinformation-2020-03-23")

%scala
// Read from CSV into data frame, and index dataframe in elasticsearch
import org.apache.spark.sql.functions.current_timestamp 
val esURL = "https://ebd-practice-module.es.ap-southeast-1.aws.found.io:9243/"
val username = "ebd-test-account"
val password = "e0938888"
val dfsave = spark.read.option("header","true").csv("/FileStore/tables/result.csv")
   
dfsave.withColumn("update_datetime", current_timestamp())

dfsave.write
  .format("org.elasticsearch.spark.sql")
  .option("es.nodes.wan.only","true")
  .option("es.port","443")
  .option("es.net.ssl","true")
  .option("es.net.http.auth.user",username)
  .option("es.net.http.auth.pass",password)
  .option("es.nodes", esURL)
  .mode("Overwrite")
  .save("test-carparkinformation-2020-03-23")
