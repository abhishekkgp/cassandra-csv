# Cassandra CSV
Simple utility java class to import data from a csv file directly to Cassandra using Spark SQL

## Usage
    ImportCsv.start(csc)
            // csv path
            .read("cassandra-csv/src/main/resources/example.csv")

            // csv structure
            .schema(schema)

            // options from spark-csv package. see https://github.com/databricks/spark-csv
            .option("header", "true")

            // select expression to be applied on csv data frame
            .selectExpr("id", "type AS transaction_type", "value")

            // keyspace and table name
            .saveToCassandra("my_keyspace", "my_table");
            
Based on package https://github.com/databricks/spark-csv.
