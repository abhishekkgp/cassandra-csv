package br.com.agcorrea.cassandra;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MainApplication {

    public static void main(String[] args) {
        CassandraSQLContext csc = getCassandraSQLContext(); // provide cassandra sql spark context

        // Declare same data types and columns from your csv
        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("type", DataTypes.StringType, true, Metadata.empty()),
                new StructField("processed", DataTypes.BooleanType, true, Metadata.empty()),
                new StructField("value", DataTypes.DoubleType, true, Metadata.empty())
        });

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
    }

    /**
     * This config only create a local spark for tests purposes.
     */
    private static CassandraSQLContext getCassandraSQLContext() {
            SparkConf sparkConf = new SparkConf(true);
            sparkConf.setMaster("local");
            sparkConf.setAppName("Cassandra CSV Import Application");
            sparkConf.set("spark.cassandra.connection.host", "localhost");
        return new CassandraSQLContext(new SparkContext(sparkConf));
    }

}
