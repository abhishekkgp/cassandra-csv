package br.com.agcorrea.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.types.StructType;

/**
 * Utility class that imports a csv and save to cassandra.
 * Accept options based on spark-csv package from DataBricks https://github.com/databricks/spark-csv
 **/
public class ImportCsv {

    private final CassandraSQLContext csc;

    private String csvPath;

    private StructType schema;

    private String[] selectExpr;

    private Map<String, String> options = new HashMap<>();

    public ImportCsv(CassandraSQLContext csc) {
        this.csc = csc;
    }

    /**
     * Start new importCsv with provided cassandra context
     *
     * @param csc cassandra spark context
     *
     * @return new ImportCsv
     */
    public static ImportCsv start(CassandraSQLContext csc) {
        return new ImportCsv(csc);
    }

    /**
     * Set the path of csv file to be imported
     *
     * @param csvPath path to file
     *
     * @return same instance for chained calls
     */
    public ImportCsv read(String csvPath) {
        this.csvPath = csvPath;
        return this;
    }

    /**
     * Set the schema with column names and types
     *
     * @param schema csv schema
     *
     * @return same instance for chained calls
     */
    public ImportCsv schema(StructType schema) {
        this.schema = schema;
        return this;
    }

    /**
     * Select expression to be applied on csv data frame.
     * That's not required. If not set, all columns from schema will be saved.
     *
     * @param selectExpr same selectExpr from spark sql
     *
     * @return same instance for chained calls
     */
    public ImportCsv selectExpr(String... selectExpr) {
        this.selectExpr = selectExpr;
        return this;
    }

    /**
     * Pass a map with all options.
     * For all supported options, look at https://github.com/databricks/spark-csv
     */
    public ImportCsv options(Map<String, String> options) {
        this.options.putAll(options);
        return this;
    }

    /**
     * Set option one by one.
     * For all supported options, look at https://github.com/databricks/spark-csv
     */
    public ImportCsv option(String option, String value) {
        this.options.put(option, value);
        return this;
    }

    /**
     * Save csv dataframe to cassandra on given "keyspace.table".
     * Records are appended to existent table and data.
     *
     * @param keyspace cassandra keyspace
     * @param table cassandra table
     */
    public void saveToCassandra(String keyspace, String table) {
        getCsvDataFrame()
                .write()
                .format("org.apache.spark.sql.cassandra")
                .option("table", table)
                .option("keyspace", keyspace)
                .mode(SaveMode.Append)
                .save();
    }

    private DataFrame getCsvDataFrame() {
        DataFrame csv = csc.read()
                .format("com.databricks.spark.csv")
                .schema(schema)
                .options(options)
                .load(csvPath);

        if (selectExpr != null && selectExpr.length > 0) {
            return csv.selectExpr(selectExpr);
        }

        return csv;
    }
}
