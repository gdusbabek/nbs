package gdusbabek.nbs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.sum;

/**
 * Compute the top 10 pages by language for all languages.
 *
 * Inputs are:
 * args[0] - parent directory to parquet files that will be aggregated. Granularity is determined by the directory
 *           passed in. For now, that would be per hour, per day, per month and per year and per all-time. There is no
 *           way to aggregate hours 01 and 02.
 * args[1] - directory to write the final output to. Since outputs will always be relatively small, we coalesce to a
 *           single file.
 */
public class ComputeTop10 {
    public static void main(String args[]) {

        String sourceDirPath = args[0];
        String outputDirPath = args[1];

        // get a spark session. local for now.
        SparkSession session = SparkSession
                .builder()
//                .config("spark.shuffle.file.buffer", "64k")
//                .config("spark.shuffle.spill.numElementsForceSpillThreshold", Integer.valueOf(1024*1024*1024).toString())
//                .config("spark.shuffle.sort.initialBufferSize", Integer.valueOf(1024*1024).toString())
                .config("spark.sql.windowExec.buffer.spill.threshold", Integer.valueOf(1024*1024).toString())
                .master("local[4]")
                .appName("NBS_Coding_Challenge_Top_10")
                .getOrCreate();

        // read in the [somewhat] raw data.
        Dataset<Row> df = session
                .read()
                .schema(Util.PAGE_VIEW_SCHEMA)
                .parquet(sourceDirPath);

        // ok. since we may be scanning more than one hour here, we want a cumulative sum (this collapses rows for the
        // same page across multiple hours. If we're only dealing with one hour, this step isn't needed.
        df = df.groupBy("lang", "name")
                .agg(sum("views").as("views"))
                .select("lang", "name", "views");

        // register that result so we can select from it using sparksql.
        df.createOrReplaceTempView("tmp_requests_by_hour");

        // create a subselect that partitions by language and orders each partition by views.
        // the outer query then selects the first 10 rows of each partition.
        // NOTE: dense_rank() includes ties and produces very long tails for lightly trafficked locales. Use row_number()
        df = session.sql(
                "SELECT lang, name, views FROM " +
//                        "    (SELECT lang, name, views, dense_rank() " + // ugh.
                        "    (SELECT lang, name, views, row_number() " +
                        "       OVER (PARTITION BY lang ORDER BY views DESC) as rank " +
                        "    FROM tmp_requests_by_hour) tmp " +
                        "where rank <= 10"
        );

        // coalesce to a single file, save it all as CSV.
        df.select("lang", "name", "views")
                .coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save(outputDirPath);
    }
}
