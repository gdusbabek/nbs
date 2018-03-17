package gdusbabek.nbs;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Spark driver to convert a single CSV file to a number of parquet files.
 * We assume the schema of the wikimedia raw pagecount data (http://dumps.wikimedia.org/other/pagecounts-raw).
 *
 * This ingest class also does some light filtering.
 *
 * Inputs are:
 * args[0] - local path of csv file to read.
 * args[1] - directory to save parquet files to.
 *
 */
public class Csv2Parquet {

    // evaluating a few different filter functions to get rid of the special prefix pages.

    // uses a regex. can be expensive.
    private static final FilterFunction<Row> filterA = new FilterFunction<Row>() {
        final Pattern invalidPagePattern = Pattern.compile(".*:.*");

        @Override
        public boolean call(Row row) throws Exception {
            return !invalidPagePattern.matcher(row.get(1).toString()).matches();
        }
    };

    // doesn't rely on regex.
    private static final FilterFunction<Row> filterB = new FilterFunction<Row>() {
        @Override
        public boolean call(Row row) throws Exception {
            return !row.get(1).toString().contains(":");
        }
    };

    // english only. :(
    private static FilterFunction<Row> filterC = new FilterFunction<Row>() {
        final String[] INVALID_PAGE_PREFIXES = {
                "Special:", "Image:", "Template:", "User:", "MediaWiki:", "File:", "Meta:", "Talk:",
                // these next few are questionable.
                "Wikipedia:", "Wikinews:", "404_error",
        };
        @Override
        public boolean call(Row row) throws Exception {
            final String pageName = row.get(1).toString();
            boolean invalid = Arrays.stream(INVALID_PAGE_PREFIXES).anyMatch(prefix -> pageName.startsWith(prefix));
            return !invalid;
        }
    };


    public static void main(String args[]) {
        String sourceFilePath = args[0];
        String saveDirPath = args[1];

        // compute the year/month/day
        Util.DateInfo dateInfo = Util.path2Info(sourceFilePath);

        // get a spark session. local for now.
        SparkSession session = SparkSession
                .builder()
                .master("local[4]") // just for now.
                .appName("NBS_Coding_Challenge_Ingest")
                .getOrCreate();

        // read in the csv. there is no header. use the simple static schema.
        Dataset<Row> df = session
                .read()
                .option("header", "false")
                .option("delimiter", " ")
                .schema(Util.PAGE_VIEW_SCHEMA)
                .csv(sourceFilePath);

        // NOTE: even though this is ingest, we do light filtering and augmentation here (cleaning). There is no point
        //       in including data that will not be needed during any point of computation.

        // filter out the "foo:bar" pages.
        df = df.filter(filterB);

        // add some columns for sake of partitioning.
        df = df.withColumn("year", lit(dateInfo.year))
                .withColumn("month", lit(dateInfo.month))
                .withColumn("day", lit(dateInfo.day))
                .withColumn("hour", lit(dateInfo.hour));

        // since the input is a single file, let's split it up so that we output to a few files.
        // this will make the top-10 computation more parallelizable.
        // for a data set this small, it's not needed, but would be if we were to scale up.
        df = df.repartition(4);

        // write the parquet. partition by year/month/day/hour
        // any subsequent hour re-writes should happen in a clean directory and not be appended.
        df.write()
                .mode("append") // what state are we in if we fail here? It would probably be better to add "hour" to partitions.
                .partitionBy("year", "month", "day", "hour")
                .parquet(saveDirPath);

        session.close();
    }
}
