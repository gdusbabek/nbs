package gdusbabek.nbs;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.sum;

import java.io.File;
import java.util.Arrays;

public class Messing_Around_Please_Ignore_This {

    // one problem with this approach is that it does not take into account localization.
    // Example: "Special" is "Spezial" Romanian.
    private static final String[] INVALID_PAGE_PREFIXES = {
            "Special:",
            "Image:",
            "Template:",
            "User:",
            "MediaWiki:",
            "File:",
            "Meta:",
            "Talk:",
            // these next few are questionable.
            "Wikipedia:",
            "Wikinews:",
            "404_error",
    };

    public static void main(String args[]) {

        if (args.length < 1) {
            throw new RuntimeException("Invalid number of arguments");
        }

        File inputFile = new File(args[0]);
        if (!inputFile.exists() || !inputFile.isFile()) {
            throw new RuntimeException("Input file is not valid");
        }

        SparkSession session = SparkSession
                .builder()
                .master("local[4]")
                .appName("NBS_Coding_Challenge")
//                .config("option", "value")
                .getOrCreate();


        StructType schema = new StructType(new StructField[] {
                new StructField("lang", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("views", DataTypes.LongType, false, Metadata.empty()),
                new StructField("bytes", DataTypes.LongType, false, Metadata.empty())
        });

        // read that file into a dataframe.
        Dataset<Row> df = session
                .read()
                .option("header", "false")
                .option("delimiter", " ")
                .schema(schema)
                // no escapes or specials for quoting, right?
                .csv(args[0]);


        // ok. let's get rid of some rows we don't care about.
        // welp. df = df.filter(row -> !Arrays.stream(INVALID_PAGE_PREFIXES).anyMatch(prefix -> row.get(1).toString().startsWith(prefix)));
        // this approach doesn't handle the non-english locales though. e.g.: "Special" in english is "Spezial" in Romanian.
        df = df.filter(new FilterFunction<Row>() {
            public boolean call(Row row) throws Exception {
                final String pageName = row.get(1).toString();
                boolean invalid = Arrays.stream(INVALID_PAGE_PREFIXES).anyMatch(prefix -> pageName.startsWith(prefix));
                return !invalid;
            }
        });
        //df = df.filter(col("lang").equalTo("en").or(col("lang").equalTo("es")).or(col("lang").equalTo("af")));
        df.createOrReplaceTempView("tmp_requests_by_hour");

        // is there a guarantee of one row per article? If not, you want to do this to aggregate the sums.
//        Dataset<Row> cumulative = df.groupBy("lang", "name").agg(sum("views").as("cum_views"));
//        cumulative.select("lang", "name", "cum_views").write()
//                .format("csv")
//                .option("header", "true")
//                .save("/tmp/ugh");



        // but what I really think we're after is most popular pages by language
//        df = session.sql("select lang, name, views from tmp_requests_by_hour");
//        df.orderBy("lang", "views").groupBy("name")
        df = session.sql(
                "SELECT lang, name, views FROM " +
//                        "    (SELECT lang, name, views, dense_rank() " + // doesn't work because it includes ties, gives a really long tail in some cases.
                        "    (SELECT lang, name, views, row_number() " +
                        "       OVER (PARTITION BY lang ORDER BY views DESC) as rank " +
                        "    FROM tmp_requests_by_hour) tmp " +
                        "where rank <= 10");


        df.select("lang", "name", "views")
//                .repartition(1)
                .coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .save("/tmp/ugh");

        // you know what would be cool? If we could order the results by the sum of their counters.
    }
}
