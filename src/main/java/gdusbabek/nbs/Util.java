package gdusbabek.nbs;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

    // column names aren't important. Order is though.
    public static final StructType PAGE_VIEW_SCHEMA = new StructType(new StructField[] {
        new StructField("lang", DataTypes.StringType, false, Metadata.empty()),
        new StructField("name", DataTypes.StringType, false, Metadata.empty()),
        new StructField("views", DataTypes.LongType, false, Metadata.empty()),
        new StructField("bytes", DataTypes.LongType, false, Metadata.empty())
    });

    public static class DateInfo {
        public final String year;
        public final String month;
        public final String day;
        public final String hour;
        private DateInfo(String... parts) {
            year = parts.length > 0 ? parts[0] : null;
            month = parts.length > 1 ? parts[1] : null;
            day = parts.length > 2 ? parts[2] : null;
            hour = parts.length > 3 ? parts[3] : null;
        }
    }

    public static DateInfo path2Info(String sourceFilePath) {
        String fileName = Arrays
                .stream(sourceFilePath.split(File.separator))
                .reduce((first, second) -> second)
                .orElse(null);
        Pattern fileNamePattern = Pattern.compile("pagecounts-(\\d{4})(\\d{2})(\\d{2})-(\\d{6}).*");
        Matcher fileNameMatcher = fileNamePattern.matcher(fileName);
        if (!fileNameMatcher.matches()) throw new RuntimeException("Could not figure out YYYYMMDD");
        String year = fileNameMatcher.group(1);
        String month = fileNameMatcher.group(2);
        String day = fileNameMatcher.group(3);
        String hour = fileNameMatcher.group(4);
        return new DateInfo(year, month, day, hour);
    }
}
