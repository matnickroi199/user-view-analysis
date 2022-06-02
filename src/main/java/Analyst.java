import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.Common;
import utils.MySQL;
import utils.Spark;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Analyst {
    public static void main(String[] args) {
        Common.Arguments arguments = new Common.Arguments(args);
        Analyst analyst = new Analyst();
        if ("pc".equals(arguments.device)) {
            analyst.analyze(arguments.date, true);
        } else if ("mb".equals(arguments.device)) {
            analyst.analyze(arguments.date, false);
        } else {
            analyst.analyze(arguments.date, true);
            analyst.analyze(arguments.date, false);
        }
    }

    public void analyze(String date, boolean isPC) {
        System.out.println("Analyze log " + (isPC ? "PC" : "MB") + ": " + date);

        MySQL db = new MySQL();

        SparkSession spark = Spark.getSession("user-view-analysis");
        String pathLog = Spark.HDFS_9276 + (isPC ? LogProcessor.PC_OUTPUT_PATH : LogProcessor.MB_OUTPUT_PATH) + date;
        Dataset<Row> df = spark.read().parquet(pathLog).select(split(col("time")," ").getItem(0).as("date"), col("*"));

        List<Row> resultOV = overview(df);
        db.insertOverview(resultOV, isPC);

        List<Row> resultBrowser = browser(df);
        db.insertBrowser(resultBrowser, isPC);

        List<Row> resultOS = os(df);
        db.insertOS(resultOS, isPC);

        List<Row> resultTF = timeframe(df);
        db.insertTimeFrame(resultTF, isPC);

        if(isPC) {
            List<Row> resultLocation = location(spark, date, df);
            db.insertLocation(resultLocation);
        }

        MySQL.close();
    }

    public List<Row> overview(Dataset<Row> df) {
        return df.groupBy("date").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> browser(Dataset<Row> df) {
        return df.groupBy("date", "browser").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> os(Dataset<Row> df) {
        return df.groupBy("date", "os").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> timeframe(Dataset<Row> df) {
        return df.select(col("date"), substring(col("time"), 12, 2).cast("int").as("hour"), col("guid"))
                .groupBy("date", "hour").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> location(SparkSession spark, String date, Dataset<Row> df) {
        String logMB = Spark.HDFS_9276 + LogProcessor.MB_OUTPUT_PATH + date;
        Dataset<Row> pc = df.filter("loc != -1").select("date", "loc", "guid");
        Dataset<Row> mb = spark.read().parquet(logMB).filter("loc != -1").select(split(col("time")," ").getItem(0).as("date"), col("loc"), col("guid"));
        return pc.union(mb).groupBy("date", "loc").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }
}
