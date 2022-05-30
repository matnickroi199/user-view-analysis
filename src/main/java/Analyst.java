import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.Common;
import utils.MySQL;
import utils.Spark;

import java.util.List;

import static org.apache.spark.sql.functions.*;
import static utils.Spark.HDFS;

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
        System.out.println("Analyse log " + (isPC ? "PC" : "MB") + ": " + date);

        MySQL db = new MySQL();

        SparkSession spark = Spark.getSession("user-view-analysis");
        String pathLog = HDFS + (isPC ? LogProcessor.PC_OUTPUT_PATH : LogProcessor.MB_OUTPUT_PATH) + date;
        Dataset<Row> df = spark.read().parquet(pathLog);

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
        return df.select(split(col("time")," ").getItem(0).as("date"), col("guid"))
                .groupBy("date").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> browser(Dataset<Row> df) {
        return df.select(split(col("time")," ").getItem(0).as("date"), col("guid"), col("browser"))
                .groupBy("date", "browser").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> os(Dataset<Row> df) {
        return df.select(split(col("time")," ").getItem(0).as("date"), col("guid"), col("os"))
                .groupBy("date", "os").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> timeframe(Dataset<Row> df) {
        return df.select(split(col("time")," ").getItem(0).as("date"), substring(col("time"), 12, 2).cast("int").as("hour"), col("guid"))
                .groupBy("date", "hour").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> location(SparkSession spark, String date, Dataset<Row> df) {
        String logMB = HDFS + LogProcessor.MB_OUTPUT_PATH + date;
        Dataset<Row> pc = df.filter("loc != -1").select("time", "loc", "guid");
        Dataset<Row> mb = spark.read().parquet(logMB).filter("loc != -1").select("time", "loc", "guid");
        return pc.union(mb).select(split(col("time")," ").getItem(0).as("date"), col("loc"), col("guid"))
                .groupBy("date", "loc").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }
}
