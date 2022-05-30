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
            analyst.analyse(arguments.date, true);
        } else if ("mb".equals(arguments.device)) {
            analyst.analyse(arguments.date, false);
        } else {
            analyst.analyse(arguments.date, true);
            analyst.analyse(arguments.date, false);
        }
    }

    public void analyse(String date, boolean isPC) {
        System.out.println("Analyse log " + (isPC ? "PC" : "MB") + ": " + date);

        MySQL db = new MySQL();

        SparkSession spark = Spark.getSession("user-view-analysis");
        String pathLog = (isPC ? LogProcessor.PC_OUTPUT_PATH : LogProcessor.MB_OUTPUT_PATH) + date;

        List<Row> resultOV = overview(spark, pathLog);
        db.insertOverview(resultOV, isPC);

        MySQL.close();
    }

    public List<Row> overview(SparkSession spark, String path) {
        Dataset<Row> df = spark.read().parquet(path);
        return df.select(split(col("time")," ").getItem(0).as("date"), col("guid"))
                .groupBy("date").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }
}
