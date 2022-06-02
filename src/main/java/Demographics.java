import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.Common;
import utils.MySQL;
import utils.Spark;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Demographics {
    public static void main(String[] args) {
        Common.Arguments arguments = new Common.Arguments(args);
        Demographics demographics = new Demographics();
        if ("pc".equals(arguments.device)) {
            demographics.analyze(arguments.date, true);
        } else if ("mb".equals(arguments.device)) {
            demographics.analyze(arguments.date, false);
        } else {
            demographics.analyze(arguments.date, true);
            demographics.analyze(arguments.date, false);
        }
    }
    public void analyze(String date, boolean isPC) {
        System.out.println("Analyze demographics " + (isPC ? "PC" : "MB") + ": " + date);

        MySQL db = new MySQL();

        SparkSession spark = Spark.getSession("user-view-analysis");
        String pathLogDemo = Spark.HDFS_23202 + (isPC ? Spark.DemoPC : Spark.DemoMB) + date;
        Dataset<Row> dfDemo = spark.read().parquet(pathLogDemo).filter("age != -1")
                .select(split(col("dt")," ").getItem(0).as("date"), col("*"));

        List<Row> resultAge = age(dfDemo);
        db.insertAge(resultAge, isPC);

        List<Row> resultGender = gender(dfDemo);
        db.insertGender(resultGender, isPC);

        MySQL.close();
    }

    public List<Row> age(Dataset<Row> df) {
        return df.groupBy("date", "age").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }

    public List<Row> gender(Dataset<Row> df) {
        return df.groupBy("date", "gender").agg(count("guid").as("view"), countDistinct("guid").as("user"))
                .collectAsList();
    }
}
