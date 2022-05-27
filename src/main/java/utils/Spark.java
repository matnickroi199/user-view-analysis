package utils;

import org.apache.spark.sql.SparkSession;

public class Spark {
    private static volatile SparkSession sparkSession;

    public static SparkSession getSession(String appName) {
        if (sparkSession == null) {
            synchronized (Spark.class) {
                if (sparkSession == null) {
                    try {
                        sparkSession = SparkSession
                                .builder()
                                .appName(appName)
                                .config("spark.sql.parquet.binaryAsString", "true")
                                .config("spark.sql.files.ignoreCorruptFiles", "true")
                                .config("spark.yarn.access.hadoopFileSystems", "hdfs://10.5.92.76:9000")
                                .getOrCreate();
                    } catch (Exception ignore) {

                    }
                }
            }
        }
        return sparkSession;
    }
}
