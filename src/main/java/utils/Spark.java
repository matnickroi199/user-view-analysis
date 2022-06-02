package utils;

import org.apache.spark.sql.SparkSession;

public class Spark {
    private static volatile SparkSession sparkSession;
    public static final String HDFS_9276 = "hdfs://10.5.92.76:9000";
    public static final String HDFS_23202 = "hdfs://192.168.23.202:9000";
    public static final String DemoPC = "/data/analytics/pvTosFull/pc/";
    public static final String DemoMB = "/data/analytics/pvTosFull/mob/";
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
                                .config("spark.yarn.access.hadoopFileSystems", HDFS_9276+","+HDFS_23202)
                                .getOrCreate();
                    } catch (Exception ignore) {

                    }
                }
            }
        }
        return sparkSession;
    }
}
