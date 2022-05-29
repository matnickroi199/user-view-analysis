package utils;

import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.List;

public class MySQL {
    private static volatile Connection conn;

    public MySQL() {
        getConnection();
    }

    private void getConnection() {
        if (conn == null) {
            synchronized (MySQL.class) {
                if (conn == null) {
                    try {
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        String url = Config.getProperties().getProperty("mysql.url");
                        conn = DriverManager.getConnection(url);
                    } catch (ClassNotFoundException | SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                conn = null;
            }
        }
    }

    public void insertOverview(List<Row> rows, boolean isPC) {
        String device = isPC ? "PC" : "MB";
        try (PreparedStatement stmt = conn.prepareStatement("REPLACE INTO overview (time, device, user, view) values (?,?,?,?)")){
            for (Row row : rows) {
                stmt.setString(1, row.getAs("date"));
                stmt.setString(2, device);
                stmt.setInt(3, row.getAs("user"));
                stmt.setInt(4, row.getAs("view"));
                stmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
