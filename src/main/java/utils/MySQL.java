package utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.In;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQL {
    private static volatile Connection conn;

    public MySQL() throws SQLException, ClassNotFoundException {
        getConnection();
    }

    private void getConnection() throws ClassNotFoundException, SQLException {
        if (conn == null) {
            synchronized (MySQL.class) {
                if (conn == null) {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    String url = Config.getProperties().getProperty("mysql.url");;
                    conn = DriverManager.getConnection(url);
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
                stmt.setString(1, row.getAs("time"));
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
