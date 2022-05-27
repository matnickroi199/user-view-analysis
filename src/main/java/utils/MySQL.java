package utils;

import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.List;

public class MySQL {
    private static volatile Connection conn;

    public MySQL() throws SQLException, ClassNotFoundException {
        getConnection();
    }

    private void getConnection() throws ClassNotFoundException, SQLException {
        if (conn == null) {
            synchronized (MySQL.class) {
                if (conn == null) {
                    Class.forName("com.mysql.jdbc.Driver");
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

    public void insertToDB(List<Row> data) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("REPLACE into tbl_bidding values (?,?,?,?,?,?,?,?)");
        int count = 0;
        for(Row r : data) {
            stmt.setLong(1, r.getLong(0));
            stmt.setInt(2, r.getInt(1));
            stmt.setDouble(3, r.getDouble(2));
            stmt.setDouble(4, r.getDouble(3));
            stmt.setDouble(5, r.getDouble(4));
            stmt.setDouble(6, r.getDouble(5));
            stmt.setDouble(7, r.getDouble(6));
            stmt.setDouble(8, r.getDouble(7));
            stmt.addBatch();
            count++;
            if(count == 5000) {
                stmt.executeBatch();
                count = 0;
            }
        }
        stmt.executeBatch();
    }
}
