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
                stmt.setLong(3, row.getAs("user"));
                stmt.setLong(4, row.getAs("view"));
                stmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertBrowser(List<Row> rows, boolean isPC) {
        String device = isPC ? "PC" : "MB";
        try (PreparedStatement stmt = conn.prepareStatement("REPLACE INTO browser_analysis (time, browser_id, device, user, view) values (?,?,?,?,?)")){
            for (Row row : rows) {
                stmt.setString(1, row.getAs("date"));
                stmt.setInt(2, row.getAs("browser"));
                stmt.setString(3, device);
                stmt.setLong(4, row.getAs("user"));
                stmt.setLong(5, row.getAs("view"));
                stmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertLocation(List<Row> rows) {
        try (PreparedStatement stmt = conn.prepareStatement("REPLACE INTO location_analysis (time, loc_id, user, view) values (?,?,?,?)")){
            for (Row row : rows) {
                stmt.setString(1, row.getAs("date"));
                stmt.setInt(2, row.getAs("loc"));
                stmt.setLong(3, row.getAs("user"));
                stmt.setLong(4, row.getAs("view"));
                stmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertOS(List<Row> rows, boolean isPC) {
        String device = isPC ? "PC" : "MB";
        try (PreparedStatement stmt = conn.prepareStatement("REPLACE INTO os_analysis (time, os_id, device, user, view) values (?,?,?,?,?)")){
            for (Row row : rows) {
                stmt.setString(1, row.getAs("date"));
                stmt.setInt(2, row.getAs("os"));
                stmt.setString(3, device);
                stmt.setLong(4, row.getAs("user"));
                stmt.setLong(5, row.getAs("view"));
                stmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertTimeFrame(List<Row> rows, boolean isPC) {
        String device = isPC ? "PC" : "MB";
        try (PreparedStatement stmt = conn.prepareStatement("REPLACE INTO time_frame_analysis (time, frame_id, device, user, view) values (?,?,?,?,?)")){
            for (Row row : rows) {
                stmt.setString(1, row.getAs("date"));
                stmt.setInt(2, row.getAs("hour"));
                stmt.setString(3, device);
                stmt.setLong(4, row.getAs("user"));
                stmt.setLong(5, row.getAs("view"));
                stmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertGender(List<Row> rows, boolean isPC) {
        String device = isPC ? "PC" : "MB";
        try (PreparedStatement stmt = conn.prepareStatement("REPLACE INTO gender_analysis (time, gender, device, user, view) values (?,?,?,?,?)")){
            for (Row row : rows) {
                stmt.setString(1, row.getAs("date"));
                stmt.setInt(2, row.getAs("gender"));
                stmt.setString(3, device);
                stmt.setLong(4, row.getAs("user"));
                stmt.setLong(5, row.getAs("view"));
                stmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertAge(List<Row> rows, boolean isPC) {
        String device = isPC ? "PC" : "MB";
        try (PreparedStatement stmt = conn.prepareStatement("REPLACE INTO age_analysis (time, age, device, user, view) values (?,?,?,?,?)")){
            for (Row row : rows) {
                stmt.setString(1, row.getAs("date"));
                stmt.setInt(2, row.getAs("age"));
                stmt.setString(3, device);
                stmt.setLong(4, row.getAs("user"));
                stmt.setLong(5, row.getAs("view"));
                stmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
