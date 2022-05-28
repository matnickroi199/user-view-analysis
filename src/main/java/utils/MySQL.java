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

    public void insertToDB(Map<Integer, String> map) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("replace into location_info values (?,?)");
        int count = 0;
        for(Integer id : map.keySet()) {
            stmt.setInt(1, id);
            stmt.setString(2, map.get(id));
            stmt.addBatch();
            count++;
            if(count == 5000) {
                stmt.executeBatch();
                count = 0;
            }
        }
        if (count > 0) stmt.executeBatch();
    }

    public static void main(String[] args) {
        Map<Integer, String> loc = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("/home/thang/IdeaProjects/ip2location_v2/resources/forein_info/oneForAll.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] s = line.split("\\s+", 2);
                loc.put(Integer.valueOf(s[0]), s[1]);
            }
            br.close();
            MySQL db = new MySQL();
            db.insertToDB(loc);
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
