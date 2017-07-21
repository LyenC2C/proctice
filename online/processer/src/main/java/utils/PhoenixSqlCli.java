package utils;

import java.sql.*;

/**
 * Created by lyen on 17-7-4.
 */
public class PhoenixSqlCli {
    private static Connection conn = null;
    private static PreparedStatement ps = null;

    static {
        try {
            // load driver
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("加载驱动器类时出现异常");
        }
    }

    public static Connection getConnection() {

        try {
            conn = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        /**
         *get connection
         *jdbc 的 url 类似为 jdbc:phoenix [ :<zookeeper quorum> [ :<port number> ] [ :<root node> ] ]，
         *需要引用三个参数：hbase.zookeeper.quorum、hbase.zookeeper.property.clientPort、and zookeeper.znode.parent，
         *这些参数可以缺省不填而在 hbase-site.xml 中定义
         */
        return conn;
    }

    public static PreparedStatement getStatement(Connection conn, String sql) {
        try {
            ps = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ps;
    }

    public static void createTable(String table) {
        try {
            conn = PhoenixSqlCli.getConnection();
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }
            ResultSet rs = conn.getMetaData().getTables(null, null, table,
                    null);
            if (rs.next()) {
                System.out.println("table user is exist...");
                return;
            }
            String sql = "CREATE TABLE" + table + " (id varchar PRIMARY KEY,INFO.account varchar ,INFO.passwd varchar)";
            PreparedStatement ps = getStatement(conn, sql);
            ps.execute();
            System.out.println("create success...");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConn(conn);
        }

    }

    public static void upsert() {
        try {
            conn = PhoenixSqlCli.getConnection();
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }
            String sql = "upsert into user(id, INFO.account, INFO.passwd) values('001', 'admin', 'admin')";
            PreparedStatement ps = conn.prepareStatement(sql);

            // execute upsert
            String msg = ps.executeUpdate() > 0 ? "insert success..."
                    : "insert fail...";
            conn.commit();
            System.out.println(msg);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConn(conn);
        }
    }

    public static void query() {

        try {
            conn = PhoenixSqlCli.getConnection();
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }
            String sql = "select * from orders";
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            System.out.println("id" + "\t" + "account" + "\t" + "passwd");
            System.out.println("======================");
            if (rs != null) {
                while (rs.next()) {
                    System.out.print(rs.getString("orderid") + "\t");
                    System.out.print(rs.getString("customerid") + "\t");
                    System.out.print(rs.getString("itemid") + "\t");
                    System.out.print(rs.getDouble("quantity") + "\t");
                    System.out.println(rs.getString("date"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConn(conn);
        }
    }

    public static void closeConn(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void delete() {

        try {
            conn = PhoenixSqlCli.getConnection();
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }
            String sql = "delete from user where orderid='1630781'";
            PreparedStatement ps = conn.prepareStatement(sql);
            String msg = ps.executeUpdate() > 0 ? "delete success..."
                    : "delete fail...";
            conn.commit();
            System.out.println(msg);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {
        PhoenixSqlCli.query();
    }
}
