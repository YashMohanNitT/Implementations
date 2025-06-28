package org.yash;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ConnectionPoolingForDbConnections {
    // JDBC settings
    private static final String URL =
            "jdbc:mysql://localhost:3306/testdb"
                    + "?useSSL=false"
                    + "&allowPublicKeyRetrieval=true"
                    + "&serverTimezone=UTC";
    private static final String USER     = "root";
    private static final String PASSWORD = "root_password";

    public static void main(String[] args) {
//        callWithNonConnPool();
        callWithConnPool();
    }

    private static void callWithConnPool() {
        long startTime = System.currentTimeMillis();
        BlockingQueue<Connection> dbConnPool = getDbConnPool();
        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            Runnable runnable = getRunnable(i, dbConnPool);
            Thread e = new Thread(runnable);
            threadList.add(e);
            e.start();
        }
        threadList.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        });
        System.out.println("total time : " + (System.currentTimeMillis() - startTime)/1000.0);
    }

    private static void callWithNonConnPool() {
        long startTime = System.currentTimeMillis();
        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < 249; ++i) {
            Runnable runnable = getRunnable(i, null);
            Thread e = new Thread(runnable);
            threadList.add(e);
            e.start();
        }
        threadList.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        });
        System.out.println("total time : " + (System.currentTimeMillis() - startTime)/1000.0);
    }

    private static BlockingQueue<Connection> getDbConnPool() {
        int totalDbConnections = 140;
        BlockingQueue<Connection> pool = new LinkedBlockingQueue<>(totalDbConnections)/*ArrayBlockingQueue<>(140, true)*/;
        for (int i = 0; i < totalDbConnections; i++) {
            pool.add(newDbConnection(null));
        }
        return pool;
    }

    private static Runnable getRunnable(int i, BlockingQueue<Connection> dbConnPool) {
        return () -> {
//                synchronized (ConnectionPoolingForDbConnections.class) {
                    Connection connection = newDbConnection(dbConnPool);
                    try {
                        PreparedStatement stmt = connection.prepareStatement("SELECT 1");
                        ResultSet rs = stmt.executeQuery();
                        rs.next();
                        System.out.println(i + " :- OK (got " + rs.getInt(1) + ")");
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    if (dbConnPool != null && connection != null) {
                        try {
                            dbConnPool.put(connection);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
//                }
        };
    }

    private static Connection newDbConnection(BlockingQueue<Connection> dbConnPool) {
        if (dbConnPool != null) {
            try {
                Connection c = dbConnPool.poll(13, TimeUnit.SECONDS);
                if (c == null) {
                    throw new TimeoutException("Timed out waiting for a DB connection");
                }
                return c;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for a pooled connection", e);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to open new connection", e);
        }
    }
}
