package org.yash;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ConnectionPoolingForDbConnections {
    // JDBC settings
    private static final String URL      = "jdbc:mysql://localhost:3306/db?useSSL=false&serverTimezone=UTC";
    private static final String USER     = "root";
    private static final String PASSWORD = "<fill-password-here>";

    public static void main(String[] args) {
//        callWithNonConnPool();
        callWithConnPool();
    }

    private static void callWithConnPool() {
        long startTime = System.currentTimeMillis();
        BlockingQueue<Connection> dbConnPool = getDbConnPool();
        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < 40; ++i) {
            Runnable runnable = getRunnable(i, dbConnPool);
            Thread e = new Thread(runnable);
            e.start();
            threadList.add(e);
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
        for (int i = 0; i < 149; ++i) {
            Runnable runnable = getRunnable(i, null);
            Thread e = new Thread(runnable);
            e.start();
            threadList.add(e);
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
        BlockingQueue<Connection> pool = new LinkedBlockingQueue<>(10);
        for (int i = 0; i < 10; i++) {
            pool.add(newDbConnection(null));
        }
        return pool;
    }

    private static Runnable getRunnable(int i, BlockingQueue<Connection> dbConnPool) {
        return () -> {
//                synchronized (Main.class) {
                    Connection connection = newDbConnection(dbConnPool);
                    try {
                        PreparedStatement stmt = connection.prepareStatement("SELECT 1");
                        ResultSet rs = stmt.executeQuery();
                        rs.next();
                        System.out.println(i + " :- OK (got " + rs.getInt(1) + ")");
                        Thread.sleep(3000);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    if (connection != null) {
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
