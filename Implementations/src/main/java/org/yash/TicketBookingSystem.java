package org.yash;

import java.sql.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TicketBookingSystem {
    // JDBC settings
    private static final String URL =
            "jdbc:mysql://localhost:3306/db"
                    + "?useSSL=false"
                    + "&allowPublicKeyRetrieval=true"
                    + "&serverTimezone=UTC";
    private static final String USER     = "root";
    private static final String PASSWORD = "290516";

    BlockingQueue<Connection> dbConnPool;
    public void main() throws InterruptedException, SQLException {
        dbConnPool = getDbConnPool();
        String sql = "select * from users";
        String resetSql = "update seats set user_id = null where id > 0";
        ArrayList<Long> userIds = new ArrayList<>();
        Connection connection = dbConnPool.poll(13, TimeUnit.SECONDS);
        try {
            PreparedStatement preparedStatement;
            assert connection != null;
            preparedStatement = connection.prepareStatement(resetSql);
            preparedStatement.executeUpdate();
        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
        try {
            PreparedStatement preparedStatement;
            preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                userIds.add(resultSet.getLong(1));
            }
        } catch (Exception ignored) {
            ignored.printStackTrace();
        } finally {
            if (connection != null) {
                dbConnPool.put(connection);
            }
        }
        ArrayList<Thread> bookings = new ArrayList<>();
        for (Long userId : userIds) {
            tryToBookTicket(userId);
        }
        bookings.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        printSeatMap(1);
    }

    private void tryToBookTicket(Long userId) throws InterruptedException {
        String findSql   = "SELECT * FROM seats "
                + "WHERE trip_id = 1 AND user_id IS NULL "
                + "ORDER BY id "
                + "LIMIT 1 "
                + "FOR UPDATE";
        String updateSql = "UPDATE seats SET user_id = ? WHERE id = ?";

        Connection conn = null;

        try {
            conn = dbConnPool.poll(13, TimeUnit.SECONDS);
            if (conn == null) throw new RuntimeException("No DB connection");

            conn.setAutoCommit(false);

            try (PreparedStatement findSt = conn.prepareStatement(findSql)) {
                ResultSet rs = findSt.executeQuery();
                if (!rs.next()) {
                    System.out.println("No available seats for user: " + userId);
                    conn.rollback();
                    return;
                }
                long seatId = rs.getLong(1);
                String seatName = rs.getString(2);

                try (PreparedStatement updSt = conn.prepareStatement(updateSql)) {
                    updSt.setLong(1, userId);
                    updSt.setLong(2, seatId);
                    int rows = updSt.executeUpdate();
                    if (rows != 1) {
                        throw new SQLException("Failed to update seat_name =" + seatName);
                    }
                }

                conn.commit();
                System.out.println("User : " + userId + ", seat : " + seatName);
            }

        } catch (Exception ex) {
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException ignore) {}
            }
            System.out.println("Booking failed for user " + userId + ": " + ex.getMessage());

        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    dbConnPool.put(conn);
                } catch (Exception ignore) { }
            }
        }
    }


    /*AI generated to print the seat map*/
    private void printSeatMap(int tripId) throws SQLException, InterruptedException {
        // 1. Prepare the empty grid
        int rows = 20, cols = 6;
        char[][] grid = new char[rows][cols];
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                grid[r][c] = '·';  // dot for empty
            }
        }

        // 2. Fetch all seats for this trip, and collect assignments
        String sql = "SELECT seat_name, user_id FROM seats WHERE trip_id = ?";
        Connection conn = dbConnPool.poll(5, TimeUnit.SECONDS);
        if (conn == null) throw new SQLException("No DB connection for map");

        // map seat → list of assigned user IDs
        Map<String, ArrayList<Long>> assignments = new HashMap<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, tripId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String seat = rs.getString("seat_name");    // e.g. "12C"
                Long   uid  = rs.getObject("user_id", Long.class);

                // record assignment
                assignments
                        .computeIfAbsent(seat, k -> new ArrayList<>())
                        .add(uid);

                // parse row and col for grid
                int rowNum = Integer.parseInt(seat.substring(0, seat.length() - 1));
                int colNum = seat.charAt(seat.length() - 1) - 'A';
                // mark: X if exactly one booking, D if duplicate, · if empty
                char mark = '·';
                List<Long> users = assignments.get(seat);
                if (users.size() == 1 && uid != null) {
                    mark = 'X';
                } else if (users.size() > 1) {
                    mark = 'D';
                }
                grid[rowNum - 1][colNum] = mark;
            }
        } finally {
            dbConnPool.put(conn);
        }

        // 3. Print header
        System.out.print("    ");
        for (char c = 'A'; c <= 'F'; c++) {
            String s = " " + c + " " + (c == 'C' ? " " : "");
            System.out.print(s);
        }
        System.out.println();

        // 4. Print each row
        for (int r = 0; r < rows; r++) {
            System.out.printf("%2d: ", r + 1);
            for (int c = 0; c < cols; c++) {
                String s = " " + grid[r][c] + " " + (c == 2 ? " " : "");
                System.out.print(s);
            }
            System.out.println();
        }

        // 5. Report duplicates
        boolean foundDup = false;
        for (Map.Entry<String, ArrayList<Long>> e : assignments.entrySet()) {
            ArrayList<Long> users = e.getValue();
            // ignore rows where user_id was null or only one booking
            long nonNullCount = users.stream().filter(Objects::nonNull).count();
            if (nonNullCount > 1) {
                if (!foundDup) {
                    System.out.println("\n⚠️ Duplicate seat assignments detected:");
                    foundDup = true;
                }
                System.out.printf("  Seat %s assigned to users %s%n",
                        e.getKey(),
                        users.stream()
                                .filter(Objects::nonNull)
                                .toList());
            }
        }
        if (!foundDup) {
            System.out.println("\nNo duplicate seat assignments.");
        }
    }


    private static BlockingQueue<Connection> getDbConnPool() {
        int totalDbConnections = 140;
        BlockingQueue<Connection> pool = new LinkedBlockingQueue<>(totalDbConnections)/*ArrayBlockingQueue<>(140, true)*/;
        for (int i = 0; i < totalDbConnections; i++) {
            pool.add(newDbConnection());
        }
        return pool;
    }

    private static Connection newDbConnection() {
        try {
            return DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to open new connection", e);
        }
    }
}
