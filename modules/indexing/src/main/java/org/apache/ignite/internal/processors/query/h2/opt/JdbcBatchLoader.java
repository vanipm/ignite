package org.apache.ignite.internal.processors.query.h2.opt;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JdbcBatchLoader {
    /** */
    private static final String SQL_CREATE = "CREATE TABLE IF NOT EXISTS Person(" +
        " id integer PRIMARY KEY," +
        " name varchar(50)," +
        " age integer," +
        " salary integer" +
        ")";

    /** */
    private static final String SQL_INSERT = "INSERT INTO Person(id, name, age, salary) VALUES (?, ?, ?, ?)";

    /**
     * @param msg Message to log.
     */
    private static void log(String msg) {
        U.debug(msg);
    }

    /**
     * Main entry point.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        IgniteConfiguration cfg = new IgniteConfiguration().setLocalHost("127.0.0.1");

        try (Ignite node = Ignition.start(cfg)) {
            try {
                JdbcBatchLoader ldr = new JdbcBatchLoader();

                ldr.load(10_000_000, 10_000, 8, "127.0.0.1");
            }
            catch (Exception e) {
                log("Failed to load data into cloud");

                e.printStackTrace();
            }
        }
    }

    /**
     * Load data into cloud.
     *
     * @param total Total number of rows to lad.
     * @param batch Batch size.
     * @param threads How many threads to use.
     * @param addr JDBC endpoint address.
     * @throws Exception If failed to load data to cloud.
     */
    public void load(int total, int batch, int threads, String addr) throws Exception {
        log("Connecting to IGNITE...");

        ComboPooledDataSource dataSrc = new ComboPooledDataSource();

        dataSrc.setDriverClass("org.apache.ignite.IgniteJdbcThinDriver");
        dataSrc.setJdbcUrl("jdbc:ignite:thin://" + addr);

        try(Connection conn = dataSrc.getConnection()) {
            Statement stmt = conn.createStatement();

            stmt.execute(SQL_CREATE);

            U.closeQuiet(stmt);
        }

        int cnt = total / batch;

        CountDownLatch latch = new CountDownLatch(cnt);

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        log("Start loading of " + total + " records...");

        long start = System.currentTimeMillis();

        for (int i = 0; i < cnt; i++)
            exec.execute(new Worker(dataSrc, i, batch, latch));

        latch.await();

        log("Loading time: " + (System.currentTimeMillis() - start) / 1000 + "seconds");
        log("Loading finished!");

        U.shutdownNow(JdbcBatchLoader.class, exec, null);
        dataSrc.close();
    }

    /**
     * Class that execute batch loading.
     */
    private static class Worker implements Runnable {
        /** */
        private final ComboPooledDataSource dataSrc;

        /** */
        private final int packet;

        /** */
        private final CountDownLatch latch;

        /** */
        private final int start;

        /** */
        private final int finish;

        /**
         *
         * @param dataSrc Data source.
         * @param packet Packet ID.
         * @param batch Batch size.
         * @param latch Control latch to complete loading.
         */
        private Worker(ComboPooledDataSource dataSrc, int packet, int batch, CountDownLatch latch) {
            this.dataSrc = dataSrc;
            this.packet = packet;
            this.latch = latch;

            start = packet * batch;
            finish = start + batch;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try(Connection conn = dataSrc.getConnection()) {
                PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT);

                for (int i = start; i < finish; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "Some name" + i);
                    pstmt.setInt(3, 100);
                    pstmt.setInt(4, 200);

                    pstmt.addBatch();
                }

                pstmt.executeBatch();
            }
            catch (Throwable e) {
                log("Failed to load packet: [packet=" + packet + ", err=" + e.getMessage() + "]");

                e.printStackTrace();
            }
            finally {
                latch.countDown();

//                log("Processed packed: " + packet);
            }
        }
    }
}
