package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequestHandler;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.LongAdder;

public class JdbcBenchmarkRunner {
    private static final long KEY_CNT = 50_000_000;

    private static final int THREAD_CNT = 1;

    private static final int BATCH_SIZE = 1000;

    private static final LongAdder OPS = new LongAdder();

    private static volatile boolean done;

    public static void main(String[] args) throws Exception {
        U.delete(new File("C:\\Personal\\code\\incubator-ignite\\work"));

        IgniteConfiguration cfg = new IgniteConfiguration().setLocalHost("127.0.0.1");

        cfg.setClientConnectorConfiguration(null);

//        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalMode(WALMode.LOG_ONLY);
//
//        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
//        dsCfg.getDefaultDataRegionConfiguration().setMaxSize(4 * 1024 * 1024 * 1024L);
//
//        dsCfg.setCheckpointFrequency(Long.MAX_VALUE);
//
//        cfg.setDataStorageConfiguration(dsCfg);

        try (Ignite node = Ignition.start(cfg)) {
            node.active(true);

            IgniteConfiguration cliCfg = new IgniteConfiguration().setLocalHost("127.0.0.1").setIgniteInstanceName("cli").setClientMode(true);

            try (Ignite cli = Ignition.start(cliCfg)) {
                try (Connection conn = connect()) {
                    execute(conn, "CREATE TABLE tbl (id BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT, v4 BIGINT)");
                }

                new Thread(new Runnable() {
                    @Override public void run() {
                        while (!done) {
                            long startTime = System.currentTimeMillis();
                            long startOps = OPS.longValue();

                            try {
                                Thread.sleep(3000L);
                            }
                            catch (InterruptedException e) {
                                break;
                            }

                            long endTime = System.currentTimeMillis();
                            long endOps = OPS.longValue();

                            double t = 1000 * (double)(endOps - startOps) / (double)(endTime - startTime);

                            if (!done)
                                System.out.println("Throughput: " + String.format("%1$,.2f", t) + " ops/sec");
                        }
                    }
                }).start();

                JdbcRequestHandler.STREAMER = true;

                long start = System.currentTimeMillis();

                CyclicBarrier startBarrier = new CyclicBarrier(THREAD_CNT);
                CountDownLatch stopLatch = new CountDownLatch(THREAD_CNT);

                for (int i = 0; i < THREAD_CNT; i++) {
                    final int i0 = i;

                    new Thread(new Runnable() {
                        @SuppressWarnings("InfiniteLoopStatement")
                        @Override public void run() {
                            try (Connection conn = connect()) {
                                startBarrier.await();

                                doUpdate(conn, i0);

                                execute(conn, "FLUSH");
                            }
                            catch (Exception e) {
                                System.out.println("ERROR: " + e);
                            }
                            finally {
                                stopLatch.countDown();
                            }
                        }
                    }).start();
                }

                stopLatch.await();

                done = true;

                long end = System.currentTimeMillis();

                float dur = (float)((double)(end - start) / 1000);

                System.out.println("TOTAL DURATION: " + dur);
            }
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    private static void doUpdate(Connection conn, int idx) throws Exception {
        long keyCnt = KEY_CNT / THREAD_CNT;

        long startIdx = keyCnt * idx;
        long endIdx = startIdx + keyCnt;

        System.out.println("INSERT interval [" + startIdx + " -> " + endIdx + ')');

        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO tbl (id, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?)")) {
            if (BATCH_SIZE == 0) {
                for (long i = startIdx; i < endIdx; i++) {
                    stmt.setLong(1, i);
                    stmt.setLong(2, i);
                    stmt.setLong(3, i);
                    stmt.setLong(4, i);
                    stmt.setLong(5, i);

                    stmt.execute();

                    OPS.increment();
                }
            }
            else {
                int curSize = 0;

                for (long i = startIdx; i < endIdx; i++) {
                    stmt.setLong(1, i);
                    stmt.setLong(2, i);
                    stmt.setLong(3, i);
                    stmt.setLong(4, i);
                    stmt.setLong(5, i);

                    stmt.addBatch();

                    curSize++;

                    if (curSize == BATCH_SIZE) {
                        stmt.executeBatch();

                        OPS.add(curSize);

                        curSize = 0;
                    }
                }

                if (curSize > 0) {
                    stmt.executeBatch();

                    OPS.add(curSize);
                }
            }

        }
    }

    private static Connection connect() throws Exception {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
    }

    private static void execute(Connection conn, String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }
}
