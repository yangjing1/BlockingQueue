package com.bj58.pay.aof.exec.main;

import com.bj58.dao.core.DaoWrapper;
import com.bj58.pay.aof.exec.callable.Consumer;
import com.bj58.pay.aof.exec.callable.Producer;
import com.bj58.pay.aof.model.Pairs;
import com.bj58.pay.aof.service.impl.AofDBServiceImpl;
import com.bj58.pay.common.log.PayLogger;
import com.bj58.pay.common.log.PayLoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:yangjing06@58ganji.com">yangjing06</a>
 * @since 16/11/8 下午2:22
 * @version 1.0
 */
public class SubOrderHisMerge {
    private static       PayLogger            logger          = PayLoggerFactory.getLogger(SubOrderHisMerge.class);
    private static       ExecutorService      executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    private final static BlockingQueue<Pairs> blockingQueue   = new ArrayBlockingQueue<Pairs>(5000);
    private static       CountDownLatch       countDownLatch;//  = new CountDownLatch(size);
    public static        String               January         = "2015-01-01 00:00:00";
    public static        String               February        = "2015-02-01 00:00:00";
    public static        String               END             = "2016-01-01 00:00:00";
    public static        String               March           = "2015-03-01 00:00:00";
    public static        long                 LINE            = 200L;
    public static        long                 TIME_OUT        = 50L;
    private static       AtomicInteger        querySize       = new AtomicInteger(0);
    public static       AtomicInteger        writeSize       = new AtomicInteger(0);

    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        SubOrderHisMerge subOrderHisMerge = new SubOrderHisMerge();
        /*
         * checking for update db.
         */
        boolean isWrite = InitUtil.getArg(args);
        try {
            /*
             * init db and log.
             */
            InitUtil.initDbAndLog4j();
            /*
             * put data into blockingQueue.
             */
            subOrderHisMerge.produceData();
            /*
             * poll data from blockingQueue.
             */
            subOrderHisMerge.consumeData(isWrite);
            /*
             * waiting to done.
             */
            subOrderHisMerge.handling();
        } catch (Exception e) {
            logger.error("[main err]", e);
        } finally {
            /*
             * count time and shutdown thread pool.
             */
            subOrderHisMerge.end(time);
        }
        /*
         * procedure exit.
         */
        subOrderHisMerge.exit();
    }

    /**
     * produce data from db. and put into queue
     */
    private void produceData() {
        List<DaoWrapper> daoList = AofDBServiceImpl.getDaoList("order");
        countDownLatch  = new CountDownLatch(daoList.size() + 1);
        for (DaoWrapper dao : daoList) {
            executorService.execute(new Producer(dao, countDownLatch, blockingQueue, querySize));
        }
    }

    /**
     * consum data from queue. waiting for com.bj58.pay.aof.exec.main.TIME_OU, then quit.
     * @param isWrite
     */
    private void consumeData(boolean isWrite) {
        executorService.execute(new Consumer(countDownLatch, blockingQueue, isWrite));
    }

    /**
     * waiting threads to end.
     * @throws InterruptedException
     */
    private void handling() throws InterruptedException {
        countDownLatch.await();
    }

    /**
     * shutdown thread pool. count sth.
     * @param start
     */
    private void end(long start) {
        executorService.shutdown();
        logger.info("[end] order_size " + querySize.get() + " sub_order_his size " + writeSize.get() + " cost " + (System.currentTimeMillis() - start));
    }

    /**
     * exit
     */
    private void exit() {
        System.exit(0);
    }
}
