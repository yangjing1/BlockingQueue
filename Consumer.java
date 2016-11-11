package com.bj58.pay.aof.exec.callable;

import com.bj58.pay.aof.model.Pairs;
import com.bj58.pay.aof.service.impl.AofDBServiceImpl;
import com.bj58.pay.common.log.PayLogger;
import com.bj58.pay.common.log.PayLoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.bj58.pay.aof.exec.main.SubOrderHisMerge.TIME_OUT;

/**
 * @author <a href="mailto:yangjing06@58ganji.com">yangjing06</a>
 * @since 16/11/9 下午3:53
 * @version 1.0
 */
public class Consumer implements Runnable {
    private PayLogger logger = PayLoggerFactory.getLogger(Consumer.class);
    private CountDownLatch countDownLatch;
    private boolean        isWrite;
    private BlockingQueue<Pairs> blockingQueue;

    public Consumer(CountDownLatch countDownLatch, BlockingQueue<Pairs> blockingQueue, boolean isWrite) {
        this.countDownLatch = countDownLatch;
        this.isWrite = isWrite;
        this.blockingQueue = blockingQueue;
    }

    @Override public void run() {
        try {
            Pairs pairs;
            /*
             * poll data from queue. waiting for com.bj58.pay.aof.exec.main.SubOrderHisMerge.TIME_OUT to break.
             */
            while ((pairs = blockingQueue.poll(TIME_OUT, TimeUnit.SECONDS)) != null) {
                /*
                 * insert data to db.
                 */
                AofDBServiceImpl.insertHisFromSubOrder(pairs, isWrite);
            }
        } catch (Exception e) {
            logger.error("[Consumer err] ", e);
        } finally {
            countDownLatch.countDown();
        }
    }
}
