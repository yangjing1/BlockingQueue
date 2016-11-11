package com.bj58.pay.aof.exec.callable;

import com.bj58.dao.core.DaoWrapper;
import com.bj58.pay.aof.exec.callable.util.QueueUtil;
import com.bj58.pay.aof.exec.main.SubOrderHisMerge;
import com.bj58.pay.aof.model.Pairs;
import com.bj58.pay.aof.service.impl.AofDBServiceImpl;
import com.bj58.pay.common.log.PayLogger;
import com.bj58.pay.common.log.PayLoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:yangjing06@58ganji.com">yangjing06</a>
 * @since 16/11/9 下午3:52
 * @version 1.0
 */
public class Producer implements Runnable {
    private PayLogger logger = PayLoggerFactory.getLogger(Producer.class);
    private DaoWrapper           dao;
    private CountDownLatch       countDownLatch;
    private BlockingQueue<Pairs> blockingQueue;
    private AtomicInteger        querySize;

    public Producer(DaoWrapper dao, CountDownLatch countDownLatch, BlockingQueue<Pairs> blockingQueue, AtomicInteger querySize) {
        this.dao = dao;
        this.countDownLatch = countDownLatch;
        this.blockingQueue = blockingQueue;
        this.querySize = querySize;
    }

    @Override public void run() {
        List<Pairs> list;
        Long id = 0L;
        try {
            /*
             * using db index #id to query from db.
             * query every #com.bj58.pay.aof.exec.main.SubOrderHisMerge.LINE to avoid to much data for once.
             */
            while ((list = AofDBServiceImpl.queryOrderByTime(SubOrderHisMerge.January, SubOrderHisMerge.February, dao, SubOrderHisMerge.LINE, id)).size() >= SubOrderHisMerge.LINE) {
                querySize.addAndGet(list.size());
                id = list.get(list.size() - 1).getId();
                QueueUtil.fillQueue(list, blockingQueue);
            }
            if (list.size() > 0) {
                querySize.addAndGet(list.size());
                QueueUtil.fillQueue(list, blockingQueue);
            }
        } catch (Exception e) {
            logger.error("[Producer err] ", e);
        } finally {
            countDownLatch.countDown();
            System.out.println("size " + blockingQueue.size());
        }
    }

}
