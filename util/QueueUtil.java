package com.bj58.pay.aof.exec.callable.util;

import com.bj58.pay.aof.model.Pairs;
import com.bj58.pay.aof.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author <a href="mailto:yangjing06@58ganji.com">yangjing06</a>
 * @since 16/11/9 下午3:55
 * @version 1.0
 */
public class QueueUtil {

    /**
     * cp list from queue.
     * @param list
     * @param blockingQueue
     * @throws InterruptedException
     */
    public static void fillQueue(List<Pairs> list, BlockingQueue<Pairs> blockingQueue) throws InterruptedException {
        if (CollectionUtils.isEmpty(list))
            return;
        for (Pairs pairs : list) {
            blockingQueue.put(pairs);
        }
    }
}
