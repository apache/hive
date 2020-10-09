package org.apache.hadoop.hive.llap;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestChannelOutputStreamWriteEnd {
    @Test
    public void testRaceConditionsOnVolatile() throws InterruptedException, ExecutionException {
        int taskCount = 20;
        int counterResult;
        int iterations = 0;
        ExecutorService executor = Executors.newFixedThreadPool(taskCount);

        // volatile reference, catch non zero result
        do {
            counterResult = new VolatileCounterTester(executor).run(taskCount);
            iterations++;
        } while (counterResult == 0);
        Assert.assertTrue(counterResult > 0);

        // atomic reference, zero even with 10 times more iterations
        for(int i = 0; i < iterations * 10; i++) {
            counterResult = new AtomicReferenceCounterTester(executor).run(taskCount);
            Assert.assertTrue(counterResult == 0);
        }
    }

    interface TaskCompleteNotify {
        void taskComplete();
    }

    static class VolatileCounterTester {
        private volatile int counter;
        private final Object monitor = new Object();
        private boolean placingTasks;
        private ExecutorService executor;
        private TaskCompleteNotify taskCompleteNotify = new TaskCompleteNotify() {
            @Override
            public void taskComplete() {
                counter--;
            }
        };
        VolatileCounterTester(ExecutorService executor) {
            this.executor = executor;
        }
        int run(final int taskCount) throws InterruptedException, ExecutionException {
            counter = taskCount;
            placingTasks = true;
            List<Future<?>> futures = new ArrayList<>();
            for(int i = 0; i < taskCount; i++) {
                Future<?> future = executor.submit(new Callable<Void>() {
                    public Void call() throws InterruptedException {
                        synchronized (monitor) {
                            while (placingTasks)
                                monitor.wait();
                        }
                        taskCompleteNotify.taskComplete();
                        return null;
                    }
                });
                futures.add(future);
            }
            Assert.assertTrue(counter == taskCount);
            placingTasks = false;
            synchronized (monitor) {
                monitor.notifyAll();
            }
            for(int i = 0; i < taskCount; i++) {
                futures.get(i).get();
            }
            return counter;
        }
    }

    static class AtomicReferenceCounterTester {
        private AtomicInteger counter = new AtomicInteger();
        private final Object monitor = new Object();
        private boolean placingTasks;
        private ExecutorService executor;
        private TaskCompleteNotify taskCompleteNotify = new TaskCompleteNotify() {
            @Override
            public void taskComplete() {
                counter.getAndDecrement();
            }
        };
        AtomicReferenceCounterTester(ExecutorService executor) {
            this.executor = executor;
        }
        int run(final int taskCount) throws InterruptedException, ExecutionException {
            counter.set(taskCount);
            placingTasks = true;
            List<Future<?>> futures = new ArrayList<>();
            for(int i = 0; i < taskCount; i++) {
                Future<?> future = executor.submit(new Callable<Void>() {
                    public Void call() throws InterruptedException {
                        synchronized (monitor) {
                            while (placingTasks)
                                monitor.wait();
                        }
                        taskCompleteNotify.taskComplete();
                        return null;
                    }
                });
                futures.add(future);
            }
            Assert.assertTrue(counter.get() == taskCount);
            placingTasks = false;
            synchronized (monitor) {
                monitor.notifyAll();
            }
            for(int i = 0; i < taskCount; i++) {
                futures.get(i).get();
            }
            return counter.get();
        }
    }

}
