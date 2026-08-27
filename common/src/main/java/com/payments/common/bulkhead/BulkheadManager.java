package com.payments.common.bulkhead;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BULKHEAD PATTERN — Resource Isolation
 *
 * Named after ship bulkheads: watertight compartments that prevent
 * one flooded section from sinking the whole vessel.
 *
 * Problem: Fraud Detection CPU spike causes thread starvation
 * in the Payment Processing pool → all payments stop.
 *
 * Solution: Each service gets its OWN bounded thread pool.
 * A flood in one compartment cannot starve another.
 *
 *  Pool: PAYMENT_PROCESSING  → 100 threads, 200 queue
 *  Pool: FRAUD_DETECTION     →  50 threads, 100 queue
 *  Pool: BANK_API_CALLS      →  20 threads,  40 queue  (bank is the bottleneck)
 *  Pool: LEDGER_WRITES       →  30 threads,  60 queue
 *  Pool: NOTIFICATIONS       →  10 threads,  50 queue
 */
public class BulkheadManager {

    private final Map<String, BulkheadPool> pools = new ConcurrentHashMap<>();

    // ─── Standard Payment System Pools ───────────────────────────────────────

    public static BulkheadManager createStandard() {
        BulkheadManager mgr = new BulkheadManager();

        mgr.registerPool(BulkheadPool.builder("PAYMENT_PROCESSING")
                .coreThreads(80).maxThreads(100).queueCapacity(200)
                .keepAlive(Duration.ofSeconds(60))
                .build());

        mgr.registerPool(BulkheadPool.builder("FRAUD_DETECTION")
                .coreThreads(40).maxThreads(50).queueCapacity(100)
                .keepAlive(Duration.ofSeconds(30))
                .build());

        mgr.registerPool(BulkheadPool.builder("BANK_API_CALLS")
                .coreThreads(15).maxThreads(20).queueCapacity(40)
                .keepAlive(Duration.ofSeconds(60))
                .build());

        mgr.registerPool(BulkheadPool.builder("LEDGER_WRITES")
                .coreThreads(25).maxThreads(30).queueCapacity(60)
                .keepAlive(Duration.ofSeconds(60))
                .build());

        mgr.registerPool(BulkheadPool.builder("NOTIFICATIONS")
                .coreThreads(8).maxThreads(10).queueCapacity(500)
                .keepAlive(Duration.ofSeconds(30))
                .build());

        return mgr;
    }

    public void registerPool(BulkheadPool pool) {
        pools.put(pool.getName(), pool);
    }

    // ─── Execute in Pool ──────────────────────────────────────────────────────

    public <T> CompletableFuture<T> executeAsync(String poolName,
                                                  Callable<T> task) {
        BulkheadPool pool = requirePool(poolName);
        return pool.submitAsync(task);
    }

    public <T> T executeSync(String poolName, Callable<T> task,
                             Duration timeout) throws Exception {
        BulkheadPool pool = requirePool(poolName);
        return pool.submitSync(task, timeout);
    }

    private BulkheadPool requirePool(String name) {
        BulkheadPool pool = pools.get(name);
        if (pool == null) {
            throw new IllegalArgumentException("Unknown bulkhead pool: " + name);
        }
        return pool;
    }

    public BulkheadMetrics getMetrics(String poolName) {
        return requirePool(poolName).getMetrics();
    }

    public Map<String, BulkheadMetrics> getAllMetrics() {
        Map<String, BulkheadMetrics> result = new ConcurrentHashMap<>();
        pools.forEach((name, pool) -> result.put(name, pool.getMetrics()));
        return result;
    }

    public void shutdown() {
        pools.values().forEach(BulkheadPool::shutdown);
    }

    // ─── Bulkhead Pool ────────────────────────────────────────────────────────

    public static class BulkheadPool {
        private final String               name;
        private final int                  maxThreads;
        private final int                  queueCapacity;
        private final ThreadPoolExecutor   executor;

        // Metrics
        private final AtomicLong  totalSubmitted = new AtomicLong(0);
        private final AtomicLong  totalRejected  = new AtomicLong(0);
        private final AtomicLong  totalCompleted = new AtomicLong(0);
        private final AtomicLong  totalFailed    = new AtomicLong(0);

        private BulkheadPool(String name, int coreThreads, int maxThreads,
                             int queueCapacity, Duration keepAlive) {
            this.name          = name;
            this.maxThreads    = maxThreads;
            this.queueCapacity = queueCapacity;

            // Named thread factory for observability
            ThreadFactory factory = r -> {
                Thread t = new Thread(r, "bulkhead-" + name.toLowerCase() + "-" +
                        System.nanoTime());
                t.setDaemon(true);
                return t;
            };

            // Caller runs policy: if queue full, reject (don't silently discard)
            this.executor = new ThreadPoolExecutor(
                    coreThreads, maxThreads,
                    keepAlive.toMillis(), TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(queueCapacity),
                    factory,
                    new RejectionHandler(name, totalRejected)
            );
        }

        public <T> CompletableFuture<T> submitAsync(Callable<T> task) {
            totalSubmitted.incrementAndGet();
            return CompletableFuture.supplyAsync(() -> {
                try {
                    T result = task.call();
                    totalCompleted.incrementAndGet();
                    return result;
                } catch (Exception e) {
                    totalFailed.incrementAndGet();
                    throw new CompletionException(e);
                }
            }, executor);
        }

        public <T> T submitSync(Callable<T> task, Duration timeout) throws Exception {
            totalSubmitted.incrementAndGet();
            Future<T> future = executor.submit(task);
            try {
                T result = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                totalCompleted.incrementAndGet();
                return result;
            } catch (TimeoutException e) {
                future.cancel(true);
                totalFailed.incrementAndGet();
                throw new BulkheadTimeoutException(
                        String.format("Task in pool '%s' timed out after %s", name, timeout));
            } catch (ExecutionException e) {
                totalFailed.incrementAndGet();
                throw (Exception) e.getCause();
            }
        }

        public BulkheadMetrics getMetrics() {
            return new BulkheadMetrics(
                    name,
                    executor.getActiveCount(),
                    maxThreads,
                    ((LinkedBlockingQueue<?>) executor.getQueue()).size(),
                    queueCapacity,
                    totalSubmitted.get(),
                    totalCompleted.get(),
                    totalFailed.get(),
                    totalRejected.get()
            );
        }

        public void shutdown() {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        public String getName() { return name; }

        // ─── Rejection Handler ────────────────────────────────────────────────

        private static class RejectionHandler implements RejectedExecutionHandler {
            private final String       poolName;
            private final AtomicLong   rejectedCount;

            RejectionHandler(String poolName, AtomicLong rejectedCount) {
                this.poolName     = poolName;
                this.rejectedCount = rejectedCount;
            }

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                rejectedCount.incrementAndGet();
                System.err.printf("[Bulkhead:%s] REJECTED task — pool exhausted " +
                        "(active=%d, queue=%d)%n",
                        poolName, executor.getActiveCount(),
                        executor.getQueue().size());
                throw new BulkheadRejectedException(
                        String.format("Bulkhead '%s' is full (active=%d, queueSize=%d). " +
                                "Rejecting to protect system stability.",
                                poolName, executor.getActiveCount(),
                                executor.getQueue().size()));
            }
        }

        // ─── Builder ──────────────────────────────────────────────────────────

        public static Builder builder(String name) { return new Builder(name); }

        public static class Builder {
            private final String   name;
            private int      coreThreads   = 10;
            private int      maxThreads    = 20;
            private int      queueCapacity = 50;
            private Duration keepAlive     = Duration.ofSeconds(60);

            private Builder(String name) { this.name = name; }

            public Builder coreThreads(int n)    { this.coreThreads = n; return this; }
            public Builder maxThreads(int n)     { this.maxThreads = n; return this; }
            public Builder queueCapacity(int n)  { this.queueCapacity = n; return this; }
            public Builder keepAlive(Duration d) { this.keepAlive = d; return this; }

            public BulkheadPool build() {
                return new BulkheadPool(name, coreThreads, maxThreads,
                        queueCapacity, keepAlive);
            }
        }
    }

    // ─── Metric & Exception Types ─────────────────────────────────────────────

    public record BulkheadMetrics(
            String name,
            int    activeThreads,
            int    maxThreads,
            int    queuedTasks,
            int    queueCapacity,
            long   totalSubmitted,
            long   totalCompleted,
            long   totalFailed,
            long   totalRejected
    ) {
        public double utilizationPct() {
            return maxThreads == 0 ? 0.0 : (double) activeThreads / maxThreads * 100.0;
        }

        public double queueUtilizationPct() {
            return queueCapacity == 0 ? 0.0 : (double) queuedTasks / queueCapacity * 100.0;
        }

        @Override
        public String toString() {
            return String.format(
                    "Bulkhead[%s] threads=%d/%d (%.0f%%) queue=%d/%d (%.0f%%) " +
                            "submitted=%d completed=%d rejected=%d",
                    name, activeThreads, maxThreads, utilizationPct(),
                    queuedTasks, queueCapacity, queueUtilizationPct(),
                    totalSubmitted, totalCompleted, totalRejected);
        }
    }

    public static class BulkheadRejectedException extends RuntimeException {
        public BulkheadRejectedException(String message) { super(message); }
    }

    public static class BulkheadTimeoutException extends RuntimeException {
        public BulkheadTimeoutException(String message) { super(message); }
    }
}
