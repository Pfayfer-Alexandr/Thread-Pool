import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// --- Интерфейс CustomExecutor ---
interface CustomExecutor extends Executor {
    void execute(Runnable command);
    <T> Future<T> submit(Callable<T> callable);
    void shutdown();
    void shutdownNow();
}

// --- Обработчик отказов ---
interface RejectedExecutionHandler {
    void rejected(Runnable task, String reason);
}

class DefaultRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejected(Runnable task, String reason) {
        System.out.println("[Rejected] Task " + task + " was rejected due to " + reason + "!");
    }
}

// --- Кастомная фабрика потоков ---
class CustomThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger threadCount = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable r) {
        String threadName = poolName + "-worker-" + threadCount.getAndIncrement();
        System.out.println("[ThreadFactory] Creating new thread: " + threadName);
        Thread t = new Thread(r, threadName);
        t.setUncaughtExceptionHandler((th, ex) -> {
            System.out.println("[ThreadFactory] Thread " + th.getName() + " terminated with exception: " + ex);
        });
        return t;
    }
}

// --- Класс Worker ---
class Worker extends Thread {
    private final BlockingQueue<Runnable> taskQueue;
    private final CustomThreadPool pool;
    private final long keepAliveTimeMillis;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public Worker(BlockingQueue<Runnable> taskQueue, CustomThreadPool pool, long keepAliveTime, TimeUnit unit, String name) {
        super(name);
        this.taskQueue = taskQueue;
        this.pool = pool;
        this.keepAliveTimeMillis = unit.toMillis(keepAliveTime);
    }

    @Override
    public void run() {
        try {
            while (running.get() && !pool.isShutdown()) {
                Runnable task = taskQueue.poll(keepAliveTimeMillis, TimeUnit.MILLISECONDS);
                if (task != null) {
                    System.out.println("[Worker] " + getName() + " executes " + task);
                    try {
                        task.run();
                    } catch (Exception e) {
                        System.out.println("[Worker] " + getName() + " caught exception: " + e);
                    }
                } else {
                    // idle timeout
                    if (pool.shouldTerminateWorker(this)) {
                        System.out.println("[Worker] " + getName() + " idle timeout, stopping.");
                        break;
                    }
                }
            }
        } catch (InterruptedException e) {
            // Thread interrupted, exit
        } finally {
            System.out.println("[Worker] " + getName() + " terminated.");
            pool.onWorkerExit(this);
        }
    }

    public void shutdown() {
        running.set(false);
        this.interrupt();
    }
}

// --- Класс пула потоков ---
class CustomThreadPool implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final int minSpareThreads;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final CustomThreadFactory threadFactory;
    private final RejectedExecutionHandler rejectedHandler;

    private final List<BlockingQueue<Runnable>> taskQueues;
    private final List<Worker> workers;
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger queueIndex = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final Lock workersLock = new ReentrantLock();

    public CustomThreadPool(int corePoolSize, int maxPoolSize, int minSpareThreads, long keepAliveTime, TimeUnit timeUnit, int queueSize, String poolName) {
        this(corePoolSize, maxPoolSize, minSpareThreads, keepAliveTime, timeUnit, queueSize, poolName, new DefaultRejectedExecutionHandler());
    }

    public CustomThreadPool(int corePoolSize, int maxPoolSize, int minSpareThreads, long keepAliveTime, TimeUnit timeUnit, int queueSize, String poolName, RejectedExecutionHandler handler) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.minSpareThreads = minSpareThreads;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.threadFactory = new CustomThreadFactory(poolName);
        this.rejectedHandler = handler;
        this.taskQueues = new ArrayList<>();
        this.workers = new ArrayList<>();
        for (int i = 0; i < maxPoolSize; i++) {
            taskQueues.add(new ArrayBlockingQueue<>(queueSize));
        }
        for (int i = 0; i < corePoolSize; i++) {
            addWorker(i);
        }
    }

    private void addWorker(int queueId) {
        workersLock.lock();
        try {
            if (workers.size() >= maxPoolSize) return;
            Worker worker = new Worker(taskQueues.get(queueId), this, keepAliveTime, timeUnit, threadFactory.newThread(() -> {}).getName());
            workers.add(worker);
            activeThreads.incrementAndGet();
            worker.start();
        } finally {
            workersLock.unlock();
        }
    }

    @Override
    public void execute(Runnable command) {
        submitInternal(command);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> future = new FutureTask<>(callable);
        submitInternal(future);
        return future;
    }

    private void submitInternal(Runnable task) {
        if (isShutdown.get()) {
            rejectedHandler.rejected(task, "shutdown");
            return;
        }
        int idx = queueIndex.getAndIncrement() % workers.size();
        BlockingQueue<Runnable> queue = taskQueues.get(idx);
        boolean offered = queue.offer(task);
        if (offered) {
            System.out.println("[Pool] Task accepted into queue #" + idx + ": " + task);
        } else {
            workersLock.lock();
            try {
                if (workers.size() < maxPoolSize) {
                    addWorker(workers.size());
                    queue = taskQueues.get(workers.size() - 1);
                    if (queue.offer(task)) {
                        System.out.println("[Pool] Task accepted into new queue #" + (workers.size() - 1) + ": " + task);
                        return;
                    }
                }
            } finally {
                workersLock.unlock();
            }
            rejectedHandler.rejected(task, "overload");
        }
        ensureMinSpareThreads();
    }

    private void ensureMinSpareThreads() {
        int idle = getIdleThreads();
        if (idle < minSpareThreads && workers.size() < maxPoolSize) {
            workersLock.lock();
            try {
                int toAdd = Math.min(minSpareThreads - idle, maxPoolSize - workers.size());
                for (int i = 0; i < toAdd; i++) {
                    addWorker(workers.size());
                }
            } finally {
                workersLock.unlock();
            }
        }
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public boolean shouldTerminateWorker(Worker worker) {
        int current = activeThreads.get();
        if (current > corePoolSize) {
            activeThreads.decrementAndGet();
            return true;
        }
        return false;
    }

    public void onWorkerExit(Worker worker) {
        workersLock.lock();
        try {
            workers.remove(worker);
        } finally {
            workersLock.unlock();
        }
    }

    public int getIdleThreads() {
        int idle = 0;
        for (int i = 0; i < workers.size(); i++) {
            if (taskQueues.get(i).isEmpty()) idle++;
        }
        return idle;
    }

    @Override
    public void shutdown() {
        isShutdown.set(true);
        System.out.println("[Pool] Shutdown initiated.");
    }

    @Override
    public void shutdownNow() {
        isShutdown.set(true);
        workersLock.lock();
        try {
            for (Worker w : workers) {
                w.shutdown();
            }
        } finally {
            workersLock.unlock();
        }
        System.out.println("[Pool] ShutdownNow initiated.");
    }
}

// --- Демонстрационная программа ---
public class Main {
    public static void main(String[] args) throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                2, // corePoolSize
                4, // maxPoolSize
                1, // minSpareThreads
                5, // keepAliveTime
                TimeUnit.SECONDS,
                5, // queueSize
                "MyPool"
        );

        // Имитационные задачи
        for (int i = 0; i < 12; i++) {
            final int taskId = i;
            pool.execute(() -> {
                System.out.println("[Task] Task-" + taskId + " started by " + Thread.currentThread().getName());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}
                System.out.println("[Task] Task-" + taskId + " finished by " + Thread.currentThread().getName());
            });
        }

        // Даем время задачам выполниться
        Thread.sleep(15000);

        // Завершаем пул
        pool.shutdown();

        // Ждем завершения всех задач
        Thread.sleep(7000);

        // Демонстрация shutdownNow
        pool.shutdownNow();
    }
}