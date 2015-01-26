package mmilewski.parmach.v1;

import com.google.common.collect.UnmodifiableIterator;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.google.common.collect.Iterators.partition;
import static java.lang.Math.min;

/**
 * Consumes stream of items. Reads a batch of items, processes them in parallel, and calls client back
 * with one results one-by-one.
 *
 * Processes 150MB file on 4 cores in 2 seconds (with 20 executor threads), which is a good trade-off I believe.
 *
 * Requires Java 8
 */
public class ParallelMachine<ItemT, ResultT> {
    public enum ActionOnError {
        IGNORE, // ignore this one, and keep processing other tasks
        ABORT_EVERYTHING // abort all tasks and return control to the client
    }

    private Function<ItemT, ResultT> itemProcessor;
    private BiConsumer<ItemT, ResultT> onNextCallback;
    private Function<Throwable, ActionOnError> onErrorCallback;

    private int numOfParallelExecutors;
    private int maxNumOfItemsInBatch;

    private static final int HARD_LIMIT_FOR_NUM_OF_ITEMS_IN_BATCH = 500;

    public ParallelMachine(int numOfParallelExecutors) {
        this.numOfParallelExecutors = numOfParallelExecutors;
        this.maxNumOfItemsInBatch = min(HARD_LIMIT_FOR_NUM_OF_ITEMS_IN_BATCH, numOfParallelExecutors * 5);
    }

    public ParallelMachine setProcessor(Function<ItemT, ResultT> itemProcessor) {
        this.itemProcessor = itemProcessor;
        return this;
    }

    public ParallelMachine setCallbacks(BiConsumer<ItemT, ResultT> onNextCallback,
                                        Function<Throwable, ActionOnError> onErrorCallback) {
        this.onNextCallback = onNextCallback;
        this.onErrorCallback = onErrorCallback;
        return this;
    }

    private class Task implements Callable<TaskResult> {
        private final ItemT item;

        public Task(ItemT item) {
            this.item = item;
        }

        @Override
        public TaskResult call() throws Exception {
            return new TaskResult(item, itemProcessor.apply(item));
        }
    }

    private class TaskResult {
        private final ItemT item;
        private final ResultT result;

        public TaskResult(ItemT item, ResultT result) {
            this.item = item;
            this.result = result;
        }
    }

    public void processIterator(Iterator<ItemT> itemsIt) throws InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(numOfParallelExecutors);
        try {
            // For more info about ExecutorCompletionService see http://java.dzone.com/articles/executorcompletionservice
            ExecutorCompletionService<TaskResult> executor = new ExecutorCompletionService<>(es);
            UnmodifiableIterator<List<ItemT>> batchesIterator = partition(itemsIt, maxNumOfItemsInBatch);
            boolean shouldAbort = false;
            while (batchesIterator.hasNext() && !shouldAbort) {
                shouldAbort = processBatch(executor, batchesIterator.next());
                if (shouldAbort) {
                    es.shutdownNow();
                }
            }
        } finally {
            if (!es.isShutdown()) {
                es.shutdown();
            }
            // no need to await termination because processBatch() waits for all tasks to complete before it returns.
        }
    }

    private boolean processBatch(ExecutorCompletionService<TaskResult> executor, List<ItemT> batchOfLines) throws InterruptedException {
        // submit all items in this batch to executor
        batchOfLines.forEach(item -> executor.submit(new Task(item)));
        // wait for all tasks to execute
        for (ItemT onlyNumberOfItemsMattersSoYouShouldTakeAsManyItemsAsYouSubmitted : batchOfLines) {
            Future<TaskResult> maybeResult = executor.take();
            try {
                TaskResult taskResult = maybeResult.get();
                onNextCallback.accept(taskResult.item, taskResult.result);
            } catch (ExecutionException e) {
                ActionOnError actionOnError = onErrorCallback.apply(e);
                switch (actionOnError) {
                    case IGNORE:
                        break;
                    case ABORT_EVERYTHING:
                        return true;
                }
            }
        }
        return false;
    }
}
