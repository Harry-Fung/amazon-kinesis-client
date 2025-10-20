package software.amazon.kinesis.lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

/**
 * Composition-based group of per-shard {@link ShardConsumer} instances.
 *
 * Phase 1 (no buffering): This class is metadata only. Each shard still uses its own ShardConsumer
 * and processing semantics are unchanged. Future phases will add buffering and coalesced emission
 * within this aggregation boundary.
 */
@Slf4j
public class GroupedShardConsumers implements ShardGroup {

    @Getter
    private final ConcurrentHashMap<ShardInfo, ShardConsumer> children = new ConcurrentHashMap<>();

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final AtomicBoolean updating = new AtomicBoolean(false);

    private final ReentrantLock updateLock = new ReentrantLock();

    @Getter
    private final String groupId;

    // private volatile int targetBatchSize = 100;
    // private volatile long maxBufferTimeMs = 1000;
    // private volatile long lastEmitTime = System.currentTimeMillis();

    public GroupedShardConsumers(String groupId) {
        this.groupId = groupId;
    }

    @Override
    public void addShard(ShardInfo shardInfo, ShardConsumer child) {
        updateLock.lock();
        try {
            if (shutdown.get()) return; // ignore additions after shutdown

            updating.set(true);
            // Pause all existing consumers
            children.values().forEach(this::transitionToUpdating);

            children.putIfAbsent(shardInfo, child);

            // Resume all consumers
            children.values().forEach(this::transitionToProcessing);
            updating.set(false);
        } finally {
            updateLock.unlock();
        }
    }

    @Override
    public void removeShard(ShardInfo shardInfo) {
        updateLock.lock();
        try {
            updating.set(true);
            children.values().forEach(this::transitionToUpdating);

            children.remove(shardInfo);

            children.values().forEach(this::transitionToProcessing);
            updating.set(false);
        } finally {
            updateLock.unlock();
        }
    }

    @Override
    public int shardCount() {
        return children.size();
    }

    @Override
    public Map<ShardInfo, ShardConsumer> shardsView() {
        return Collections.unmodifiableMap(children);
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public void shutdownIfEmpty() {
        if (children.isEmpty()) {
            shutdown.compareAndSet(false, true);
        }
    }

    public void executeGroupedConsumerLifecycle() {
        // TODO: may need special handling here
        //  or modify the existing logic in the ShardConsumer class
        //
        for (ShardConsumer shardConsumer : children.values()) {
            shardConsumer.executeLifecycle();
        }
    }

    private void transitionToUpdating(ShardConsumer consumer) {
        // Inject UPDATING state transition
        consumer.pauseForUpdate();
    }

    private void transitionToProcessing(ShardConsumer consumer) {
        consumer.resumeFromUpdate();
    }

    // public void bufferRecords(ShardInfo shardInfo, ProcessRecordsInput input) {
    //     if (updating.get()) {
    //         // Queue during updates
    //         buffer.offer(input);
    //         return;
    //     }

    //     buffer.offer(input);

    //     // Check if we should emit
    //     if (shouldEmit()) {
    //         emitCoalescedBatch();
    //     }
    // }

    // private boolean shouldEmit() {
    //     return buffer.size() >= targetBatchSize ||
    //            (System.currentTimeMillis() - lastEmitTime) >= maxBufferTimeMs;
    // }

    // private void emitCoalescedBatch() {
    //     List<ProcessRecordsInput> batch = new ArrayList<>();
    //     ProcessRecordsInput input;
    //     while ((input = buffer.poll()) != null && batch.size() < targetBatchSize) {
    //         batch.add(input);
    //     }

    //     if (!batch.isEmpty()) {
    //         ProcessRecordsInput coalesced = coalesceInputs(batch);
    //         // Emit to single handler or distribute
    //         lastEmitTime = System.currentTimeMillis();
    //     }
    // }


}
