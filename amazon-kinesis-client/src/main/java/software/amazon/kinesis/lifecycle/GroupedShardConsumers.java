package software.amazon.kinesis.lifecycle;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;
import software.amazon.kinesis.leases.ShardInfo;

/**
 * Composition-based group of per-shard {@link ShardConsumer} instances.
 *
 * Phase 1 (no buffering): This class is metadata only. Each shard still uses its own ShardConsumer
 * and processing semantics are unchanged. Future phases will add buffering and coalesced emission
 * within this aggregation boundary.
 */
public class GroupedShardConsumers implements ShardGroup {

    private final ConcurrentHashMap<ShardInfo, ShardConsumer> children = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    @Getter
    private final String groupId;

    public GroupedShardConsumers(String groupId) {
        this.groupId = groupId;
    }

    @Override
    public void addShard(ShardInfo shardInfo, ShardConsumer child) {
        if (shutdown.get()) {
            return; // ignore additions after shutdown
        }
        children.putIfAbsent(shardInfo, child);
    }

    @Override
    public void removeShard(ShardInfo shardInfo) {
        children.remove(shardInfo);
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

    // public void createOrGetGroupedShardConsumers() {
    //     // Coalescing enabled: group shards by a stable group id (default: stream identifier)
    //     final StreamIdentifier streamIdentifier = getStreamIdentifier(shardInfo.streamIdentifierSerOpt());
    //     final ShardGroupInfo shardGroupInfo = ShardGroupInfo.from(shardInfo, streamIdentifier);
    //     GroupedShardConsumers group = groupedShardInfoShardConsumersMap.get(shardGroupInfo);
    //     if (group == null) {
    //         final GroupedShardConsumers created = new GroupedShardConsumers(shardGroupInfo.getGroupId());
    //         final GroupedShardConsumers existing =
    //                 groupedShardInfoShardConsumersMap.putIfAbsent(shardGroupInfo, created);
    //         group = existing == null ? created : existing;
    //         if (existing == null) {
    //             slog.infoForce("Created new GroupedShardConsumer metadata group: " + shardGroupInfo.getGroupId());
    //         }
    //     }

    //     // Build a standard per-shard ShardConsumer (no semantic change) and register it in the group and per-shard
    // map
    //     if ((consumer == null)
    //             || (consumer.isShutdown() && consumer.shutdownReason().equals(ShutdownReason.LEASE_LOST))) {
    //         consumer = buildConsumer(shardInfo, shardRecordProcessorFactory, leaseCleanupManager);
    //         shardInfoShardConsumerMap.put(shardInfo, consumer);
    //         slog.infoForce("Created new shardConsumer for : " + shardInfo + " in group " +
    // shardGroupInfo.getGroupId());
    //     }
    //     group.addShard(shardInfo, consumer);
    // }

    // public void cleanupGroupedShardConsumers() {
    //     // Grouped mode: remove shard membership metadata, retain per-shard lifecycle semantics.
    //     shardInfoShardConsumerMap.remove(shard);
    //     final StreamIdentifier streamIdentifier = getStreamIdentifier(shard.streamIdentifierSerOpt());
    //     final ShardGroupInfo shardGroupInfo = ShardGroupInfo.from(shard, streamIdentifier);
    //     final GroupedShardConsumers group = groupedShardInfoShardConsumersMap.get(shardGroupInfo);
    //     if (group != null) {
    //         group.removeShard(shard);
    //         log.info("Removed shard {} from group {}", ShardInfo.getLeaseKey(shard), group.getGroupId());
    //         if (group.shardCount() == 0) {
    //             group.shutdownIfEmpty();
    //             groupedShardInfoShardConsumersMap.remove(shardGroupInfo, group);
    //             log.info("Removed empty group {}", shardGroupInfo.getGroupId());
    //         }
    //     }
    // }

    public void executeGroupedConsumerLifecycle() {
        // TODO: may need special handling here
        //  or modify the existing logic in the ShardConsumer class
        //
//        System.out.println("HARRXF: " + this.groupId);
        for (ShardConsumer shardConsumer : children.values()) {
            shardConsumer.executeLifecycle();
        }
    }
}
