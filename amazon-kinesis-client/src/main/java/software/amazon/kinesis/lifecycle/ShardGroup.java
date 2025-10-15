package software.amazon.kinesis.lifecycle;

import java.util.Collections;
import java.util.Map;

import software.amazon.kinesis.leases.ShardInfo;

/**
 * Internal abstraction representing a logical grouping of shards.
 *
 * Initial phase (no buffering): purely tracks membership so that future
 * coalescing and buffering logic can evolve without changing Scheduler call sites.
 */
interface ShardGroup {
    /** Add a shard with its dedicated child ShardConsumer. */
    void addShard(ShardInfo shardInfo, ShardConsumer child);

    /** Remove a shard from the group. */
    void removeShard(ShardInfo shardInfo);

    /** Number of member shards. */
    int shardCount();

    /** Read-only snapshot of current shards mapped to their child consumers. */
    Map<ShardInfo, ShardConsumer> shardsView();

    /** Whether this group has been shutdown (placeholder for future phases). */
    boolean isShutdown();

    /** Attempt shutdown if empty (no-op placeholder in initial phase). */
    void shutdownIfEmpty();

    /** Utility base implementation for empty immutable map usage. */
    ShardGroup EMPTY = new ShardGroup() {
        @Override
        public void addShard(ShardInfo shardInfo, ShardConsumer child) {}

        @Override
        public void removeShard(ShardInfo shardInfo) {}

        @Override
        public int shardCount() {
            return 0;
        }

        @Override
        public Map<ShardInfo, ShardConsumer> shardsView() {
            return Collections.emptyMap();
        }

        @Override
        public boolean isShutdown() {
            return true;
        }

        @Override
        public void shutdownIfEmpty() {}
    };
}
