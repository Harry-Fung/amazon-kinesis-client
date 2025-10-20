package software.amazon.kinesis.leases;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import software.amazon.kinesis.common.StreamIdentifier;

/**
 * Identifies a shard group by its groupId.
 *
 * <p>In multi-stream mode, groupId is typically the stream identifier.
 * Multiple shards from the same stream share the same groupId and are
 * grouped together in a {@link software.amazon.kinesis.lifecycle.GroupedShardConsumers}.
 *
 * <p>This class is used as a map key. The actual shard container is
 * {@link software.amazon.kinesis.lifecycle.GroupedShardConsumers}.
 */
@Getter
@EqualsAndHashCode
@ToString
public final class ShardGroupInfo {

    /**
     * Unique identifier for this shard group.
     * Derived from stream identifier for both single-stream and multi-stream modes.
     * All shards with the same groupId belong to the same group.
     */
    @NonNull
    @Getter
    private final String groupId;

    public ShardGroupInfo(@NonNull String groupId) {
        this.groupId = groupId;
    }

    /**
     * Creates ShardGroupInfo by deriving groupId from ShardInfo and StreamIdentifier.
     *
     * <p>Derivation logic:
     * - Multi-stream mode: uses stream identifier from shardInfo if available
     * - Single-stream mode: uses provided streamIdentifier parameter
     * - This ensures all shards from the same stream share the same groupId
     *
     * @param shardInfo the shard to derive group ID from
     * @param streamIdentifier the stream identifier to use as fallback for single-stream mode
     * @return ShardGroupInfo with appropriate groupId
     */
    public static ShardGroupInfo from(@NonNull ShardInfo shardInfo, @NonNull StreamIdentifier streamIdentifier) {
        // Try to get stream identifier from shardInfo first (multi-stream mode)
        // Otherwise use the provided streamIdentifier (single-stream mode)
        String groupId = shardInfo.streamIdentifierSerOpt().orElse(streamIdentifier.serialize());
        return new ShardGroupInfo(groupId);
    }

    public static ShardGroupInfo of(@NonNull String groupId) {
        return new ShardGroupInfo(groupId);
    }
}
