package redis.clients.util;

/**
 * shard信息
 * @author lijunqing
 * @param <T>
 */
public abstract class ShardInfo<T> {

    private int weight;

    public ShardInfo() {
    }

    public ShardInfo(int weight) {
        this.weight=weight;
    }

    public int getWeight() {
        return this.weight;
    }

    protected abstract T createResource();

    public abstract String getName();
}
