package redis.clients.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 一致性hash算法
 * TreeMap模拟节点环
 * @author lijunqing
 * @param <R>
 * @param <S>
 */
public class Sharded<R, S extends ShardInfo<R>> {

    public static final int DEFAULT_WEIGHT=1;

    private TreeMap<Long, S> nodes; // 虚拟节点

    private final Hashing algo;
    
    //真实节点
    private final Map<ShardInfo<R>, R> resources=new LinkedHashMap<ShardInfo<R>, R>();

    /**
     * The default pattern used for extracting a key tag. 
     * The pattern must have a group (between parenthesis),
     *  which delimits the
     * tag to be hashed. A null pattern avoids applying the regular expression for each lookup, 
     * improving performance a little bit
     * is key tags aren't being used.
     */
    private Pattern tagPattern=null;

    // the tag is anything between {}
    public static final Pattern DEFAULT_KEY_TAG_PATTERN=Pattern.compile("\\{(.+?)\\}");

    public Sharded(List<S> shards) {
        this(shards, Hashing.MURMUR_HASH); // MD5 is really not good as we works
        // with 64-bits not 128
    }

    public Sharded(List<S> shards, Hashing algo) {
        this.algo=algo;
        initialize(shards);
    }

    public Sharded(List<S> shards, Pattern tagPattern) {
        this(shards, Hashing.MURMUR_HASH, tagPattern); // MD5 is really not good
        // as we works with
        // 64-bits not 128
    }

    public Sharded(List<S> shards, Hashing algo, Pattern tagPattern) {
        this.algo=algo;
        this.tagPattern=tagPattern;
        initialize(shards);
    }
    
    /**
     * 初始化一致性hash环 
     * @param shards
     */
    private void initialize(List<S> shards) { 
        nodes=new TreeMap<Long, S>();

        for(int i=0; i != shards.size(); ++i) { //每个真实机器节点都需要关联虚拟节点 
            final S shardInfo=shards.get(i);
            if(shardInfo.getName() == null)
                for(int n=0; n < 160 * shardInfo.getWeight(); n++) { //真实节点关联虚拟节点
                    nodes.put(this.algo.hash("SHARD-" + i + "-NODE-" + n), shardInfo);
                }
            else
                for(int n=0; n < 160 * shardInfo.getWeight(); n++) {
                    nodes.put(this.algo.hash(shardInfo.getName() + "*" + shardInfo.getWeight() + n), shardInfo);
                }
            resources.put(shardInfo, shardInfo.createResource());
        }
    }

    public R getShard(byte[] key) {
        return resources.get(getShardInfo(key));
    }

    public R getShard(String key) {
        return resources.get(getShardInfo(key));
    }

    public S getShardInfo(byte[] key) {
        SortedMap<Long, S> tail=nodes.tailMap(algo.hash(key)); //沿环的顺时针找到一个虚拟节点
        if(tail.isEmpty()) {
            return nodes.get(nodes.firstKey());
        }
        return tail.get(tail.firstKey()); //返回该虚拟节点对应的真实机器节点的信息  
    }

    public S getShardInfo(String key) {
        return getShardInfo(SafeEncoder.encode(getKeyTag(key)));
    }

    /**
     * A key tag is a special pattern inside a key that, if preset, is the only part of the key hashed in order to select the server
     * for this key.
     * @see http://code.google.com/p/redis/wiki/FAQ#I 'm_using_some_form_of_key_hashing_for_partitioning,_but_wh
     * @param key
     * @return The tag if it exists, or the original key
     */
    public String getKeyTag(String key) {
        if(tagPattern != null) {
            Matcher m=tagPattern.matcher(key);
            if(m.find())
                return m.group(1);
        }
        return key;
    }

    public Collection<S> getAllShardInfo() {
        return Collections.unmodifiableCollection(nodes.values());
    }

    public Collection<R> getAllShards() {
        return Collections.unmodifiableCollection(resources.values());
    }
}
