package redis.clients.jedis;

import org.apache.commons.pool.BasePoolableObjectFactory;

/**
 * PoolableObjectFactory custom impl.
 */
class JedisFactory extends BasePoolableObjectFactory {

    private final String host;

    private final int port;

    private final int timeout;

    private final String password;

    private final int database;

    public JedisFactory(final String host, final int port, final int timeout, final String password, final int database) {
        super();
        this.host=host;
        this.port=port;
        this.timeout=timeout;
        this.password=password;
        this.database=database;
    }
    
    /**
     * 生成对象
     */
    public Object makeObject() throws Exception {
        final Jedis jedis=new Jedis(this.host, this.port, this.timeout);

        jedis.connect();
        if(null != this.password) {
            jedis.auth(this.password);
        }
        if(database != 0) {
            jedis.select(database);
        }

        return jedis;
    }
    
    /**
     * 激活对象
     */
    @Override
    public void activateObject(Object obj) throws Exception {
        if(obj instanceof Jedis) {
            final Jedis jedis=(Jedis)obj;
            if(jedis.getDB() != database) {
                jedis.select(database);
            }
        }
    }
    
    /**
     * 销毁对象
     */
    public void destroyObject(final Object obj) throws Exception {
        if(obj instanceof Jedis) {
            final Jedis jedis=(Jedis)obj;
            if(jedis.isConnected()) {
                try {
                    try {
                        jedis.quit();
                    } catch(Exception e) {
                    }
                    jedis.disconnect();
                } catch(Exception e) {

                }
            }
        }
    }
    
    /**
     * 验证对象
     */
    public boolean validateObject(final Object obj) {
        if(obj instanceof Jedis) {
            final Jedis jedis=(Jedis)obj;
            try {
                return jedis.isConnected() && jedis.ping().equals("PONG");
            } catch(final Exception e) {
                return false;
            }
        } else {
            return false;
        }
    }
}