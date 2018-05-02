package redis;

import java.util.ResourceBundle;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Jedis连接池封装
 *
 */
public class RedisPool {

	public static JedisPool pool;

	private final static int POOL_TIMEOUT = 2000;

	static {
		ResourceBundle bundle = ResourceBundle.getBundle("redis");
		if (null == bundle) {
			throw new IllegalArgumentException("redis.properties is not found!");
		}
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(Integer.valueOf(bundle.getString("redis.pool.maxActive").trim()).intValue());
		config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle").trim()).intValue());
		config.setMaxWait(Long.valueOf(bundle.getString("redis.pool.maxWait").trim()).longValue());
		config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow").trim()).booleanValue());
		config.setTestOnReturn(Boolean.valueOf(bundle.getString("redis.pool.testOnReturn").trim()).booleanValue());
		String host = bundle.getString("redis.host").trim();
		int port = Integer.valueOf(bundle.getString("redis.port").trim()).intValue();
		boolean isAuth = Boolean.valueOf(bundle.getString("redis.isauth").trim()).booleanValue();
		String auth = null;
		if (isAuth) {
			auth = bundle.getString("redis.auth");
			auth = auth.isEmpty() ? null : auth;
		}
		Integer timeout = Integer.valueOf(bundle.getString("redis.timeout").trim());
		timeout = (null == timeout) ? POOL_TIMEOUT : timeout;

		pool = new JedisPool(config, host, port, timeout, auth);
	}
}
