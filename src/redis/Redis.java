package redis;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Redis操作类
 *
 */
public class Redis {

	protected static final Logger logger = Logger.getLogger(Redis.class);

	private static final String SET_OK = "OK";

	private static final int SETNX_OK = 1;


	public static void expire(final String key, final int expire) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("Redis.expire(String, String) parameters are empty value.");
		}
		Jedis jedis = null;

		try {
			jedis = RedisPool.pool.getResource();
			if (expire > 0) {
				jedis.expire(key, expire);
			}
		} catch (Exception e) {
			logger.error("[设置缓存生效时长方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[设置缓存生效时长]方法中returnResource(jedis)错误", e);
				}
			}
		}

	}

	/**
	 * 添加一个key-value对
	 * @param key
	 * @param value
	 * @param expire 过期时间（秒），0为自动删除、-1永不过期
	 * @return
	 * @throws Exception
	 */
	public static boolean set(final String key, final String value, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == value || value.trim().isEmpty()) {
			throw new IllegalArgumentException("Redis.set(String, String) parameters are empty value.");
		}
		Jedis jedis = null;
		String redisReturn = null;

		try {
			jedis = RedisPool.pool.getResource();
			if (expire > 0) {
				redisReturn = jedis.setex(key, expire, value);
			} else {
				redisReturn = jedis.set(key, value);
			}
		} catch (JedisException je) {
			logger.error("[添加一个key-value对]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[添加一个key-value对]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[添加一个key-value对]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[添加一个key-value对]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return (redisReturn != null && redisReturn.equals(SET_OK)) ? true : false;
	}

	/**
	 * 当key不存在时添加一个key-value对，key存在则不添加
	 * @param key
	 * @param value
	 * @param expire 过期时间（秒），0为自动删除、-1永不过期
	 * @return
	 * @throws Exception
	 */
	public static boolean setNx(final String key, final String value, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == value || value.trim().isEmpty()) {
			throw new IllegalArgumentException("Redis.setNx(String, String, int) parameters are empty value.");
		}
		Jedis jedis = null;
		Long redisReturn = null;

		try {
			jedis = RedisPool.pool.getResource();
			redisReturn = jedis.setnx(key, value);
			if (expire > 0) {
				jedis.expire(key, expire);
			}
		} catch (JedisException je) {
			logger.error("Redis.setNx() 方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("Redis.setNx() 方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("Redis.setNx() 方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("Redis.setNx() 方法中returnResource(jedis)错误", e);
				}
			}
		}

		return (redisReturn != null && redisReturn.longValue() == SETNX_OK) ? true : false;
	}
	
	/**
	 * 获取一个key-value对
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public static String get(final String key) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("Redis.get(String) parameters are empty value.");
		}
		Jedis jedis = null;
		String value = null;

		try {
			jedis = RedisPool.pool.getResource();
			value = jedis.get(key);
		} catch (JedisException je) {
			logger.error("[获取一个key-value对]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[获取一个key-value对]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[获取一个key-value对]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[获取一个key-value对]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return value;
	}


	/**
	 * 根据key删除key-value对
	 * @param keys
	 * @return
	 * @throws Exception
	 */
	public static void del(final String... keys) throws Exception {
		if (null == keys || keys.length == 0) {
			throw new IllegalArgumentException("Redis.del(String) parameters are empty value.");
		}
		Jedis jedis = null;

		try {
			jedis = RedisPool.pool.getResource();
			jedis.del(keys);
		} catch (JedisException je) {
			logger.error("[根据key删除key-value对]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[根据key删除key-value对]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[根据key删除key-value对]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[根据key删除key-value对]方法中returnResource(jedis)错误", e);
				}
			}
		}
	}

	/**
	 * 添加一个key-value对（自定义类型）
	 * @param <T>
	 * @param key: redis key
	 * @param t: redis value 自定义对象
	 * @param expire redis 失效时间
	 * @return
	 * @throws Exception
	 */
	public static <T> boolean setT(final String key, final T t, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == t) {
			throw new IllegalArgumentException("<T> Redis.setT(String, T) parameters are empty value.");
		}
		Jedis jedis = null;
		String redisReturn = null;
		
		try {
			jedis = RedisPool.pool.getResource();
			if (expire > 0) {
				redisReturn = jedis.setex(key.getBytes(), expire, SerializeUtil.serialize(t));
			} else {
				redisReturn = jedis.set(key.getBytes(), SerializeUtil.serialize(t));
			}
		} catch (JedisException je) {
			logger.error("[添加一个key-value对（自定义类型）]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[添加一个key-value对（自定义类型）]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[添加一个key-value对（自定义类型）]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[添加一个key-value对（自定义类型）]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return (redisReturn != null && redisReturn.equals(SET_OK)) ? true : false;
	}

	/**
	 * 获取一个key-value对（自定义类型）
	 * @param <T>
	 * @param key redis key
	 * @return
	 * @throws Exception
	 */
	public static <T> T getT(final String key) throws Exception {
		if (null == key || key.isEmpty()) {
			throw new IllegalArgumentException("<T> Redis.getT(String) parameters are empty value.");
		}

		T t = null;
		Jedis jedis = null;
		try {
			jedis = RedisPool.pool.getResource();
			byte[] buf = jedis.get(key.getBytes());
			if (null == buf || buf.length == 0) {
				return null;
			}
			t = (T) SerializeUtil.unserialize(buf);
		} catch (JedisException je) {
			logger.error("[获取一个key-value对（自定义类型）]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[获取一个key-value对（自定义类型）]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[获取一个key-value对（自定义类型）]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[获取一个key-value对（自定义类型）]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return t;
	}

	/** -----------------------------------------------------------------------------
	 * 将 key 中储存的数字值增一（永不失效）
	 * @param key redis key
	 * @return
	 * @throws Exception
	 */
	public static long incr(String key) throws Exception {
		return incr(key, 0);
	}

	/**
	 * 将 key 中储存的数字值增一
	 * @param key redis key
	 * @param expire redis 失效时间（秒）
	 * @return
	 * @throws Exception
	 */
	public static long incr(String key, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("<T> Redis.incr(String, int) parameters are empty value.");
		}
		Jedis jedis = null;
		long redisReturn;
		
		try {
			jedis = RedisPool.pool.getResource();
			redisReturn = jedis.incr(key);
			if (expire > 0) {
				jedis.expire(key, expire);
			}
		} catch (JedisException je) {
			logger.error("[将 key 中储存的数字值增一]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[将 key 中储存的数字值增一]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[将 key 中储存的数字值增一]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[将 key 中储存的数字值增一]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return redisReturn;
	}

	/** -----------------------------------------------------------------------------
	 * 将 key 中储存的数字值减一（永不失效）
	 * @param key redis key
	 * @return
	 * @throws Exception
	 */
	public static long decr(String key) throws Exception {
		return decr(key, 0);
	}

	/**
	 * 将 key 中储存的数字值减一
	 * @param key redis key
	 * @param expire redis 失效时间（秒）
	 * @return
	 * @throws Exception
	 */
	public static long decr(String key, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("<T> Redis.decr(String, int) parameters are empty value.");
		}
		Jedis jedis = null;
		long redisReturn;
		
		try {
			jedis = RedisPool.pool.getResource();
			redisReturn = jedis.decr(key);
			if (expire > 0) {
				jedis.expire(key, expire);
			}
		} catch (JedisException je) {
			logger.error("[将 key 中储存的数字值减一]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[将 key 中储存的数字值减一]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[将 key 中储存的数字值减一]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[将 key 中储存的数字值减一]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return redisReturn;
	}

	/** List集合 -----------------------------------------------------------------------------
	 * 将arr元素添加到list集合的表尾，如果list集合不存在则创建
	 * @param key: redis key
	 * @param arr: redis value
	 * @param expire: redis 失效时间
	 * @return
	 * @throws Exception
	 */
	public static Long rpush(final String key, final String[] arr, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == arr || arr.length == 0) {
			throw new IllegalArgumentException("Redis.rpush() parameters are empty value.");
		}
		Jedis jedis = null;
		Long redisReturn = null;
		try {
			jedis = RedisPool.pool.getResource();
			if (arr.length == 1) {
				redisReturn = jedis.rpush(key, arr[0]);
				if (expire > 0) {
					redisReturn = jedis.expire(key, expire);
				}
				return redisReturn;
			}

			Transaction tx = jedis.multi();
			for (String s: arr) {
				tx.rpush(key, s);
			}
			List result = tx.exec();
			if (expire > 0) {
				jedis.expire(key, expire);
			}
			return (Long) result.get(result.size() - 1);
		} catch (JedisException je) {
			logger.error("[将arr元素添加到list集合的表尾]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[将arr元素添加到list集合的表尾]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e2) {
					logger.error("[将arr元素添加到list集合的表尾]方法中returnResource(jedis)错误", e2);
				}
			}
		}
	}

	/**
	 * 获取List集合
	 * @param key: redis key
	 * @param start: 获取redis list集合数据的开始索引
	 * @param end: 获取redis list集合数据的结束索引
	 * @return
	 * @throws Exception
	 */
	public static List<String> lrange(final String key, final long start, long end) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("Redis.lrange() parameters are empty value.");
		}
		Jedis jedis = null;
		try {
			jedis = RedisPool.pool.getResource();
			return jedis.lrange(key, start, end);
		} catch (JedisException je) {
			logger.error("[获取List集合]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[获取List集合]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e2) {
					logger.error("[获取List集合]方法中returnResource(jedis)错误", e2);
				}
			}
		}
	}

	/** Set集合 -----------------------------------------------------------------------------
	 * 创建set集合，若集合存在则将元素加入set集合中
	 * @param key: redis key
	 * @param members: redis value
	 * @param expire: redis 失效时间
	 * @return
	 * @throws Exception
	 */
	public static boolean sadd(final String key, final Set<String> members, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == members || members.size() == 0) {
			throw new IllegalArgumentException("Redis.sadd() parameters are empty value.");
		}
		Jedis jedis = null;

		try {
			jedis = RedisPool.pool.getResource();

			Transaction tx = jedis.multi();
			for (String member: members) {
				tx.sadd(key, member);
			}
			if (expire > 0) {
				tx.expire(key, expire);
			}
			List result = tx.exec();
//			return (Long) result.get(result.size() - 1);
		} catch (JedisException je) {
			logger.error("[创建set集合，若集合存在则将元素加入set集合中]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[创建set集合，若集合存在则将元素加入set集合中]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e2) {
					logger.error("[创建set集合，若集合存在则将元素加入set集合中]方法中returnResource(jedis)错误", e2);
				}
			}
		}
	
		return true;
	}

	/**
	 * 创建set集合，若集合存在则将元素加入set集合中（自定义类型）
	 * @param <T>: 自定义类型
	 * @param key: redis key
	 * @param members: redis value（自定义类型）
	 * @param expire: redis 失效时间
	 * @return
	 * @throws Exception
	 */
	public static <T> boolean saddT(final String key, final Set<T> members, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == members || members.size() == 0) {
			throw new IllegalArgumentException("<T> Redis.saddT() parameters are empty value.");
		}
		Jedis jedis = null;

		try {
			jedis = RedisPool.pool.getResource();
			Transaction tx = jedis.multi();
			for (T t : members) {
				tx.sadd(key.getBytes(), SerializeUtil.serialize(t));
			}
			if (expire > 0) {
				tx.expire(key.getBytes(), expire);
			}
			tx.exec();
		} catch (JedisException je) {
			logger.error("[创建set集合，若集合存在则将元素加入set集合中（自定义类型）]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[创建set集合，若集合存在则将元素加入set集合中（自定义类型）]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e2) {
					logger.error("[创建set集合，若集合存在则将元素加入set集合中（自定义类型）]方法中returnResource(jedis)错误", e2);
				}
			}
		}
	
		return true;
	}

	/**
	 * 从set集合中获取全部元素
	 * @param key: redis key
	 * @return
	 * @throws Exception
	 */
	public static Set<String> smembers(final String key) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("Redis.smembers(String) parameters are empty value.");
		}
		Jedis jedis = null;
		Set<String> members = null;

		try {
			jedis = RedisPool.pool.getResource();
			members = jedis.smembers(key);
		} catch (JedisException je) {
			logger.error("[从Set集合中获取全部元素]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[从Set集合中获取全部元素]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[从Set集合中获取全部元素]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return members;
	}

	/**
	 * 获取set集合中全部元素（自定义类型）
	 * @param <T>
	 * @param key: redis key
	 * @return
	 * @throws Exception
	 */
	public static <T> Set<T> smembersT(final String key) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("<T> Set<T> Redis.smembersT(key) parameters are empty value.");
		}
		Jedis jedis = null;
		Set<byte[]> byteSet = null;
		T t = null;
		Set<T> members = null;

		try {
			jedis = RedisPool.pool.getResource();
			byteSet = jedis.smembers(key.getBytes());
			if (byteSet != null && byteSet.size() > 0) {
				members = new HashSet<T>();
				for (byte[] buf : byteSet) {
					t = (T) SerializeUtil.unserialize(buf);
					members.add(t);
				}
			}
		} catch (JedisException je) {
			logger.error("[获取set集合中全部元素（自定义类型）]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[获取set集合中全部元素（自定义类型）]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[获取set集合中全部元素（自定义类型）]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[获取集合key中全部元素（自定义类型）]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return members;
	}

	/** Hash -----------------------------------------------------------------------------
	 * 添加hash类型数据
	 * @param key: redis key
	 * @param hash: redis value
	 * @param expire: redis 失效时间
	 * @return
	 * @throws Exception
	 */
	public static boolean hmset(final String key, final Map<String, String> hash, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == hash || hash.isEmpty()) {
			throw new IllegalArgumentException("Redis.hmset(key, map, expire) parameters are empty value.");
		}
		Jedis jedis = null;

		try {
			jedis = RedisPool.pool.getResource();
			Transaction tx = jedis.multi();
			tx.hmset(key, hash);
			if (expire > 0) {
				tx.expire(key, expire);
			}
			tx.exec();
		} catch (JedisException je) {
			logger.error("[添加hash类型数据]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[添加hash类型数据]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[添加hash类型数据]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[添加hash类型数据]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return true;
	}

	/**
	 * 添加hash类型数据（自定义类型）
	 * @param key: redis key
	 * @param hash: redis hash类型数据的value（自定义类型）
	 * @param expire: redis 失效时间
	 * @return
	 * @throws Exception
	 */
	public static boolean hmsetT(final String key, final Map<byte[], byte[]> hash, int expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == hash || hash.isEmpty()) {
			throw new IllegalArgumentException("boolean Redis.hmsetT(key, map, expire) parameters are empty value.");
		}
		Jedis jedis = null;

		try {
			jedis = RedisPool.pool.getResource();
			Transaction tx = jedis.multi();
			tx.hmset(key.getBytes(), hash);
			if (expire > 0) {
				tx.expire(key, expire);
			}
			tx.exec();
		} catch (JedisException je) {
			logger.error("[添加hash类型数据（自定义类型）]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[添加hash类型数据（自定义类型）]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[添加hash类型数据（自定义类型）]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[添加hash类型数据（自定义类型）]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return true;
	}


	public static List<String> hmget(final String key, final String[] hashKey) throws Exception {
		return hmget(key, hashKey, null);
	}
	/**
	 * 获取hash类型中指定hashKey的value值
	 * @param key: redis key
	 * @param hashKey: redis hash类型数据的key
	 * @return
	 * @throws Exception
	 */
	public static List<String> hmget(final String key, final String[] hashKey, final Integer expire) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == hashKey || hashKey.length == 0) {
			throw new IllegalArgumentException("Redis.hmget(key, hashKey) parameters are empty value.");
		}
		Jedis jedis = null;
		List<String> list = null;

		try {
			jedis = RedisPool.pool.getResource();
			list = jedis.hmget(key, hashKey);
			if (expire != null && expire > 0) {
				jedis.expire(key, expire);
			}
		} catch (JedisException je) {
			logger.error("[获取hash类型中指定hashKey的value值]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[获取hash类型中指定hashKey的value值]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[获取hash类型中指定hashKey的value值]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[获取hash类型中指定hashKey的value值]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return list;
	}

	/**
	 * 获取hash类型中指定hashKey的value值（自定义类型）
	 * @param key: redis key
	 * @param hashKey: redis hash类型数据的key（自定义类型）
	 * @return
	 * @throws Exception
	 */
	public static List<byte[]> hmgetT(final String key, final byte[] hashKey) throws Exception {
		if (null == key || key.trim().isEmpty()
				|| null == hashKey || hashKey.length == 0) {
			throw new IllegalArgumentException("List<byte[]> Redis.hmgetT(key, hashKey) parameters are empty value.");
		}
		Jedis jedis = null;
		List<byte[]> list = null;

		try {
			jedis = RedisPool.pool.getResource();
			list = jedis.hmget(key.getBytes(), hashKey);
		} catch (JedisException je) {
			logger.error("[获取hash类型中指定hashKey的value值（自定义类型）]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[获取hash类型中指定hashKey的value值（自定义类型）]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[获取hash类型中指定hashKey的value值（自定义类型）]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[获取hash类型中指定hashKey的value值（自定义类型）]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return list;
	}

	/**
	 * 获取hash类型中全部hashKey-value对
	 * @param key: redis key
	 * @return
	 * @throws Exception
	 */
	public static Map<String, String> hgetAll(final String key) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("Redis.hgetAll(key) parameters are empty value.");
		}
		Jedis jedis = null;
		Map<String, String> hash = null;

		try {
			jedis = RedisPool.pool.getResource();
			hash = jedis.hgetAll(key);
		} catch (JedisException je) {
			logger.error("[获取hash类型中全部hashKey-value对]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[获取hash类型中全部hashKey-value对]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[获取hash类型中全部hashKey-value对]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[获取hash类型中全部hashKey-value对]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return hash;
	}

	/**
	 * 获取hash类型中全部hashKey-value对（自定义类型）
	 * @param key: redis key
	 * @return
	 * @throws Exception
	 */
	public static Map<byte[], byte[]> hgetAllT(final String key) throws Exception {
		if (null == key || key.trim().isEmpty()) {
			throw new IllegalArgumentException("Map<byte[], byte[]> Redis.hgetAll(key) parameters are empty value.");
		}
		Jedis jedis = null;
		Map<byte[], byte[]> hash = null;

		try {
			jedis = RedisPool.pool.getResource();
			hash = jedis.hgetAll(key.getBytes());
		} catch (JedisException je) {
			logger.error("[获取hash类型中全部hashKey-value对（自定义类型）]方法异常", je);
			try {
				RedisPool.pool.returnBrokenResource(jedis);
			} catch (Exception e1) {
				logger.error("[获取hash类型中全部hashKey-value对（自定义类型）]方法异常中returnBrokenResource(jedis)错误", e1);
			}
			jedis = null;
			throw new Exception(je);
		} catch (Exception e) {
			logger.error("[获取hash类型中全部hashKey-value对（自定义类型）]方法异常", e);
			throw new Exception(e);
		} finally {
			if (jedis != null) {
				try {
					RedisPool.pool.returnResource(jedis);
				} catch (Exception e) {
					logger.error("[获取hash类型中全部hashKey-value对（自定义类型）]方法中returnResource(jedis)错误", e);
				}
			}
		}

		return hash;
	}

	public static void main(String[] args) {

		//27496111119009
		System.out.println(System.nanoTime());
		System.out.println(new Date().getTime());

		/*try {
			incr("key_incr", 0);
		} catch (Exception e) {
			System.out.println(e);
		}*/

	}
}
