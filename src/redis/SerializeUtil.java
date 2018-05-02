package redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

/**
 * 序列化与反序列化工具类
 *
 */
public class SerializeUtil {

	protected static final Logger logger = Logger.getLogger(SerializeUtil.class);

	/**
	 * 序列化对象
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public static byte[] serialize(Object obj) throws RuntimeException {
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;
		try {
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(obj);
			byte[] buf = baos.toByteArray();
			return buf;
		} catch (Exception e) {
			logger.error("序列化对象异常", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * 对象反序列化
	 * @param buf
	 * @return
	 * @throws Exception
	 */
	public static Object unserialize(byte[] buf) throws RuntimeException {
		ByteArrayInputStream bais = null;
		try {
			bais = new ByteArrayInputStream(buf);
			ObjectInputStream ois = new ObjectInputStream(bais);
			return ois.readObject();
		} catch (Exception e) {
			logger.error("对象反序列化异常", e);
			throw new RuntimeException(e);
		}
	}
}
