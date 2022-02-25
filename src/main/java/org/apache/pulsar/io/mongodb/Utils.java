package org.apache.pulsar.io.mongodb;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {

	public static void closeCloseable(Closeable closeObj) {

		if (null == closeObj) {
			return;
		}

		try {
			closeObj.close();
		} catch (Exception e) {
			log.error("{}", e);
		}

	}

	public static void closeExecutorService(ExecutorService executorService) {

		if (null == executorService) {
			return;
		}

		try {
			executorService.shutdown();
		} catch (Exception e) {
			log.error("{}", e);
		}
	}

	public static boolean hasValue(String str) {
		return !(StringUtils.isEmpty(str) || StringUtils.isBlank(str));
	}

	// @SuppressWarnings("static-access")
	public static void sleepMillis(final long millis) {
		try {
			Thread.currentThread().sleep(millis);
		} catch (Exception e) {
			log.error("{}", e);
		}
	}

	// public static void sleepNanos(final int nanos) {
	// try {
	// Thread.currentThread().sleep(0, nanos);
	// } catch (Exception e) {
	// log.error("{}", e);
	// }
	// }

}
