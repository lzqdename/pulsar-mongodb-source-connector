package org.apache.pulsar.io.mongodb.source;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

public abstract class MongoPushSource<T> implements Source<T> {

	private volatile LinkedBlockingQueue<Record<T>> dataQueue;
	private volatile LinkedBlockingQueue<Record<T>> offsetQueue;

	public MongoPushSource() {
	}

	public void setDataQueue(LinkedBlockingQueue<Record<T>> q) {
		this.dataQueue = q;
	}

	public void setOffsetQueue(LinkedBlockingQueue<Record<T>> q) {
		this.offsetQueue = q;
	}

	public Record<T> readData(int millis) throws Exception {
		// return queue.take();
		try {
			return dataQueue.poll(millis, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			return null;
		}
	}

	public Record<T> peakOffset() {
		return offsetQueue.peek();
	}

	public Record<T> readOffset() {
		return offsetQueue.poll();
	}

	/**
	 * Open connector with configuration.
	 *
	 * @param config
	 *            initialization config
	 * @param sourceContext
	 *            environment where the source connector is running
	 * @throws Exception
	 *             IO type exceptions when opening a connector
	 */
	abstract public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception;

	/**
	 * Attach a consumer function to this Source. This is invoked by the
	 * implementation to pass messages whenever there is data to be pushed to
	 * Pulsar.
	 *
	 * @param record
	 *            next message from source which should be sent to a Pulsar topic
	 * @throws Exception
	 */
	public void consumeData(Record<T> record) {
		try {
			dataQueue.put(record);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void consumeOffset(Record<T> record) throws Exception {
		offsetQueue.put(record);
	}
}
