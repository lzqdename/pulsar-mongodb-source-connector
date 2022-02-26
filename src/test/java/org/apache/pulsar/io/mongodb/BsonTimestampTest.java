package org.apache.pulsar.io.mongodb;

import org.bson.BsonTimestamp;

public class BsonTimestampTest {
	public static void main(String[] args) {
		BsonTimestamp ts = new BsonTimestamp(7068871537574543361L);
		System.out.println(ts.getTime());
		System.out.println(ts.getInc());

		//
		for (int count = 10; count >= 0; count--) {
			long ms = System.currentTimeMillis();
			int second = (int) (ms / 1000);
			int incr = (int) (ms % 1000);
			System.out.println(new BsonTimestamp(second, incr).getValue());
		}

	}
}
