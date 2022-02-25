package org.apache.pulsar.io.mongodb.source;

import java.util.Optional;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.mongodb.source.RecordSource;
import org.bson.BsonDocument;

import lombok.Data;

@Data
public class DocRecord implements Record<byte[]> {

	// state
	private volatile boolean acked = false;
	private volatile boolean failed = false;

	// content
	private final Optional<String> key;
	private final byte[] value;
	private final BsonDocument resumeToken;
	private final RecordSource source;

	public DocRecord(Optional<String> k, byte[] v, BsonDocument t, RecordSource s) {
		key = k;
		value = v;
		resumeToken = t;
		source = s;
	}

	@Override
	public void ack() {
		acked = true;
	}

	public void fail() {
		failed = true;
	}

}
