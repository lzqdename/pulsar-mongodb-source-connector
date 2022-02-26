package org.apache.pulsar.io.mongodb.source;

import lombok.Data;

@Data
public class MongoSourceState {

	private String resumeToken;

}
