/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.mongodb.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration class for the MongoDB Connectors.
 */
@Data
@Accessors(chain = true)
public class MongoSourceConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final int DEFAULT_BATCH_SIZE = 100;

	private static final long DEFAULT_BATCH_TIME_MS = 1000;

	private static final boolean DEFAULT_COPY_EXISTING = false;

	private static final String DEFAULT_COPY_EXISTING_NAMESPACE_REGIX = "";

	private static final int DEFAULT_GLOBAL_QUEUE_SIZE = 2000;

	private static final int DEFAULT_COPY_EXISTING_MAX_THREADS = 8;

	private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 3000;

	@FieldDoc(required = true, defaultValue = "", help = "The uri of mongodb that the connector connects to"
			+ " (see: https://docs.mongodb.com/manual/reference/connection-string/)")
	private volatile String mongoUri;

	@FieldDoc(defaultValue = "", help = "The database name to which the collection belongs and which must be watched for the source connector")
	private volatile String database;

	@FieldDoc(defaultValue = "", help = "The collection name where the messages are written or which is watched for the source connector")
	private volatile String collection;

	@FieldDoc(defaultValue = "" + DEFAULT_BATCH_SIZE, help = "The batch size of read from the database")
	private volatile int batchSize = DEFAULT_BATCH_SIZE;

	// @FieldDoc(defaultValue = "" + DEFAULT_BATCH_TIME_MS, help = "The batch
	// operation interval in milliseconds")
	// private volatile long batchTimeMs = DEFAULT_BATCH_TIME_MS;

	@FieldDoc(defaultValue = "" + DEFAULT_COPY_EXISTING, help = "whether or not to copy existing mongo data")
	private volatile boolean copyExisting = DEFAULT_COPY_EXISTING;

	@FieldDoc(defaultValue = ""
			+ DEFAULT_COPY_EXISTING_NAMESPACE_REGIX, help = "the regix used to filter namespace in copy existing document")
	private volatile String copyExistingNamespaceRegex = DEFAULT_COPY_EXISTING_NAMESPACE_REGIX;

	@FieldDoc(defaultValue = ""
			+ DEFAULT_GLOBAL_QUEUE_SIZE, help = "the queue size used for this source to put mongodb document")
	private volatile int globalQueueSize = DEFAULT_GLOBAL_QUEUE_SIZE;

	@FieldDoc(defaultValue = ""
			+ DEFAULT_COPY_EXISTING_MAX_THREADS, help = "thread count used for copy executorService")
	private volatile int copyExistingMaxThreads = DEFAULT_COPY_EXISTING_MAX_THREADS;

	private volatile long heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;

	public static MongoSourceConfig load(String yamlFile) throws IOException {
		final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		final MongoSourceConfig cfg = mapper.readValue(new File(yamlFile), MongoSourceConfig.class);

		return cfg;
	}

	public static MongoSourceConfig load(Map<String, Object> map) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final MongoSourceConfig cfg = mapper.readValue(new ObjectMapper().writeValueAsString(map),
				MongoSourceConfig.class);

		return cfg;
	}

	public void validate(boolean dbRequired, boolean collectionRequired) {
		if (StringUtils.isEmpty(getMongoUri()) || (dbRequired && StringUtils.isEmpty(getDatabase()))
				|| (collectionRequired && StringUtils.isEmpty(getCollection()))) {

			throw new IllegalArgumentException("Required property not set.");
		}

		Preconditions.checkArgument(getBatchSize() > 0, "batchSize must be a positive integer.");
		// Preconditions.checkArgument(getBatchTimeMs() > 0, "batchTimeMs must be a
		// positive long.");
	}
}
