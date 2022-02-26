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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.state.DefaultStateStore;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.mongodb.Utils;
import org.apache.pulsar.io.mongodb.source.MongoPushSource;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;

/**
 * The base class for MongoDB sources.
 */
@Connector(name = "mongo", type = IOType.SOURCE, help = "A source connector that sends mongodb documents to pulsar", configClass = MongoSourceConfig.class)
@Slf4j
public class MongoSource extends MongoPushSource<byte[]> implements Runnable {

	private static final String STATE_KEY = "MONGO_SOURCE_STATE_KEY";
	public static final String ID_FIELD = "_id";

	public Gson gson = new GsonBuilder().create();

	private final Supplier<MongoClient> clientProvider;
	private volatile MongoSourceConfig mongoSourceConfig;

	// fields for state
	private volatile boolean needCopy = false;
	private volatile ChangeStreamDocument<Document> cachedDocument = null;
	private volatile BsonDocument resumeToken = null;

	// used for threads
	private volatile ExecutorService mainManageExecutorService = null;
	private volatile ExecutorService copyExistingExecutorService = null;
	private volatile ExecutorService newestOffsetUpdateExecutorService = null;
	private volatile String newestResumeToken = null;
	private volatile String savedResumeToken = null;
	private volatile ScheduledExecutorService saveResumeTokenscheduledThreadPool = null;
	private volatile DefaultStateStore defaultStateStore = null;

	// fields for exception system
	private volatile Exception latestException = null;
	private volatile boolean running = true;

	public MongoSource() {
		this(null);
	}

	public MongoSource(Supplier<MongoClient> clientProvider) {
		this.clientProvider = clientProvider;
	}

	public void reportException(Exception e) {
		log.error("{}", e);
		latestException = e;
		running = false;
	}

	private void checkException() {
		Preconditions.checkArgument(null == latestException, "error happened , please check the log for more details ");
	}

	private MongoClient createMongoClient() {

		// create client
		MongoClient client = null;
		if (clientProvider != null) {
			client = clientProvider.get();
		} else {
			ConnectionString connectionStr = new ConnectionString(mongoSourceConfig.getMongoUri());
			MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(connectionStr);
			// MongoDriverInformation driverInfo = MongoDriverInformation.builder()
			// .driverName("pulsar-io|mongo-source|driver").build();
			MongoDriverInformation driverInfo = null;
			client = MongoClients.create(builder.build(), driverInfo);
		}

		return client;

	}

	private ChangeStreamIterable<Document> createStream(final MongoClient client) {

		// create change stream
		String database = mongoSourceConfig.getDatabase();
		String collection = mongoSourceConfig.getCollection();
		Optional<List<Document>> pipeline = Optional.empty();
		ChangeStreamIterable<Document> changeStream = null;

		if (!Utils.hasValue(database)) {
			log.info("receive all changes on whole cluster");
			changeStream = pipeline.map(client::watch).orElse(client.watch());
		} else if (!Utils.hasValue(collection)) {
			database = database.trim();
			log.info("receive changes for whole db '{}'", database);
			MongoDatabase db = client.getDatabase(database);
			changeStream = pipeline.map(db::watch).orElse(db.watch());
		} else {
			database = database.trim();
			collection = collection.trim();
			log.info("receive  changes for single collection '{}.{}'", database, collection);
			MongoCollection<Document> coll = client.getDatabase(database).getCollection(collection);
			changeStream = pipeline.map(coll::watch).orElse(coll.watch());
		}

		// optimize the change stream
		int batchSize = mongoSourceConfig.getBatchSize();
		if (batchSize > 0) {
			changeStream.batchSize(batchSize);
		}
		// here,we want full document
		changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);

		return changeStream;
	}

	private MongoChangeStreamCursor<ChangeStreamDocument<Document>> createCursor(
			final ChangeStreamIterable<Document> changeStream, final BsonDocument resumeToken) {

		// use startAfter vs resumeAfter
		if (null != resumeToken) {
			try {
				changeStream.startAfter(resumeToken);
				log.info("succeed to use changeStream.startAfter(resumeToken)");
			} catch (Exception e) {
				log.warn("fail to use startAfter... , so we will try to use resumeAfter... ");
				changeStream.resumeAfter(resumeToken);
				log.info("succeed to use changeStream.resumeAfter(resumeToken)");
			}
		}

		return changeStream.cursor();
	}

	private void initialize(SourceContext sourceContext) throws Exception {

		// try to recover last resume token based on remote state
		String tenant = sourceContext.getTenant();
		String namespace = sourceContext.getNamespace();
		String name = sourceContext.getSourceName();
		defaultStateStore = (DefaultStateStore) sourceContext.getStateStore(tenant, namespace, name);
		final ByteBuffer stateBuf = defaultStateStore.get(STATE_KEY);
		if (null != stateBuf) {
			byte[] stateArray = stateBuf.array();
			if (null != stateArray && stateArray.length > 0) {
				//
				MongoSourceState state = gson.fromJson(new String(stateArray), MongoSourceState.class);
				Preconditions.checkArgument(null != state.getResumeToken(),
						"invalid resume token from remote state store");
				resumeToken = BsonDocument.parse(state.getResumeToken());
				log.info("succeed to recover resume token from remote state store as {}", resumeToken);
			}
		}

		// enter stream sync mode already, no need to copy
		if (null != resumeToken) {
			needCopy = false;
			cachedDocument = null;
		} else {

			needCopy = mongoSourceConfig.isCopyExisting();

			// open mongoclient to get current cachedDocument && resumeToken
			MongoClient client = createMongoClient();
			ChangeStreamIterable<Document> changeStream = this.createStream(client);
			MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = createCursor(changeStream, null);
			log.info("succeed to create cursor");

			// try to get just first document,and this document can be null
			ChangeStreamDocument<Document> firstDocument = cursor.tryNext();
			if (null != firstDocument) {
				this.cachedDocument = firstDocument;
				this.resumeToken = firstDocument.getResumeToken();
			} else {
				// if .tryNext() return null,may be cursor has been closed by the server
				Preconditions.checkArgument(null != cursor.getServerCursor(),
						"The cursor has been closed by the server in initialize function ");
				this.cachedDocument = null;
				this.resumeToken = cursor.getResumeToken();
			}

			// ensure the cursor resource is released
			Utils.closeCloseable(cursor);
			Utils.closeCloseable(client);
			cursor = null;
			client = null;

		}

		log.info("initializd result is as follows - copyExisting {} , cachedDocumet {} , resumeToken {}", this.needCopy,
				this.cachedDocument, this.resumeToken);

	}

	private List<MongoNamespace> listCopyCollections(final MongoClient mongoClient) {

		List<MongoNamespace> result = new ArrayList<MongoNamespace>();

		// filter db
		MongoIterable<String> dbNames = mongoClient.listDatabaseNames();
		for (String db : dbNames) {
			if (db.startsWith("admin") || db.startsWith("config") || db.startsWith("local")) {
				continue;
			}
			List<MongoNamespace> collections = listCopyCollections(mongoClient, db);
			result.addAll(collections);
		}

		return result;

	}

	private List<MongoNamespace> listCopyCollections(final MongoClient mongoClient, final String database) {

		List<MongoNamespace> result = new ArrayList<MongoNamespace>();

		// filter collection
		MongoDatabase db = mongoClient.getDatabase(database);
		MongoIterable<String> collectionNames = db.listCollectionNames();
		for (String collection : collectionNames) {
			if (collection.startsWith("system.")) {
				continue;
			}
			result.add(new MongoNamespace(database, collection));
		}

		return result;
	}

	private List<MongoNamespace> listCopyNamespaces(final MongoClient mongoClient) {

		String database = mongoSourceConfig.getDatabase();
		String collection = mongoSourceConfig.getCollection();

		List<MongoNamespace> namespaces = null;
		if (!Utils.hasValue(database)) {
			namespaces = listCopyCollections(mongoClient);
		} else if (!Utils.hasValue(collection)) {
			namespaces = listCopyCollections(mongoClient, database);
		} else {
			namespaces = singletonList(new MongoNamespace(database, collection));
		}

		String namespacesRegex = mongoSourceConfig.getCopyExistingNamespaceRegex();
		if (Utils.hasValue(namespacesRegex)) {
			Predicate<String> predicate = Pattern.compile(namespacesRegex).asPredicate();
			namespaces = namespaces.stream().filter(s -> predicate.test(s.getFullName())).collect(toList());
		}

		return namespaces;

	}

	private void runCopy() throws Exception {

		log.info("begin to invoke runCopy() ");
		// invoke copy threads if needed
		if (!this.needCopy) {
			return;
		}

		// now,we get all copy namespaces
		final MongoClient client = this.createMongoClient();
		List<MongoNamespace> namespaces = listCopyNamespaces(client);
		log.info("to copy namespaces as follows:size {} , detail {} ", namespaces.size(), namespaces);
		if (null == namespaces || namespaces.size() == 0) {
			Utils.closeCloseable(client);
			return;
		}

		final CountDownLatch latch = new CountDownLatch(namespaces.size());

		// create copy threads
		int threadCount = Math.min(2 * Runtime.getRuntime().availableProcessors(),
				mongoSourceConfig.getCopyExistingMaxThreads());
		threadCount = Math.min(namespaces.size(), threadCount);
		copyExistingExecutorService = Executors.newFixedThreadPool(threadCount);
		// deliver runnable
		AtomicInteger copyThreadId = new AtomicInteger(0);

		for (final MongoNamespace namespace : namespaces) {
			copyExistingExecutorService.submit(new Runnable() {
				@Override
				public void run() {

					Thread.currentThread()
							.setName("pulsar-mongo-source-child-copy-thread-" + copyThreadId.getAndIncrement());

					try {
						MongoCollection<RawBsonDocument> collection = client.getDatabase(namespace.getDatabaseName())
								.getCollection(namespace.getCollectionName(), RawBsonDocument.class);
						collection.aggregate(new ArrayList<>()).forEach(doc -> {

							log.info("receive a copy document {}", doc);

							BsonDocument keyDocument = new BsonDocument(ID_FIELD, doc.get(ID_FIELD));
							String key = keyDocument.toJson();

							// Build a record with the essential information
							final Map<String, Object> recordValue = new HashMap<>();
							recordValue.put("fullDocument", doc.toJson());
							recordValue.put("ns", namespace);
							long ms = System.currentTimeMillis();
							int seconds = (int) (ms / 1000);
							int incr = (int) (ms % 1000);
							recordValue.put("clusterTime", new BsonTimestamp(seconds, incr).getValue());
							recordValue.put("operation", "copy");

							byte[] value = MongoSource.this.gson.toJson(recordValue).getBytes(StandardCharsets.UTF_8);

							// create DocRecord
							DocRecord docRecord = new DocRecord(Optional.of(key), value, null, RecordSource.COPY);

							// put into queue
							MongoSource.this.consumeData(docRecord);

						});

					} catch (Exception e) {
						MongoSource.this.reportException(e);
					} finally {
						latch.countDown();
					}
				}
			});
		}

		// finished
		latch.await();

		// release resources
		copyExistingExecutorService.shutdown();
		Utils.closeCloseable(client);

		// check exception
		log.info("copy threads finished {}", null == latestException ? "successfully" : "exception happened");
		checkException();

	}

	private DocRecord changeStreamDocumentToDocRecord(ChangeStreamDocument<Document> document) throws Exception {

		if (null != document.getOperationType()
				&& OperationType.OTHER.getValue().equals(document.getOperationType().getValue())) {
			throw new Exception("please update the driver to get the actual operation type ");
		}

		// Build a record with the essential information
		String key = "";
		if (null != document.getDocumentKey()) {
			key = document.getDocumentKey().toJson();
		}

		// value
		final Map<String, Object> recordValue = new HashMap<>();
		if (null != document.getFullDocument()) {
			recordValue.put("fullDocument", document.getFullDocument().toJson());
		}
		recordValue.put("ns", document.getNamespace());
		recordValue.put("clusterTime", document.getClusterTime().getValue());
		recordValue.put("operation", document.getOperationType().getValue());

		// extra
		MongoNamespace destinationNamespace = document.getDestinationNamespace();
		if (null != destinationNamespace) {
			recordValue.put("destNamespace", destinationNamespace);
		}
		byte[] value = this.gson.toJson(recordValue).getBytes(StandardCharsets.UTF_8);

		// create record
		DocRecord record = new DocRecord(Optional.of(key), value, document.getResumeToken(), RecordSource.STREAM);
		return record;

	}

	private void runCached() throws Exception {

		if (null == this.cachedDocument) {
			return;
		}

		try {
			// put into queue
			DocRecord record = changeStreamDocumentToDocRecord(this.cachedDocument);
			consumeData(record);
			log.info("handle cache document finished successfully");
		} catch (Exception e) {
			reportException(e);
			log.info("handle cache document finished , exception happened");
			throw e;
		} finally {
			// release resources
			this.cachedDocument = null;
		}

	}

	private void runStream() throws Exception {

		log.info("now , enter run Stream mode ");
		Preconditions.checkArgument(null != this.resumeToken, "NPE - resume token is null in runStream mode");

		MongoClient client = this.createMongoClient();
		ChangeStreamIterable<Document> changeStream = this.createStream(client);
		MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = this.createCursor(changeStream,
				this.resumeToken);

		// work at stream mode
		long lastCheckHeartbeat = 0;

		try {

			while (running) {

				ChangeStreamDocument<Document> document = cursor.tryNext();

				if (null != document) {

					DocRecord record = changeStreamDocumentToDocRecord(document);
					consumeData(record);

				} else {

					// if .tryNext() return null,may be cursor has been closed by the server
					Preconditions.checkArgument(null != cursor.getServerCursor(),
							"The cursor has been closed by the server when handle runStream() ");

					// whether to create heartbeat record
					long currentMs = System.currentTimeMillis();
					if (currentMs - lastCheckHeartbeat >= mongoSourceConfig.getHeartbeatIntervalMs()) {

						lastCheckHeartbeat = currentMs;

						// create heartbeat record
						DocRecord record = new DocRecord(null, null, cursor.getResumeToken(), RecordSource.HEARTBEAT);
						consumeData(record);
						log.info("create a heartbeat record  {}", record.getResumeToken());

					}

					// to reduce cpu cost
					Utils.sleepMillis(1);

				}
			}
		} catch (Exception e) {
			reportException(e);
			throw e;
		} finally {
			// release resources
			Utils.closeCloseable(cursor);
			Utils.closeCloseable(client);
		}

	}

	@Override
	public void run() {

		Thread.currentThread().setName("pulsar-mongo-source-main-manage");

		try {
			runCopy();
			runCached();
			runStream();
		} catch (Exception e) {
			reportException(e);
		} finally {
			log.error("main manage thread exit , error ,please check the log ");
		}

	}

	private void runUpdateOffset() {
		try {

			while (true) {

				DocRecord record = (DocRecord) super.peakOffset();
				if (null == record) {
					// reduce cpu cost
					Utils.sleepMillis(1);
					continue;
				}

				// error happened
				if (record.isFailed()) {
					throw new Exception("fail to handle a record ");
				}

				if (false == record.isAcked() && false == record.isFailed()) {
					// wait for next check
					// reduce cpu cost
					Utils.sleepMillis(1);
					continue;
				}

				// acked
				record = (DocRecord) super.readOffset();
				Preconditions.checkArgument(null != record, "NPE in runOffset() ");

				RecordSource source = record.getSource();
				// copy data no resume token
				if (RecordSource.COPY == source) {
					continue;
				}

				// get resume token && update it !
				BsonDocument resumeToken = record.getResumeToken();
				Preconditions.checkArgument(null != resumeToken, "NPE resume token , check the log please ");
				newestResumeToken = resumeToken.toJson();

			}
		} catch (Exception e) {
			reportException(e);
		}

	}

	private void runStoreOffset() {

		try {

			boolean shouldUpdate = (null == savedResumeToken && null != newestResumeToken);
			boolean shouldUpdate0 = (null != savedResumeToken && null != newestResumeToken
					&& false == savedResumeToken.equals(newestResumeToken));

			if (shouldUpdate || shouldUpdate0) {

				savedResumeToken = newestResumeToken;

				// save
				MongoSourceState state = new MongoSourceState();
				state.setResumeToken(savedResumeToken);

				defaultStateStore.put(STATE_KEY, ByteBuffer.wrap(gson.toJson(state).getBytes()));

				log.info("succeed to store resume token {}", savedResumeToken);
			}

		} catch (Exception e) {
			this.reportException(e);
		}

	}

	@Override
	public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {

		log.info("Open MongoDB Source");

		// generate local source config object
		mongoSourceConfig = MongoSourceConfig.load(config);
		mongoSourceConfig.validate(false, false);

		// prepare global queues
		super.setDataQueue(new LinkedBlockingQueue<Record<byte[]>>(mongoSourceConfig.getGlobalQueueSize()));
		// then prepare offset queue
		super.setOffsetQueue(new LinkedBlockingQueue<Record<byte[]>>(2 * mongoSourceConfig.getGlobalQueueSize()));

		// init current objects
		initialize(sourceContext);

		// will not concurrently execute
		saveResumeTokenscheduledThreadPool = Executors.newSingleThreadScheduledExecutor();
		saveResumeTokenscheduledThreadPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				runStoreOffset();
			}
		}, 0, 3, TimeUnit.SECONDS);

		// then,let us start offset update service
		newestOffsetUpdateExecutorService = Executors.newSingleThreadExecutor();
		newestOffsetUpdateExecutorService.submit(new Runnable() {
			@Override
			public void run() {
				Thread.currentThread().setName("pulsar-mongo-source-main-update-offset");
				MongoSource.this.runUpdateOffset();
			}
		});

		// then,we can start main thread
		mainManageExecutorService = Executors.newSingleThreadExecutor();
		mainManageExecutorService.execute(this);
		log.info("main manage thread starts successfully");

	}

	@Override
	public Record<byte[]> read() throws Exception {

		while (true) {

			// very important, a chance to check global error
			// if there is a exception, this exception will be catched by pulsar io
			// framework
			// then there is a chance to let close function invoked !
			checkException();

			Record<byte[]> record = super.readData(5);
			if (null == record) {
				continue;
			}

			// put into the offset queue orderly
			super.consumeOffset(record);

			// now ,let us handle it
			if (RecordSource.HEARTBEAT == ((DocRecord) record).getSource()) {
				record.ack();
				continue;
			} else {
				return record;
			}

		}

	}

	@Override
	public void close() throws Exception {

		Utils.closeExecutorService(saveResumeTokenscheduledThreadPool);

		// save offset as possible
		runStoreOffset();

		try {
			defaultStateStore.close();
			defaultStateStore = null;
		} catch (Exception e) {
			log.warn("{}", e);
		}

		Utils.closeExecutorService(newestOffsetUpdateExecutorService);

		Utils.closeExecutorService(copyExistingExecutorService);

		Utils.closeExecutorService(mainManageExecutorService);

	}

}