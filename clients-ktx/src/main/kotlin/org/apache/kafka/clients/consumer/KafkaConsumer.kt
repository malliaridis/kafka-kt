/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.consumer

import org.apache.kafka.clients.ApiVersions
import org.apache.kafka.clients.ClientUtils.createChannelBuilder
import org.apache.kafka.clients.ClientUtils.parseAndValidateAddresses
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.CommonClientConfigs.metricsReporters
import org.apache.kafka.clients.GroupRebalanceConfig
import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.PollCondition
import org.apache.kafka.clients.consumer.internals.Fetch
import org.apache.kafka.clients.consumer.internals.Fetcher
import org.apache.kafka.clients.consumer.internals.FetcherMetricsRegistry
import org.apache.kafka.clients.consumer.internals.KafkaConsumerMetrics
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.internals.SubscriptionState
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Metric
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.InvalidGroupIdException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.KafkaMetricsContext
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.MetricsContext
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.AppInfoParser.registerAppInfo
import org.apache.kafka.common.utils.AppInfoParser.unregisterAppInfo
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Timer
import org.apache.kafka.common.utils.Utils.closeQuietly
import org.apache.kafka.common.utils.Utils.propsToMap
import org.slf4j.Logger
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.function.BiConsumer
import java.util.regex.Pattern
import kotlin.math.min

/**
 * A client that consumes records from a Kafka cluster.
 *
 * This client transparently handles the failure of Kafka brokers, and transparently adapts as topic
 * partitions it fetches migrate within the cluster. This client also interacts with the broker to
 * allow groups of consumers to load balance consumption using [consumer groups](#consumergroups).
 *
 * The consumer maintains TCP connections to the necessary brokers to fetch data. Failure to close
 * the consumer after use will leak these connections. The consumer is not thread-safe. See
 * [Multi-threaded Processing](#multithreaded) for more details.
 *
 * ### Cross-Version Compatibility
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers
 * may not support certain features. For example, 0.10.0 brokers do not support offsetsForTimes,
 * because this feature was added in version 0.10.1. You will receive an
 * [org.apache.kafka.common.errors.UnsupportedVersionException] when invoking an API that is not
 * available on the running broker version.
 *
 * ### Offsets and Consumer Position
 * Kafka maintains a numerical offset for each record in a partition. This offset acts as a unique
 * identifier of a record within that partition, and also denotes the position of the consumer in
 * the partition. For example, a consumer which is at position 5 has consumed records with offsets 0
 * through 4 and will next receive the record with offset 5. There are actually two notions of
 * position relevant to the user of the consumer:
 *
 * The [position] of the consumer gives the offset of the next record that will be given out. It
 * will be one larger than the highest offset the consumer has seen in that partition. It
 * automatically advances every time the consumer receives messages in a call to [poll].
 *
 * The [committed position][commitSync] is the last offset that has been stored securely. Should the
 * process fail and restart, this is the offset that the consumer will recover to. The consumer can
 * either automatically commit offsets periodically; or it can choose to control this committed
 * position manually by calling one of the commit APIs (e.g. [commitSync] and [commitAsync]).
 *
 * This distinction gives the consumer control over when a record is considered consumed. It is
 * discussed in further detail below.
 *
 * ### [Consumer Groups and Topic Subscriptions](#consumergroups)
 *
 * Kafka uses the concept of *consumer groups* to allow a pool of processes to divide the work of
 * consuming and processing records. These processes can either be running on the same machine or
 * they can be distributed over many machines to provide scalability and fault tolerance for
 * processing. All consumer instances sharing the same `group.id` will be part of the same consumer
 * group.
 *
 * Each consumer in a group can dynamically set the list of topics it wants to subscribe to through
 * one of the [subscribe] APIs. Kafka will deliver each message in the subscribed topics to one
 * process in each consumer group. This is achieved by balancing the partitions between all members
 * in the consumer group so that each partition is assigned to exactly one consumer in the group. So
 * if there is a topic with four partitions, and a consumer group with two processes, each process
 * would consume from two partitions.
 *
 * Membership in a consumer group is maintained dynamically: if a process fails, the partitions
 * assigned to it will be reassigned to other consumers in the same group. Similarly, if a new
 * consumer joins the group, partitions will be moved from existing consumers to the new one. This
 * is known as *rebalancing* the group and is discussed in more detail [below](#failuredetection).
 * Group rebalancing is also used when new partitions are added to one of the subscribed topics or
 * when a new topic matching a [subscribed regex][subscribe] is created. The group will
 * automatically detect the new partitions through periodic metadata refreshes and assign them to
 * members of the group.
 *
 * Conceptually you can think of a consumer group as being a single logical subscriber that happens
 * to be made up of multiple processes. As a multi-subscriber system, Kafka naturally supports
 * having any number of consumer groups for a given topic without duplicating data (additional
 * consumers are actually quite cheap).
 *
 * This is a slight generalization of the functionality that is common in messaging systems. To get
 * semantics similar to a queue in a traditional messaging system all processes would be part of a
 * single consumer group and hence record delivery would be balanced over the group like with a
 * queue. Unlike a traditional messaging system, though, you can have multiple such groups. To get
 * semantics similar to pub-sub in a traditional messaging system each process would have its own
 * consumer group, so each process would subscribe to all the records published to the topic.
 *
 * In addition, when group reassignment happens automatically, consumers can be notified through a
 * [ConsumerRebalanceListener], which allows them to finish necessary application-level logic such
 * as state cleanup, manual offset commits, etc. See
 * [Storing Offsets Outside Kafka](#rebalancecallback) for more details.
 *
 * It is also possible for the consumer to [manually assign](#manualassignment) specific partitions
 * (similar to the older "simple" consumer) using [assign]. In this case, dynamic partition
 * assignment and consumer group coordination will be disabled.
 *
 * ### [Detecting Consumer Failures](#failuredetection)
 *
 * After subscribing to a set of topics, the consumer will automatically join the group when [poll]
 * is invoked. The poll API is designed to ensure consumer liveness. As long as you continue to call
 * poll, the consumer will stay in the group and continue to receive messages from the partitions it
 * was assigned. Underneath the covers, the consumer sends periodic heartbeats to the server. If the
 * consumer crashes or is unable to send heartbeats for a duration of `session.timeout.ms`, then the
 * consumer will be considered dead and its partitions will be reassigned.
 *
 * It is also possible that the consumer could encounter a "livelock" situation where it is
 * continuing to send heartbeats, but no progress is being made. To prevent the consumer from
 * holding onto its partitions indefinitely in this case, we provide a liveness detection mechanism
 * using the `max.poll.interval.ms` setting. Basically if you don't call poll at least as frequently
 * as the configured max interval, then the client will proactively leave the group so that another
 * consumer can take over its partitions. When this happens, you may see an offset commit failure
 * (as indicated by a [CommitFailedException] thrown from a call to [commitSync]). This is a safety
 * mechanism which guarantees that only active members of the group are able to commit offsets. So
 * to stay in the group, you must continue to call poll.
 *
 * The consumer provides two configuration settings to control the behavior of the poll loop:
 *
 * 1. `max.poll.interval.ms`: By increasing the interval between expected polls, you can give the
 *    consumer more time to handle a batch of records returned from [poll]. The drawback is that
 *    increasing this value may delay a group rebalance since the consumer will only join the
 *    rebalance inside the call to poll. You can use this setting to bound the time to finish a
 *    rebalance, but you risk slower progress if the consumer cannot actually call [poll] often
 *    enough.
 * 2. `max.poll.records`: Use this setting to limit the total records returned from a single call to
 *    poll. This can make it easier to predict the maximum that must be handled within each poll
 *    interval. By tuning this value, you may be able to reduce the poll interval, which will reduce
 *    the impact of group rebalancing.
 *
 * For use cases where message processing time varies unpredictably, neither of these options may be
 * sufficient. The recommended way to handle these cases is to move message processing to another
 * thread, which allows the consumer to continue calling [poll] while the processor is still
 * working. Some care must be taken to ensure that committed offsets do not get ahead of the actual
 * position. Typically, you must disable automatic commits and manually commit processed offsets for
 * records only after the thread has finished handling them (depending on the delivery semantics you
 * need). Note also that you will need to [pause] the partition so that no new records are
 * received from poll until after thread has finished handling those previously returned.
 *
 * ### Usage Examples
 * The consumer APIs offer flexibility to cover a variety of consumption use cases. Here are some
 * examples to demonstrate how to use them.
 *
 * #### Automatic Offset Committing
 * This example demonstrates a simple usage of Kafka's consumer api that relies on automatic offset
 * committing.
 *
 * ```java
 * Properties props = new Properties();
 * props.setProperty("bootstrap.servers", "localhost:9092");
 * props.setProperty("group.id", "test");
 * props.setProperty("enable.auto.commit", "true");
 * props.setProperty("auto.commit.interval.ms", "1000");
 * props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 * props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 * KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 * consumer.subscribe(Arrays.asList("foo", "bar"));
 * while (true) {
 *   ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *   for (ConsumerRecord&lt;String, String&gt; record : records)
 *     System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
 * }
 * ```
 *
 * The connection to the cluster is bootstrapped by specifying a list of one or more brokers to
 * contact using the configuration `bootstrap.servers`. This list is just used to discover the rest
 * of the brokers in the cluster and need not be an exhaustive list of servers in the cluster
 * (though you may want to specify more than one in case there are servers down when the client is
 * connecting).
 *
 * Setting `enable.auto.commit` means that offsets are committed automatically with a frequency
 * controlled by the config `auto.commit.interval.ms`.
 *
 * In this example the consumer is subscribing to the topics *foo* and *bar* as part of a group of
 * consumers called *test* as configured with `group.id`.
 *
 * The deserializer settings specify how to turn bytes into objects. For example, by specifying
 * string deserializers, we are saying that our record's key and value will just be simple strings.
 *
 * #### Manual Offset Control
 *
 * Instead of relying on the consumer to periodically commit consumed offsets, users can also
 * control when records should be considered as consumed and hence commit their offsets. This is
 * useful when the consumption of the messages is coupled with some processing logic and hence a
 * message should not be considered as consumed until it is completed processing.
 *
 * ```java
 * Properties props = new Properties();
 * props.setProperty("bootstrap.servers", "localhost:9092");
 * props.setProperty("group.id", "test");
 * props.setProperty("enable.auto.commit", "false");
 * props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 * props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 * KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 * consumer.subscribe(Arrays.asList("foo", "bar"));
 * final int minBatchSize = 200;
 * List&lt;ConsumerRecord&lt;String, String&gt;&gt; buffer = new ArrayList&lt;&gt;();
 * while (true) {
 *   ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *   for (ConsumerRecord&lt;String, String&gt; record : records) {
 *     buffer.add(record);
 *   }
 *   if (buffer.size() &gt;= minBatchSize) {
 *     insertIntoDb(buffer);
 *     consumer.commitSync();
 *     buffer.clear();
 *   }
 * }
 * ```
 *
 * In this example we will consume a batch of records and batch them up in memory. When we have
 * enough records batched, we will insert them into a database. If we allowed offsets to auto commit
 * as in the previous example, records would be considered consumed after they were returned to the
 * user in [poll]. It would then be possible for our process to fail after batching the records, but
 * before they had been inserted into the database.
 *
 * To avoid this, we will manually commit the offsets only after the corresponding records have been
 * inserted into the database. This gives us exact control of when a record is considered consumed.
 * This raises the opposite possibility: the process could fail in the interval after the insert
 * into the database but before the commit (even though this would likely just be a few
 * milliseconds, it is a possibility). In this case the process that took over consumption would
 * consume from last committed offset and would repeat the insert of the last batch of data. Used in
 * this way Kafka provides what is often called "at-least-once" delivery guarantees, as each record
 * will likely be delivered one time but in failure cases could be duplicated.
 *
 * **Note: Using automatic offset commits can also give you "at-least-once" delivery, but the
 * requirement is that you must consume all data returned from each call to [poll] before any
 * subsequent calls, or before [closing][close] the consumer. If you fail to do either of these, it
 * is possible for the committed offset to get ahead of the consumed position, which results in
 * missing records. The advantage of using manual offset control is that you have direct control
 * over when a record is considered "consumed."**
 *
 * The above example uses [commitSync] to mark all received records as committed. In some cases you
 * may wish to have even finer control over which records have been committed by specifying an
 * offset explicitly. In the example below we commit offset after we finish handling the records in
 * each partition.
 *
 * ```java
 * try {
 *   while(running) {
 *     ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
 *     for (TopicPartition partition : records.partitions()) {
 *       List&lt;ConsumerRecord&lt;String, String&gt;&gt; partitionRecords = records.records(partition);
 *         for (ConsumerRecord&lt;String, String&gt; record : partitionRecords) {
 *           System.out.println(record.offset() + ": " + record.value());
 *         }
 *       long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
 *       consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
 *     }
 *   }
 * } finally {
 *   consumer.close();
 * }
 * ```
 *
 * **Note: The committed offset should always be the offset of the next message that your
 * application will read.** Thus, when calling [commitSync(offsets)][commitSync] you should add one
 * to the offset of the last message processed.
 *
 * #### [Manual Partition Assignment](#manualassignment)
 *
 * In the previous examples, we subscribed to the topics we were interested in and let Kafka
 * dynamically assign a fair share of the partitions for those topics based on the active consumers
 * in the group. However, in some cases you may need finer control over the specific partitions that
 * are assigned. For example:
 *
 * - If the process is maintaining some kind of local state associated with that partition (like a
 *   local on-disk key-value store), then it should only get records for the partition it is
 *   maintaining on disk.
 * - If the process itself is highly available and will be restarted if it fails (perhaps using a
 *   cluster management framework like YARN, Mesos, or AWS facilities, or as part of a stream
 *   processing framework). In this case there is no need for Kafka to detect the failure and
 *   reassign the partition since the consuming process will be restarted on another machine.
 *
 * To use this mode, instead of subscribing to the topic using [subscribe], you just call [assign]
 * with the full list of partitions that you want to consume.
 *
 * ```java
 * String topic = "foo";
 * TopicPartition partition0 = new TopicPartition(topic, 0);
 * TopicPartition partition1 = new TopicPartition(topic, 1);
 * consumer.assign(Arrays.asList(partition0, partition1));
 * ```
 *
 * Once assigned, you can call [poll][poll] in a loop, just as in the preceding examples to consume
 * records. The group that the consumer specifies is still used for committing offsets, but now the
 * set of partitions will only change with another call to [assign]. Manual partition assignment
 * does not use group coordination, so consumer failures will not cause assigned partitions to be
 * rebalanced. Each consumer acts independently even if it shares a groupId with another consumer.
 * To avoid offset commit conflicts, you should usually ensure that the groupId is unique for each
 * consumer instance.
 *
 * Note that it isn't possible to mix manual partition assignment (i.e. using [assign]) with dynamic
 * partition assignment through topic subscription (i.e. using [subscribe]).
 *
 * #### [Storing Offsets Outside Kafka](#rebalancecallback)
 *
 * The consumer application need not use Kafka's built-in offset storage, it can store offsets in a
 * store of its own choosing. The primary use case for this is allowing the application to store
 * both the offset and the results of the consumption in the same system in a way that both the
 * results and offsets are stored atomically. This is not always possible, but when it is it will
 * make the consumption fully atomic and give "exactly once" semantics that are stronger than the
 * default "at-least once" semantics you get with Kafka's offset commit functionality.
 *
 * Here are a couple of examples of this type of usage:
 *
 * - If the results of the consumption are being stored in a relational database, storing the offset
 *   in the database as well can allow committing both the results and offset in a single
 *   transaction. Thus either the transaction will succeed and the offset will be updated based on
 *   what was consumed or the result will not be stored and the offset won't be updated.
 * - If the results are being stored in a local store it may be possible to store the offset there
 *   as well. For example a search index could be built by subscribing to a particular partition and
 *   storing both the offset and the indexed data together. If this is done in a way that is atomic,
 *   it is often possible to have it be the case that even if a crash occurs that causes unsync'd
 *   data to be lost, whatever is left has the corresponding offset stored as well. This means that
 *   in this case the indexing process that comes back having lost recent updates just resumes
 *   indexing from what it has ensuring that no updates are lost.
 *
 * Each record comes with its own offset, so to manage your own offset you just need to do the
 * following:
 *
 * - Configure `enable.auto.commit=false`
 * - Use the offset provided with each [ConsumerRecord] to save your position.
 * - On restart restore the position of the consumer using [seek].
 *
 * This type of usage is simplest when the partition assignment is also done manually (this would be
 * likely in the search index use case described above). If the partition assignment is done
 * automatically special care is needed to handle the case where partition assignments change. This
 * can be done by providing a [ConsumerRebalanceListener] instance in the call to [subscribe] and
 * [subscribe].
 *
 * For example, when partitions are taken from a consumer the consumer will want to commit its
 * offset for those partitions by implementing [ConsumerRebalanceListener.onPartitionsRevoked]. When
 * partitions are assigned to a consumer, the consumer will want to look up the offset for those new
 * partitions and correctly initialize the consumer to that position by implementing
 * [ConsumerRebalanceListener.onPartitionsAssigned].
 *
 * Another common use for [ConsumerRebalanceListener] is to flush any caches the application
 * maintains for partitions that are moved elsewhere.
 *
 * #### Controlling The Consumer's Position
 *
 * In most use cases the consumer will simply consume records from beginning to end, periodically
 * committing its position (either automatically or manually). However Kafka allows the consumer to
 * manually control its position, moving forward or backwards in a partition at will. This means a
 * consumer can re-consume older records, or skip to the most recent records without actually
 * consuming the intermediate records.
 *
 * There are several instances where manually controlling the consumer's position can be useful.
 *
 * One case is for time-sensitive record processing it may make sense for a consumer that falls far
 * enough behind to not attempt to catch up processing all records, but rather just skip to the most
 * recent records.
 *
 * Another use case is for a system that maintains local state as described in the previous section.
 * In such a system the consumer will want to initialize its position on start-up to whatever is
 * contained in the local store. Likewise if the local state is destroyed (say because the disk is
 * lost) the state may be recreated on a new machine by re-consuming all the data and recreating the
 * state (assuming that Kafka is retaining sufficient history).
 *
 * Kafka allows specifying the position using [seek] to specify the new position. Special methods
 * for seeking to the earliest and latest offset the server maintains are also available (
 * [seekToBeginning] and [seekToEnd] respectively).
 *
 * #### Consumption Flow Control
 *
 * If a consumer is assigned multiple partitions to fetch data from, it will try to consume from all
 * of them at the same time, effectively giving these partitions the same priority for consumption.
 * However in some cases consumers may want to first focus on fetching from some subset of the
 * assigned partitions at full speed, and only start fetching other partitions when these partitions
 * have few or no data to consume.
 *
 * One of such cases is stream processing, where processor fetches from two topics and performs the
 * join on these two streams. When one of the topics is long lagging behind the other, the processor
 * would like to pause fetching from the ahead topic in order to get the lagging stream to catch up.
 * Another example is bootstraping upon consumer starting up where there are a lot of history data
 * to catch up, the applications usually want to get the latest data on some of the topics before
 * consider fetching other topics.
 *
 * Kafka supports dynamic controlling of consumption flows by using [pause] and [resume] to pause
 * the consumption on the specified assigned partitions and resume the consumption on the specified
 * paused partitions respectively in the future [poll] calls.
 *
 * ### Reading Transactional Messages
 *
 * Transactions were introduced in Kafka 0.11.0 wherein applications can write to multiple topics
 * and partitions atomically. In order for this to work, consumers reading from these partitions
 * should be configured to only read committed data. This can be achieved by setting the
 * `isolation.level=read_committed` in the consumer's configuration.
 *
 * In `read_committed` mode, the consumer will read only those transactional messages which have
 * been successfully committed. It will continue to read non-transactional messages as before. There
 * is no client-side buffering in `read_committed` mode. Instead, the end offset of a partition for
 * a `read_committed` consumer would be the offset of the first message in the partition belonging
 * to an open transaction. This offset is known as the 'Last Stable Offset'(LSO).
 *
 * A `read_committed` consumer will only read up to the LSO and filter out any transactional
 * messages which have been aborted. The LSO also affects the behavior of [seekToEnd] and
 * [endOffsets] for `read_committed` consumers, details of which are in each method's documentation.
 * Finally, the fetch lag metrics are also adjusted to be relative to the LSO for `read_committed`
 * consumers.
 *
 * Partitions with transactional messages will include commit or abort markers which indicate the
 * result of a transaction. There markers are not returned to applications, yet have an offset in
 * the log. As a result, applications reading from topics with transactional messages will see gaps
 * in the consumed offsets. These missing messages would be the transaction markers, and they are
 * filtered out for consumers in both isolation levels. Additionally, applications using
 * `read_committed` consumers may also see gaps due to aborted transactions, since those messages
 * would not be returned by the consumer and yet would have valid offsets.
 *
 * ### [Multi-threaded Processing](#multithreaded)
 *
 * The Kafka consumer is NOT thread-safe. All network I/O happens in the thread of the application
 * making the call. It is the responsibility of the user to ensure that multi-threaded access is
 * properly synchronized. Un-synchronized access will result in [ConcurrentModificationException].
 *
 * The only exception to this rule is [wakeup], which can safely be used from an external thread to
 * interrupt an active operation. In this case, a [org.apache.kafka.common.errors.WakeupException]
 * will be thrown from the thread blocking on the operation. This can be used to shutdown the
 * consumer from another thread. The following snippet shows the typical pattern:
 *
 * ```java
 * public class KafkaConsumerRunner implements Runnable {
 *   private final AtomicBoolean closed = new AtomicBoolean(false);
 *   private final KafkaConsumer consumer;
 *
 *   public KafkaConsumerRunner(KafkaConsumer consumer) {
 *     this.consumer = consumer;
 *   }
 *
 *   @Override
 *   public void run() {
 *     try {
 *       consumer.subscribe(Arrays.asList("topic"));
 *       while (!closed.get()) {
 *         ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
 *         // Handle new records
 *       }
 *     } catch (WakeupException e) {
 *       // Ignore exception if closing
 *       if (!closed.get()) throw e;
 *     } finally {
 *       consumer.close();
 *     }
 *   }
 *
 *   // Shutdown hook which can be called from a separate thread
 *   public void shutdown() {
 *     closed.set(true);
 *     consumer.wakeup();
 *   }
 * }
 * ```
 *
 * Then in a separate thread, the consumer can be shutdown by setting the closed flag and waking up
 * the consumer.
 *
 * ```java
 * closed.set(true);
 * consumer.wakeup();
 * ```
 *
 * Note that while it is possible to use thread interrupts instead of [wakeup] to abort a blocking
 * operation (in which case, [InterruptException] will be raised), we discourage their use since
 * they may cause a clean shutdown of the consumer to be aborted. Interrupts are mainly supported
 * for those cases where using [wakeup] is impossible, e.g. when a consumer thread is managed by
 * code that is unaware of the Kafka client.
 *
 * We have intentionally avoided implementing a particular threading model for processing. This
 * leaves several options for implementing multi-threaded processing of records.
 *
 * #### 1. One Consumer Per Thread
 *
 * A simple option is to give each thread its own consumer instance. Here are the pros and cons of
 * this approach:
 *
 * - **PRO**: It is the easiest to implement
 * - **PRO**: It is often the fastest as no inter-thread co-ordination is needed
 * - **PRO**: It makes in-order processing on a per-partition basis very easy to implement (each
 *   thread just processes messages in the order it receives them).
 * - **CON**: More consumers means more TCP connections to the cluster (one per thread). In general
 *   Kafka handles connections very efficiently so this is generally a small cost.
 * - **CON**: Multiple consumers means more requests being sent to the server and slightly less
 *   batching of data which can cause some drop in I/O throughput.
 * - **CON**: The number of total threads across all processes will be limited by the total number
 *   of partitions.
 *
 * #### 2. Decouple Consumption and Processing
 *
 * Another alternative is to have one or more consumer threads that do all data consumption and
 * hands off [ConsumerRecords] instances to a blocking queue consumed by a pool of processor threads
 * that actually handle the record processing.
 *
 * This option likewise has pros and cons:
 *
 * - **PRO**: This option allows independently scaling the number of consumers and processors. This
 *   makes it possible to have a single consumer that feeds many processor threads, avoiding any
 *   limitation on partitions.
 * - **CON**: Guaranteeing order across the processors requires particular care as the threads will
 *   execute independently an earlier chunk of data may actually be processed after a later chunk of
 *   data just due to the luck of thread execution timing. For processing that has no ordering
 *   requirements this is not a problem.
 * - **CON**: Manually committing the position becomes harder as it requires that all threads
 *   co-ordinate to ensure that processing is complete for that partition.
 *
 * There are many possible variations on this approach. For example each processor thread can have
 * its own queue, and the consumer threads can hash into these queues using the TopicPartition to
 * ensure in-order consumption and simplify commit.
 */
class KafkaConsumer<K, V> : Consumer<K, V> {

    // Visible for testing
    val metrics: Metrics

    val kafkaConsumerMetrics: KafkaConsumerMetrics

    private lateinit var log: Logger

    private val clientId: String?

    private val groupId: String?

    private val coordinator: ConsumerCoordinator?

    private val keyDeserializer: Deserializer<K>

    private val valueDeserializer: Deserializer<V>

    private val fetcher: Fetcher<K, V>

    private val interceptors: ConsumerInterceptors<K, V>

    private val isolationLevel: IsolationLevel

    private val time: Time

    private val client: ConsumerNetworkClient

    private val subscriptions: SubscriptionState

    private val metadata: ConsumerMetadata

    private val retryBackoffMs: Long

    private val requestTimeoutMs: Long

    private val defaultApiTimeoutMs: Int

    @Volatile
    private var closed = false

    private var assignors: List<ConsumerPartitionAssignor>? = null

    // currentThread holds the threadId of the current thread accessing KafkaConsumer
    // and is used to prevent multi-threaded access
    private val currentThread = AtomicLong(NO_CURRENT_THREAD)

    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private val refcount = AtomicInteger(0)

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata
    // updates
    private var cachedSubscriptionHasAllFetchPositions = false

    /**
     * A consumer is instantiated by providing a [java.util.Properties] object as configuration, and
     * a key and a value [Deserializer].
     *
     * Valid configuration strings are documented at [ConsumerConfig].
     *
     * Note: after creating a `KafkaConsumer` you must always [close] it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements [Deserializer]. The
     * configure() method won't be called in the consumer when the deserializer is passed in
     * directly.
     * @param valueDeserializer The deserializer for value that implements [Deserializer]. The
     * configure() method won't be called in the consumer when the deserializer is passed in
     * directly.
     */
    constructor(
        properties: Properties,
        keyDeserializer: Deserializer<K>? = null,
        valueDeserializer: Deserializer<V>? = null,
    ) : this(
        propsToMap(properties),
        keyDeserializer,
        valueDeserializer,
    )

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and
     * optional a key and a value [Deserializer]. Valid configuration strings are documented
     * [here](http://kafka.apache.org/documentation.html#consumerconfigs). Values can be either
     * strings or objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     *
     * A consumer is instantiated by providing a set of key-value pairs as configuration.
     *
     * Valid configuration strings are documented at [ConsumerConfig].
     *
     * Note: after creating a `KafkaConsumer` you must always [close] it to avoid resource leaks.
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements [Deserializer]. The
     * configure() method won't be called in the consumer when the deserializer is passed in
     * directly.
     * @param valueDeserializer The deserializer for value that implements [Deserializer]. The
     * configure() method won't be called in the consumer when the deserializer is passed in
     * directly.
     */
    constructor(
        configs: Map<String, Any?>,
        keyDeserializer: Deserializer<K>? = null,
        valueDeserializer: Deserializer<V>? = null,
    ) : this(
        config = ConsumerConfig(
            ConsumerConfig.appendDeserializerToConfig(
                configs = configs,
                keyDeserializer = keyDeserializer,
                valueDeserializer = valueDeserializer,
            )
        ),
        keyDeserializer = keyDeserializer,
        valueDeserializer = valueDeserializer,
    )

    internal constructor(
        config: ConsumerConfig,
        keyDeserializer: Deserializer<K>?,
        valueDeserializer: Deserializer<V>?,
    ) {
        try {
            val groupRebalanceConfig = GroupRebalanceConfig(
                config = config,
                protocolType = GroupRebalanceConfig.ProtocolType.CONSUMER,
            )
            groupId = groupRebalanceConfig.groupId
            clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG)!!

            // If group.instance.id is set, we will append it to the log context.
            val logContext = LogContext(
                if (groupRebalanceConfig.groupInstanceId != null)
                    "[Consumer instanceId=${groupRebalanceConfig.groupInstanceId}, " +
                            "clientId=$clientId, groupId=$groupId] "
                else "[Consumer clientId=$clientId, groupId=$groupId] "
            )
            log = logContext.logger(javaClass)

            val enableAutoCommit = config.maybeOverrideEnableAutoCommit()
            groupId?.let { groupIdStr ->
                if (groupIdStr.isEmpty()) log.warn(
                    "Support for using the empty group id by consumers is deprecated and will be " +
                            "removed in the next major release."
                )
            }
            log.debug("Initializing the Kafka consumer")
            requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!.toLong()
            defaultApiTimeoutMs = (config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG))!!
            time = Time.SYSTEM
            metrics = buildMetrics(config, time, clientId)
            retryBackoffMs = (config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG))!!

            val interceptorList = config.getConfiguredInstances(
                key = ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                t = ConsumerInterceptor::class.java,
                configOverrides = mapOf(ConsumerConfig.CLIENT_ID_CONFIG to clientId)
            ) as List<ConsumerInterceptor<K, V>>

            interceptors = ConsumerInterceptors(interceptorList)

            if (keyDeserializer == null) {
                this.keyDeserializer = config.getConfiguredInstance(
                    key = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    t = Deserializer::class.java,
                ) as Deserializer<K>

                this.keyDeserializer.configure(
                    configs = config.originals(mapOf(ConsumerConfig.CLIENT_ID_CONFIG to clientId)),
                    isKey = true,
                )
            } else {
                config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                this.keyDeserializer = keyDeserializer
            }
            if (valueDeserializer == null) {
                this.valueDeserializer = config.getConfiguredInstance(
                    key = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    t = Deserializer::class.java,
                ) as Deserializer<V>
                this.valueDeserializer.configure(
                    configs = config.originals(mapOf(ConsumerConfig.CLIENT_ID_CONFIG to clientId)),
                    isKey = false,
                )
            } else {
                config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                this.valueDeserializer = valueDeserializer
            }
            val offsetResetStrategy = OffsetResetStrategy.valueOf(
                config.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)!!.uppercase()
            )
            subscriptions = SubscriptionState(logContext, offsetResetStrategy)
            val clusterResourceListeners = configureClusterResourceListeners(
                keyDeserializer = keyDeserializer,
                valueDeserializer = valueDeserializer,
                metrics.reporters,
                interceptorList,
            )
            metadata = ConsumerMetadata(
                refreshBackoffMs = retryBackoffMs,
                metadataExpireMs = config.getLong(ConsumerConfig.METADATA_MAX_AGE_CONFIG)!!,
                includeInternalTopics =
                !config.getBoolean(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG)!!,
                allowAutoTopicCreation =
                config.getBoolean(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG)!!,
                subscription = subscriptions,
                logContext = logContext,
                clusterResourceListeners = clusterResourceListeners,
            )
            val addresses = parseAndValidateAddresses(
                urls = config.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)!!,
                clientDnsLookupConfig = config.getString(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG)!!,
            )
            metadata.bootstrap(addresses)
            val metricGrpPrefix = "consumer"
            val metricsRegistry = FetcherMetricsRegistry(
                tags = setOf(CLIENT_ID_METRIC_TAG),
                metricGrpPrefix = metricGrpPrefix,
            )
            val channelBuilder = createChannelBuilder(config, time, logContext)
            isolationLevel = IsolationLevel.valueOf(
                config.getString(ConsumerConfig.ISOLATION_LEVEL_CONFIG)!!.uppercase()
            )
            val throttleTimeSensor = Fetcher.throttleTimeSensor(metrics, metricsRegistry)
            val heartbeatIntervalMs = (config.getInt(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG))!!
            val apiVersions = ApiVersions()
            val netClient = NetworkClient(
                selector = Selector(
                    connectionMaxIdleMs =
                    config.getLong(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG)!!,
                    metrics = metrics,
                    time = time,
                    metricGrpPrefix = metricGrpPrefix,
                    channelBuilder = channelBuilder,
                    logContext = logContext,
                ),
                metadata = metadata,
                clientId = clientId,
                // a fixed large enough value will suffice for max in-flight requests
                maxInFlightRequestsPerConnection = 100,
                reconnectBackoffMs = config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG)!!,
                reconnectBackoffMax =
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)!!,
                socketSendBuffer = config.getInt(ConsumerConfig.SEND_BUFFER_CONFIG)!!,
                socketReceiveBuffer = config.getInt(ConsumerConfig.RECEIVE_BUFFER_CONFIG)!!,
                defaultRequestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!,
                connectionSetupTimeoutMs =
                config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)!!,
                connectionSetupTimeoutMaxMs =
                config.getLong(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)!!,
                time = time,
                discoverBrokerVersions = true,
                apiVersions = apiVersions,
                throttleTimeSensor = throttleTimeSensor,
                logContext = logContext,
            )
            client = ConsumerNetworkClient(
                logContext = logContext,
                client = netClient,
                metadata = metadata,
                time = time,
                retryBackoffMs = retryBackoffMs,
                requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)!!,
                maxPollTimeoutMs = heartbeatIntervalMs,
            ) //Will avoid blocking an extended period of time to prevent heartbeat thread starvation
            assignors = ConsumerPartitionAssignor.getAssignorInstances(
                assignorClasses =
                config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)!!,
                configs = config.originals(mapOf(ConsumerConfig.CLIENT_ID_CONFIG to clientId)),
            )

            // no coordinator will be constructed for the default (null) group id
            if (groupId == null) {
                config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG)
                config.ignore(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED)
                coordinator = null
            } else {
                coordinator = ConsumerCoordinator(
                    rebalanceConfig = groupRebalanceConfig,
                    logContext = logContext,
                    client = client,
                    assignors = assignors!!,
                    metadata = metadata,
                    subscriptions = subscriptions,
                    metrics = metrics,
                    metricGrpPrefix = metricGrpPrefix,
                    time = time,
                    autoCommitEnabled = enableAutoCommit,
                    autoCommitIntervalMs =
                    config.getInt(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG)!!,
                    interceptors = interceptors,
                    throwOnFetchStableOffsetsUnsupported =
                    config.getBoolean(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED)!!,
                    rackId = config.getString(ConsumerConfig.CLIENT_RACK_CONFIG),
                )
            }
            fetcher = Fetcher(
                logContext = logContext,
                client = client,
                minBytes = (config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG))!!,
                maxBytes = (config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG))!!,
                maxWaitMs = (config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG))!!,
                fetchSize = (config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG))!!,
                maxPollRecords = (config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG))!!,
                checkCrcs = (config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG))!!,
                clientRackId = (config.getString(ConsumerConfig.CLIENT_RACK_CONFIG))!!,
                keyDeserializer = this.keyDeserializer,
                valueDeserializer = this.valueDeserializer,
                metadata = metadata,
                subscriptions = subscriptions,
                metrics = metrics,
                metricsRegistry = metricsRegistry,
                time = time,
                retryBackoffMs = retryBackoffMs,
                requestTimeoutMs = requestTimeoutMs,
                isolationLevel = isolationLevel,
                apiVersions = apiVersions,
            )
            kafkaConsumerMetrics = KafkaConsumerMetrics(metrics, metricGrpPrefix)
            config.logUnused()
            registerAppInfo(
                prefix = JMX_PREFIX,
                id = clientId,
                metrics = metrics,
                nowMs = time.milliseconds(),
            )
            log.debug("Kafka consumer initialized")
        } catch (t: Throwable) {
            // call close methods if internal objects are already constructed; this is to prevent
            // resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` was not initialized, which means no
            // internal objects were initialized.
            if (::log.isInitialized) close(
                timeoutMs = 0,
                swallowException = true
            )

            // now propagate the exception
            throw KafkaException("Failed to construct kafka consumer", t)
        }
    }

    // visible for testing
    internal constructor(
        logContext: LogContext,
        clientId: String?,
        coordinator: ConsumerCoordinator?,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>,
        fetcher: Fetcher<K, V>,
        interceptors: ConsumerInterceptors<K, V>,
        time: Time,
        client: ConsumerNetworkClient,
        metrics: Metrics,
        subscriptions: SubscriptionState,
        metadata: ConsumerMetadata,
        retryBackoffMs: Long,
        requestTimeoutMs: Long,
        defaultApiTimeoutMs: Int,
        assignors: List<ConsumerPartitionAssignor>?,
        groupId: String?,
    ) {
        log = logContext.logger(javaClass)
        this.clientId = clientId
        this.coordinator = coordinator
        this.keyDeserializer = keyDeserializer
        this.valueDeserializer = valueDeserializer
        this.fetcher = fetcher
        isolationLevel = IsolationLevel.READ_UNCOMMITTED
        this.interceptors = Objects.requireNonNull(interceptors)
        this.time = time
        this.client = client
        this.metrics = metrics
        this.subscriptions = subscriptions
        this.metadata = metadata
        this.retryBackoffMs = retryBackoffMs
        this.requestTimeoutMs = requestTimeoutMs
        this.defaultApiTimeoutMs = defaultApiTimeoutMs
        this.assignors = assignors
        this.groupId = groupId
        kafkaConsumerMetrics = KafkaConsumerMetrics(
            metrics = metrics,
            metricGrpPrefix = "consumer",
        )
    }

    /**
     * Get the set of partitions currently assigned to this consumer. If subscription happened by
     * directly assigning partitions using [assign] then this will simply return the same partitions
     * that were assigned. If topic subscription was used, then this will give the set of topic
     * partitions currently assigned to the consumer (which may be none if the assignment hasn't
     * happened yet, or the partitions are in the process of getting reassigned).
     *
     * @return The set of partitions currently assigned to this consumer
     */
    override fun assignment(): Set<TopicPartition> {
        acquireAndEnsureOpen()
        try {
            return subscriptions.assignedPartitions().toSet()
        } finally {
            release()
        }
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * [subscribe], or an empty set if no such call has been made.
     *
     * @return The set of topics currently subscribed to
     */
    override fun subscription(): Set<String> {
        acquireAndEnsureOpen()
        try {
            return Collections.unmodifiableSet(HashSet(subscriptions.subscription()))
        } finally {
            release()
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions. **Topic
     * subscriptions are not incremental. This list will replace the current assignment (if there is
     * one).** Note that it is not possible to combine topic subscription with group management with
     * manual partition assignment through [assign].
     *
     * If the given list of topics is empty, it is treated the same as [unsubscribe].
     *
     * As part of group management, the consumer will keep track of the list of consumers that
     * belong to a particular group and will trigger a rebalance operation if any one of the
     * following events are triggered:
     *
     * - Number of partitions change for any of the subscribed topics
     * - A subscribed topic is created or deleted
     * - An existing member of the consumer group is shutdown or fails
     * - A new member is added to the consumer group
     *
     * When any of these events are triggered, the provided listener will be invoked first to
     * indicate that the consumer's assignment has been revoked, and then again when the new
     * assignment has been received. Note that rebalances will only occur during an active call to
     * [poll], so callbacks will also only be invoked during that time.
     *
     * The provided listener will immediately override any listener set in a previous call to
     * subscribe. It is guaranteed, however, that the partitions revoked/assigned through this
     * interface are from topics subscribed in this call. See [ConsumerRebalanceListener] for more
     * details.
     *
     * @param topics The list of topics to subscribe to
     * @param callback Non-null listener instance to get notifications on partition
     * assignment/revocation for the subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements, or if
     * listener is null
     * @throws IllegalStateException If `subscribe()` is called previously with pattern, or assign
     * is called previously (without a subsequent call to [unsubscribe]), or if not configured
     * at-least one partition assignment strategy
     */
    override fun subscribe(topics: Collection<String>, callback: ConsumerRebalanceListener) {
        acquireAndEnsureOpen()
        try {
            maybeThrowInvalidGroupIdException()
            if (topics.isEmpty()) {
                // treat subscribing to empty topic list as the same as unsubscribing
                unsubscribe()
            } else {
                for (topic in topics) {
                    require(topic.isNotBlank()) {
                        "Topic collection to subscribe to cannot contain null or empty topic"
                    }
                }
                throwIfNoAssignorsConfigured()
                fetcher.clearBufferedDataForUnassignedTopics(topics)
                log.info("Subscribed to topic(s): {}", topics.joinToString(", "))

                if (subscriptions.subscribe(topics.toSet(), callback))
                    metadata.requestUpdateForNewTopics()
            }
        } finally {
            release()
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions. **Topic
     * subscriptions are not incremental. This list will replace the current assignment (if there is
     * one).** It is not possible to combine topic subscription with group management with manual
     * partition assignment through [assign].
     *
     * If the given list of topics is empty, it is treated the same as [unsubscribe].
     *
     * This is a short-hand for [subscribe], which uses a no-op listener. If you need the ability to
     * seek to particular offsets, you should prefer [subscribe], since group rebalances will cause
     * partition offsets to be reset. You should also provide your own listener if you are doing
     * your own offset management since the listener gives you an opportunity to commit offsets
     * before a rebalance finishes.
     *
     * @param topics The list of topics to subscribe to
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     * @throws IllegalStateException If `subscribe()` is called previously with pattern, or assign
     * is called previously (without a subsequent call to [unsubscribe]), or if not configured
     * at-least one partition assignment strategy
     */
    override fun subscribe(topics: Collection<String>) = subscribe(
        topics = topics,
        callback = NoOpConsumerRebalanceListener(),
    )

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics existing at the time of
     * check. This can be controlled through the `metadata.max.age.ms` configuration: by lowering
     * the max metadata age, the consumer will refresh metadata more often and check for matching
     * topics.
     *
     * See [subscribe] for details on the use of the [ConsumerRebalanceListener]. Generally
     * rebalances are triggered when there is a change to the topics matching the provided pattern
     * and when consumer group membership changes. Group rebalances only take place during an active
     * call to [poll].
     *
     * @param pattern Pattern to subscribe to
     * @param callback Non-null listener instance to get notifications on partition
     * assignment/revocation for the subscribed topics
     * @throws IllegalArgumentException If pattern or listener is null
     * @throws IllegalStateException If `subscribe()` is called previously with topics, or assign is
     * called previously (without a subsequent call to [unsubscribe]), or if not configured at-least
     * one partition assignment strategy
     */
    override fun subscribe(pattern: Pattern, callback: ConsumerRebalanceListener) {
        maybeThrowInvalidGroupIdException()
        require(pattern.toString().isNotEmpty()) {
            "Topic pattern to subscribe to cannot be empty"
        }
        acquireAndEnsureOpen()

        try {
            throwIfNoAssignorsConfigured()
            log.info("Subscribed to pattern: '{}'", pattern)
            subscriptions.subscribe(pattern, callback)
            coordinator!!.updatePatternSubscription(metadata.fetch())
            metadata.requestUpdateForNewTopics()
        } finally {
            release()
        }
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against topics existing at the time of check.
     *
     * This is a short-hand for [subscribe], which uses a no-op listener. If you need the ability to
     * seek to particular offsets, you should prefer [subscribe], since group rebalances will cause
     * partition offsets to be reset. You should also provide your own listener if you are doing
     * your own offset management since the listener gives you an opportunity to commit offsets
     * before a rebalance finishes.
     *
     * @param pattern Pattern to subscribe to
     * @throws IllegalArgumentException If pattern is null
     * @throws IllegalStateException If `subscribe()` is called previously with topics, or assign is
     * called previously (without a subsequent call to [unsubscribe]), or if not configured at-least
     * one partition assignment strategy
     */
    override fun subscribe(pattern: Pattern) = subscribe(
        pattern = pattern,
        callback = NoOpConsumerRebalanceListener(),
    )

    /**
     * Unsubscribe from topics currently subscribed with [subscribe] or [subscribe]. This also
     * clears any partitions directly assigned through [assign].
     *
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g.
     * rebalance callback errors)
     */
    override fun unsubscribe() {
        acquireAndEnsureOpen()
        try {
            fetcher.clearBufferedDataForUnassignedPartitions(emptySet())
            if (coordinator != null) {
                coordinator.onLeavePrepare()
                coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics")
            }
            subscriptions.unsubscribe()
            log.info("Unsubscribed all topics or patterns and assigned partitions")
        } finally {
            release()
        }
    }

    /**
     * Manually assign a list of partitions to this consumer. This interface does not allow for
     * incremental assignment and will replace the previous assignment (if there is one).
     *
     * If the given list of topic partitions is empty, it is treated the same as [unsubscribe].
     *
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership
     * or cluster and topic metadata change. Note that it is not possible to use both manual
     * partition assignment with [assign] and group assignment with [subscribe].
     *
     * If auto-commit is enabled, an async commit (based on the old assignment) will be triggered
     * before the new assignment replaces the old one.
     *
     * @param partitions The list of partitions to assign this consumer
     * @throws IllegalArgumentException If partitions is null or contains null or empty topics
     * @throws IllegalStateException If `subscribe()` is called previously with topics or pattern
     * (without a subsequent call to [unsubscribe])
     */
    override fun assign(partitions: Collection<TopicPartition>) {
        acquireAndEnsureOpen()
        try {
            if (partitions.isEmpty()) unsubscribe()
            else {
                for (tp: TopicPartition? in partitions) {
                    require(!tp?.topic.isNullOrBlank()) {
                        "Topic partitions to assign to cannot have null or empty topic"
                    }
                }
                fetcher.clearBufferedDataForUnassignedPartitions(partitions)

                // make sure the offsets of topic partitions the consumer is unsubscribing from
                // are committed since there will be no following rebalance
                coordinator?.maybeAutoCommitOffsetsAsync(time.milliseconds())
                log.info("Assigned to partition(s): {}", partitions.joinToString(", "))

                if (subscriptions.assignFromUser(HashSet(partitions)))
                    metadata.requestUpdateForNewTopics()
            }
        } finally {
            release()
        }
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It
     * is an error to not have subscribed to any topics or partitions before polling for data.
     *
     * On each poll, consumer will try to use the last consumed offset as the starting offset and
     * fetch sequentially. The last consumed offset can be manually set through [seek] or
     * automatically set as the last committed offset for the subscribed list of partitions
     *
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available in
     * the buffer. If 0, returns immediately with any records that are available currently in the
     * buffer, else returns empty. Must not be negative.
     * @return map of topic to records since the last fetch for the subscribed list of topics and
     * partitions
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a
     * partition or set of partitions is undefined or out of range and no offset reset policy has
     * been configured
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to
     * any of the subscribed topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g.
     * invalid groupId or session timeout, errors deserializing key/value pairs, or any new error
     * cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or
     * manually assigned any partitions to consume from
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    @Deprecated("")
    override fun poll(timeout: Long): ConsumerRecords<K?, V?> = poll(
        timer = time.timer(timeout),
        includeMetadataInTimeout = false,
    )

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It
     * is an error to not have subscribed to any topics or partitions before polling for data.
     *
     * On each poll, consumer will try to use the last consumed offset as the starting offset and
     * fetch sequentially. The last consumed offset can be manually set through [seek] or
     * automatically set as the last committed offset for the subscribed list of partitions.
     *
     * This method returns immediately if there are records available or if the position advances
     * past control records or aborted transactions when isolation.level=read_committed. Otherwise,
     * it will await the passed timeout. If the timeout expires, an empty record set will be
     * returned. Note that this method may block beyond the timeout in order to execute custom
     * [ConsumerRebalanceListener] callbacks.
     *
     * @param timeout The maximum time to block (must not be greater than [Long.MAX_VALUE]
     * milliseconds)
     * @return map of topic to records since the last fetch for the subscribed list of topics and
     * partitions
     * @throws InvalidOffsetException if the offset for a partition or set of partitions is
     * undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws InterruptException if the calling thread is interrupted before or while this function
     * is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to
     * any of the subscribed topics or to the configured groupId. See the exception for more details
     * @throws KafkaException for any other unrecoverable errors (e.g. invalid groupId or session
     * timeout, errors deserializing key/value pairs, your rebalance callback thrown exceptions, or
     * any new error cases in future versions)
     * @throws IllegalArgumentException if the timeout value is negative
     * @throws IllegalStateException if the consumer is not subscribed to any topics or manually
     * assigned any partitions to consume from
     * @throws ArithmeticException if the timeout is greater than [Long.MAX_VALUE]
     * milliseconds.
     * @throws org.apache.kafka.common.errors.InvalidTopicException if the current subscription
     * contains any invalid topic (per [org.apache.kafka.common.internals.Topic.validate])
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts
     * to fetch stable offsets when the broker doesn't support this feature
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    override fun poll(timeout: Duration): ConsumerRecords<K?, V?> = poll(
        timer = time.timer(timeout),
        includeMetadataInTimeout = true,
    )

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private fun poll(timer: Timer, includeMetadataInTimeout: Boolean): ConsumerRecords<K?, V?> {
        acquireAndEnsureOpen()
        try {
            kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs)
            check(!subscriptions.hasNoSubscriptionOrUserAssignment()) {
                "Consumer is not subscribed to any topics or assigned any partitions"
            }
            do {
                client.maybeTriggerWakeup()
                if (includeMetadataInTimeout) {
                    // try to update assignment metadata BUT do not need to block on the timer for
                    // join group
                    updateAssignmentMetadataIfNeeded(
                        timer = timer,
                        waitForJoinGroup = false,
                    )
                } else while (
                    !updateAssignmentMetadataIfNeeded(
                        timer = time.timer(Long.MAX_VALUE),
                        waitForJoinGroup = true,
                    )
                ) log.warn("Still waiting for metadata")

                val fetch: Fetch<K?, V?> = pollForFetches(timer)
                if (!fetch.isEmpty) {
                    // before returning the fetched records, we can send off the next round of
                    // fetches and avoid block waiting for their responses to enable pipelining
                    // while the user is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched
                    // records.
                    if (fetcher.sendFetches() > 0 || client.hasPendingRequests())
                        client.transmitSends()

                    if (fetch.records().isEmpty()) log.trace(
                        "Returning empty records from `poll()` since the consumer's position has " +
                                "advanced for at least one topic partition"
                    )

                    return interceptors.onConsume(ConsumerRecords(fetch.records()))
                }
            } while (timer.isNotExpired)
            return ConsumerRecords.empty()
        } finally {
            release()
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs)
        }
    }

    fun updateAssignmentMetadataIfNeeded(timer: Timer, waitForJoinGroup: Boolean = true): Boolean {
        return if (coordinator != null && !coordinator.poll(timer, waitForJoinGroup)) false
        else updateFetchPositions(timer)
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private fun pollForFetches(timer: Timer): Fetch<K?, V?> {
        var pollTimeout = if (coordinator == null) timer.remainingMs
        else min(coordinator.timeToNextPoll(timer.currentTimeMs), timer.remainingMs)

        // if data is available already, return it immediately
        val fetch: Fetch<K?, V?> = fetcher.collectFetch()
        if (!fetch.isEmpty) return fetch

        // send any new fetches (won't resend pending fetches)
        fetcher.sendFetches()

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs
        }
        log.trace("Polling for fetches with timeout {}", pollTimeout)
        val pollTimer: Timer = time.timer(pollTimeout)
        client.poll(pollTimer, { !fetcher.hasAvailableFetches() })
        timer.update(pollTimer.currentTimeMs)
        return fetcher.collectFetch()
    }

    /**
     * Commit offsets returned on the last [poll] for all the subscribed list of topics and
     * partitions.
     *
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the
     * first fetch after every rebalance and also on startup. As such, if you need to store offsets
     * in anything other than Kafka, this API should not be used.
     *
     * This is a synchronous commit and will block until either the commit succeeds, an
     * unrecoverable error is encountered (in which case it is thrown to the caller), or the timeout
     * specified by `default.api.timeout.ms` expires (in which case a
     * [org.apache.kafka.common.errors.TimeoutException] is thrown to the caller).
     *
     * Note that asynchronous offset commits sent previously with the [commitAsync] (or similar) are
     * guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and
     * cannot be retried. This fatal error can only occur if you are using automatic group
     * management with [subscribe], or if there is an active group with the same `group.id` which is
     * using group management. In such cases, when you are trying to commit to partitions that are
     * no longer assigned to this consumer because the consumer is for example no longer part of the
     * group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance
     * is in the middle of a rebalance so it is not yet determined which partitions would be
     * assigned to the consumer. In such cases you can first complete the rebalance by calling
     * [poll] and commit can be reconsidered afterwards.
     *
     * NOTE when you reconsider committing after the rebalance, the assigned partitions may have
     * changed, and also for those partitions that are still assigned their fetch positions may have
     * changed too if more records are returned from the [poll] call.
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if
     * offset metadata is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout specified by
     * `default.api.timeout.ms` expires before successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    override fun commitSync() = commitSync(Duration.ofMillis(defaultApiTimeoutMs.toLong()))

    /**
     * Commit offsets returned on the last [poll()][poll] for all the subscribed list of topics and
     * partitions.
     *
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the
     * first fetch after every rebalance and also on startup. As such, if you need to store offsets
     * in anything other than Kafka, this API should not be used.
     *
     * This is a synchronous commit and will block until either the commit succeeds, an
     * unrecoverable error is encountered (in which case it is thrown to the caller), or the passed
     * timeout expires.
     *
     * Note that asynchronous offset commits sent previously with the [commitAsync] (or similar) are
     * guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and
     * cannot be retried. This can only occur if you are using automatic group management with
     * [subscribe], or if there is an active group with the same `group.id` which is using group
     * management. In such cases, when you are trying to commit to partitions that are no longer
     * assigned to this consumer because the consumer is for example no longer part of the group
     * this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance
     * is in the middle of a rebalance so it is not yet determined which partitions would be
     * assigned to the consumer. In such cases you can first complete the rebalance by calling
     * [poll] and commit can be reconsidered afterwards. NOTE when you reconsider committing after
     * the rebalance, the assigned partitions may have changed, and also for those partitions that
     * are still assigned their fetch positions may have changed too if more records are returned
     * from the [poll] call.
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if
     * offset metadata is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before
     * successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    override fun commitSync(timeout: Duration) = commitSync(subscriptions.allConsumed(), timeout)

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     *
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first
     * fetch after every rebalance and also on startup. As such, if you need to store offsets in
     * anything other than Kafka, this API should not be used. The committed offset should be the
     * next message your application will consume, i.e. lastProcessedMessageOffset + 1. If automatic
     * group management with [subscribe] is used, then the committed offsets must belong to the
     * currently auto-assigned partitions.
     *
     * This is a synchronous commit and will block until either the commit succeeds or an
     * unrecoverable error is encountered (in which case it is thrown to the caller), or the timeout
     * specified by `default.api.timeout.ms` expires (in which case a
     * [org.apache.kafka.common.errors.TimeoutException] is thrown to the caller).
     *
     * Note that asynchronous offset commits sent previously with the [commitAsync] (or similar) are
     * guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and
     * cannot be retried. This can only occur if you are using automatic group management with
     * [subscribe], or if there is an active group with the same `group.id` which is using group
     * management. In such cases, when you are trying to commit to partitions that are no longer
     * assigned to this consumer because the consumer is for example no longer part of the group
     * this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance
     * is in the middle of a rebalance so it is not yet determined which partitions would be
     * assigned to the consumer. In such cases you can first complete the rebalance by calling
     * [poll] and commit can be reconsidered afterwards. NOTE when you reconsider committing after
     * the rebalance, the assigned partitions may have changed, and also for those partitions that
     * are still assigned their fetch positions may have changed too if more records are returned
     * from the [poll] call, so when you retry committing you should consider updating the passed in
     * `offset` parameter.
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if
     * offset metadata is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before
     * successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>) = commitSync(
        offsets = offsets,
        timeout = Duration.ofMillis(defaultApiTimeoutMs.toLong()),
    )

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     *
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first
     * fetch after every rebalance and also on startup. As such, if you need to store offsets in
     * anything other than Kafka, this API should not be used. The committed offset should be the
     * next message your application will consume, i.e. lastProcessedMessageOffset + 1. If automatic
     * group management with [subscribe] is used, then the committed offsets must belong to the
     * currently auto-assigned partitions.
     *
     * This is a synchronous commit and will block until either the commit succeeds, an
     * unrecoverable error is encountered (in which case it is thrown to the caller), or the timeout
     * expires.
     *
     * Note that asynchronous offset commits sent previously with the [commitAsync] (or similar) are
     * guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @param timeout The maximum amount of time to await completion of the offset commit
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and
     * cannot be retried. This can only occur if you are using automatic group management with
     * [subscribe], or if there is an active group with the same `group.id` which is using group
     * management. In such cases, when you are trying to commit to partitions that are no longer
     * assigned to this consumer because the consumer is for example no longer part of the group
     * this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance
     * is in the middle of a rebalance so it is not yet determined which partitions would be
     * assigned to the consumer. In such cases you can first complete the rebalance by calling
     * [poll] and commit can be reconsidered afterwards. NOTE when you reconsider committing after
     * the rebalance, the assigned partitions may have changed, and also for those partitions that
     * are still assigned their fetch positions may have changed too if more records are returned
     * from the [poll] call, so when you retry committing you should consider updating the passed in
     * `offset` parameter.
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if
     * offset metadata is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before
     * successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>, timeout: Duration) {
        acquireAndEnsureOpen()
        val commitStart: Long = time.nanoseconds()
        try {
            maybeThrowInvalidGroupIdException()
            offsets.forEach { (topicPartition, offsetAndMetadata) ->
                updateLastSeenEpochIfNewer(
                    topicPartition = topicPartition,
                    offsetAndMetadata = offsetAndMetadata,
                )
            }
            if (!coordinator!!.commitOffsetsSync(offsets, time.timer(timeout)))
                throw TimeoutException(
                    "Timeout of ${timeout.toMillis()}ms expired before successfully committing " +
                            "offsets $offsets"
                )

        } finally {
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart)
            release()
        }
    }

    /**
     * Commit offsets returned on the last [poll] for all the subscribed list of topics and
     * partition. Same as [commitAsync(null)][commitAsync]
     *
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    override fun commitAsync() = commitAsync(null)

    /**
     * Commit offsets returned on the last [poll()][poll] for the subscribed list of topics and
     * partitions.
     *
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the
     * first fetch after every rebalance and also on startup. As such, if you need to store offsets
     * in anything other than Kafka, this API should not be used.
     *
     * This is an asynchronous call and will not block. Any errors encountered are either passed to
     * the callback (if provided) or discarded.
     *
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same
     * order as the invocations. Corresponding commit callbacks are also invoked in the same order.
     * Additionally note that offsets committed through this API are guaranteed to complete before a
     * subsequent call to [commitSync] (and variants) returns.
     *
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    override fun commitAsync(callback: OffsetCommitCallback?) = commitAsync(
        offsets = subscriptions.allConsumed(),
        callback = callback,
    )

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     *
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first
     * fetch after every rebalance and also on startup. As such, if you need to store offsets in
     * anything other than Kafka, this API should not be used. The committed offset should be the
     * next message your application will consume, i.e. lastProcessedMessageOffset + 1. If automatic
     * group management with [subscribe] is used, then the committed offsets must belong to the
     * currently auto-assigned partitions.
     *
     * This is an asynchronous call and will not block. Any errors encountered are either passed to
     * the callback (if provided) or discarded.
     *
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same
     * order as the invocations. Corresponding commit callbacks are also invoked in the same order.
     * Additionally note that offsets committed through this API are guaranteed to complete before a
     * subsequent call to [commitSync] (and variants) returns.
     *
     * @param offsets A map of offsets by partition with associate metadata. This map will be copied
     * internally, so it is safe to mutate the map after returning.
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance
     * gets fenced by broker.
     */
    override fun commitAsync(
        offsets: Map<TopicPartition, OffsetAndMetadata>,
        callback: OffsetCommitCallback?,
    ) {
        acquireAndEnsureOpen()
        try {
            maybeThrowInvalidGroupIdException()
            log.debug("Committing offsets: {}", offsets)
            offsets.forEach { (topicPartition, offsetAndMetadata) ->
                updateLastSeenEpochIfNewer(
                    topicPartition = topicPartition,
                    offsetAndMetadata = offsetAndMetadata
                )
            }
            coordinator!!.commitOffsetsAsync(offsets.toMap(), callback)
        } finally {
            release()
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next [poll(timeout)][poll]. If
     * this API is invoked for the same partition more than once, the latest offset will be used on
     * the next poll(). Note that you may lose data if this API is arbitrarily used in the middle of
     * consumption, to reset the fetch offsets.
     *
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     */
    override fun seek(partition: TopicPartition, offset: Long) {
        require(offset >= 0) { "seek offset must not be a negative number" }
        acquireAndEnsureOpen()
        try {
            log.info("Seeking to offset {} for partition {}", offset, partition)
            val newPosition = FetchPosition(
                offset = offset,
                offsetEpoch = null,  // This will ensure we skip validation
                currentLeader = metadata.currentLeader(partition)
            )
            this.subscriptions.seekUnvalidated(partition, newPosition)
        } finally {
            release()
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next [poll(timeout)][poll]. If
     * this API is invoked for the same partition more than once, the latest offset will be used on
     * the next poll(). Note that you may lose data if this API is arbitrarily used in the middle of
     * consumption, to reset the fetch offsets. This method allows for setting the leaderEpoch along
     * with the desired offset.
     *
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     */
    override fun seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) {
        val offset = offsetAndMetadata.offset
        require (offset >= 0) { "seek offset must not be a negative number" }
        acquireAndEnsureOpen()

        try {
            if (offsetAndMetadata.leaderEpoch() != null) log.info(
                "Seeking to offset {} for partition {} with epoch {}",
                offset,
                partition,
                offsetAndMetadata.leaderEpoch(),
            )
            else log.info("Seeking to offset {} for partition {}", offset, partition)

            val currentLeaderAndEpoch: LeaderAndEpoch = metadata.currentLeader(partition)
            val newPosition = FetchPosition(
                offsetAndMetadata.offset,
                offsetAndMetadata.leaderEpoch(),
                currentLeaderAndEpoch,
            )
            updateLastSeenEpochIfNewer(partition, offsetAndMetadata)
            subscriptions.seekUnvalidated(partition, newPosition)
        } finally {
            release()
        }
    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily,
     * seeking to the first offset in all partitions only when [poll] or [position] are called. If
     * no partitions are provided, seek to the first offset for all of the currently assigned
     * partitions.
     *
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to
     * this consumer
     */
    override fun seekToBeginning(partitions: Collection<TopicPartition>) {
        acquireAndEnsureOpen()
        try {
            val parts = partitions.ifEmpty { subscriptions.assignedPartitions() }
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.EARLIEST)
        } finally {
            release()
        }
    }

    /**
     * Seek to the last offset for each of the given partitions. This function evaluates lazily,
     * seeking to the final offset in all partitions only when [poll] or [position] are called.
     * If no partitions are provided, seek to the final offset for all of the currently assigned
     * partitions.
     *
     * If `isolation.level=read_committed`, the end offset will be the Last Stable Offset, i.e., the
     * offset of the first message with an open transaction.
     *
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to
     * this consumer
     */
    override fun seekToEnd(partitions: Collection<TopicPartition>) {
        acquireAndEnsureOpen()
        try {
            val parts = partitions.ifEmpty { this.subscriptions.assignedPartitions() }
            subscriptions.requestOffsetReset(parts, OffsetResetStrategy.LATEST)
        } finally {
            release()
        }
    }

    /**
     * Get the offset of the *next record* that will be fetched (if a record with that offset
     * exists). This method may issue a remote call to the server if there is no current position
     * for the given partition.
     *
     * This call will block until either the position could be determined or an unrecoverable error
     * is encountered (in which case it is thrown to the caller), or the timeout specified by
     * `default.api.timeout.ms` expires (in which case a
     * [org.apache.kafka.common.errors.TimeoutException] is thrown to the caller).
     *
     * @param partition The partition to get the position for
     * @return The current position of the consumer (that is, the offset of the next record to be
     * fetched)
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently
     * defined for the partition
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts
     * to fetch stable offsets when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the position cannot be determined
     * before the timeout specified by `default.api.timeout.ms` expires
     */
    override fun position(partition: TopicPartition): Long = position(
        partition = partition,
        timeout = Duration.ofMillis(defaultApiTimeoutMs.toLong()),
    )

    /**
     * Get the offset of the *next record* that will be fetched (if a record with that offset
     * exists). This method may issue a remote call to the server if there is no current position
     * for the given partition.
     *
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to get the position for
     * @param timeout The maximum amount of time to await determination of the current position
     * @return The current position of the consumer (that is, the offset of the next record to be
     * fetched)
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently
     * defined for the partition
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the position cannot be determined
     * before the passed timeout expires
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    override fun position(partition: TopicPartition, timeout: Duration): Long {
        acquireAndEnsureOpen()
        try {
            check(this.subscriptions.isAssigned(partition)) {
                "You can only check the position for partitions assigned to this consumer."
            }
            val timer: Timer = time.timer(timeout)
            do {
                val position = this.subscriptions.validPosition(partition)
                if (position != null) return position.offset
                updateFetchPositions(timer)
                client.poll(timer)
            } while (timer.isNotExpired)
            throw TimeoutException(
                "Timeout of ${timeout.toMillis()}ms expired before the position for partition " +
                        "$partition could be determined"
            )
        } finally {
            release()
        }
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this
     * process or another). This offset will be used as the position for the consumer in the event
     * of a failure.
     *
     * This call will do a remote call to get the latest committed offset from the server, and will
     * block until the committed offset is gotten successfully, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by
     * `default.api.timeout.ms` expires (in which case a
     * [org.apache.kafka.common.errors.TimeoutException] is thrown to the caller).
     *
     * @param partition The partition to check
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be
     * found before the timeout specified by `default.api.timeout.ms` expires.
     */
    @Deprecated("since 2.4 Use {@link #committed(Set)} instead")
    override fun committed(partition: TopicPartition): OffsetAndMetadata = committed(
        partition = partition,
        timeout = Duration.ofMillis(defaultApiTimeoutMs.toLong()),
    )

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this
     * process or another). This offset will be used as the position for the consumer in the event
     * of a failure.
     *
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to check
     * @param timeout  The maximum amount of time to await the current committed offset
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be
     * found before expiration of the timeout
     */
    @Deprecated("since 2.4 Use {@link #committed(Set, Duration)} instead")
    override fun committed(partition: TopicPartition, timeout: Duration): OffsetAndMetadata =
        committed(
            partitions = setOf(partition),
            timeout = timeout,
        )[partition]!!

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this
     * process or another). The returned offsets will be used as the position for the consumer in
     * the event of a failure.
     *
     * If any of the partitions requested do not exist, an exception would be thrown.
     *
     * This call will do a remote call to get the latest committed offsets from the server, and will
     * block until the committed offsets are gotten successfully, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by
     * `default.api.timeout.ms` expires (in which case a
     * [org.apache.kafka.common.errors.TimeoutException] is thrown to the caller).
     *
     * @param partitions The partitions to check
     * @return The latest committed offsets for the given partitions; `null` will be returned for
     * the partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts
     * to fetch stable offsets when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be
     * found before the timeout specified by `default.api.timeout.ms` expires.
     */
    override fun committed(
        partitions: Set<TopicPartition>,
    ): Map<TopicPartition, OffsetAndMetadata?> = committed(
        partitions = partitions,
        timeout = Duration.ofMillis(defaultApiTimeoutMs.toLong()),
    )

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this
     * process or another). The returned offsets will be used as the position for the consumer in
     * the event of a failure.
     *
     * If any of the partitions requested do not exist, an exception would be thrown.
     *
     * This call will block to do a remote call to get the latest committed offsets from the server.
     *
     * @param partitions The partitions to check
     * @param timeout  The maximum amount of time to await the latest committed offsets
     * @return The latest committed offsets for the given partitions; `null` will be returned for
     * the partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic
     * or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be
     * found before expiration of the timeout
     */
    override fun committed(
        partitions: Set<TopicPartition>,
        timeout: Duration,
    ): Map<TopicPartition, OffsetAndMetadata?> {
        acquireAndEnsureOpen()
        val start: Long = time.nanoseconds()
        try {
            maybeThrowInvalidGroupIdException()
            val offsets: Map<TopicPartition, OffsetAndMetadata?>? =
                coordinator!!.fetchCommittedOffsets(partitions, time.timer(timeout))
            if (offsets == null) {
                throw TimeoutException(
                    "Timeout of ${timeout.toMillis()}ms expired before the last committed offset " +
                            "for partitions $partitions could be determined. Try tuning " +
                            "default.api.timeout.ms larger to relax the threshold."
                )
            } else {
                offsets.forEach { (topicPartition, offsetAndMetadata) ->
                    updateLastSeenEpochIfNewer(
                        topicPartition = topicPartition,
                        offsetAndMetadata = offsetAndMetadata,
                    )
                }
                return offsets
            }
        } finally {
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start)
            release()
        }
    }

    /**
     * Get the metrics kept by the consumer
     */
    override fun metrics(): Map<MetricName, Metric> = metrics.metrics.toMap()

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to
     * the server if it does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
     * specified topic. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
     * fetched before the amount of time allocated by `default.api.timeout.ms` expires.
     */
    override fun partitionsFor(topic: String): List<PartitionInfo> = partitionsFor(
        topic = topic,
        timeout = Duration.ofMillis(defaultApiTimeoutMs.toLong()),
    )

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to
     * the server if it does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @param timeout The maximum of time to await topic metadata
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
     * specified topic. See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if topic metadata cannot be fetched
     * before expiration of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    override fun partitionsFor(topic: String, timeout: Duration): List<PartitionInfo> {
        acquireAndEnsureOpen()
        try {
            val cluster: Cluster = metadata.fetch()
            val parts = cluster.partitionsForTopic(topic)
            if (parts.isNotEmpty()) return parts
            val timer: Timer = time.timer(timeout)
            val topicMetadata = fetcher.getTopicMetadata(
                request = MetadataRequest.Builder(
                    topics = listOf(topic),
                    allowAutoTopicCreation = metadata.allowAutoTopicCreation(),
                ),
                timer = timer,
            )
            return topicMetadata.getOrDefault(topic, emptyList())
        } finally {
            release()
        }
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method
     * will issue a remote call to the server.
     *
     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
     * fetched before the amount of time allocated by `default.api.timeout.ms` expires.
     */
    override fun listTopics(): Map<String, List<PartitionInfo>> =
        listTopics(Duration.ofMillis(defaultApiTimeoutMs.toLong()))

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method
     * will issue a remote call to the server.
     *
     * @param timeout The maximum time this operation will block to fetch topic metadata
     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException if [wakeup] is called before or while
     * this function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be
     * fetched before expiration of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    override fun listTopics(timeout: Duration): Map<String, List<PartitionInfo>> {
        acquireAndEnsureOpen()
        try {
            return fetcher.getAllTopicMetadata(time.timer(timeout))
        } finally {
            release()
        }
    }

    /**
     * Suspend fetching from the requested partitions. Future calls to [poll] will not return any
     * records from these partitions until they have been resumed using [resume].
     *
     * Note that this method does not affect partition subscription. In particular, it does not
     * cause a group rebalance when automatic assignment is used.
     *
     * Note: Rebalance will not preserve the pause/resume state.
     *
     * @param partitions The partitions which should be paused
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to
     * this consumer
     */
    override fun pause(partitions: Collection<TopicPartition>) {
        acquireAndEnsureOpen()
        try {
            log.debug("Pausing partitions {}", partitions)
            for (partition in partitions) subscriptions.pause(partition)
        } finally {
            release()
        }
    }

    /**
     * Resume specified partitions which have been paused with [pause]. New calls to [poll] will
     * return records from these partitions if there are any to be fetched. If the partitions were
     * not previously paused, this method is a no-op.
     *
     * @param partitions The partitions which should be resumed
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to
     * this consumer
     */
    override fun resume(partitions: Collection<TopicPartition>) {
        acquireAndEnsureOpen()
        try {
            log.debug("Resuming partitions {}", partitions)
            for (partition in partitions) subscriptions.resume(partition)
        } finally {
            release()
        }
    }

    /**
     * Get the set of partitions that were previously paused by a call to [pause].
     *
     * @return The set of paused partitions
     */
    override fun paused(): Set<TopicPartition> {
        acquireAndEnsureOpen()
        try {
            return subscriptions.pausedPartitions().toSet()
        } finally {
            release()
        }
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each
     * partition is the earliest offset whose timestamp is greater than or equal to the given
     * timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have
     * timestamps, null will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     *
     * @return a mapping from partition to the timestamp and offset of the first message with
     * timestamp greater than or equal to the target timestamp. `null` will be returned for the
     * partition if there is no such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
     * topic(s). See the exception for more details
     * @throws IllegalArgumentException if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
     * fetched before the amount of time allocated by `default.api.timeout.ms` expires.
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not
     * support looking up the offsets by timestamp
     */
    override fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
    ): Map<TopicPartition, OffsetAndTimestamp?> = offsetsForTimes(
        timestampsToSearch = timestampsToSearch,
        timeout = Duration.ofMillis(defaultApiTimeoutMs.toLong()),
    )

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each
     * partition is the earliest offset whose timestamp is greater than or equal to the given
     * timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions. If the
     * message format version in a partition is before 0.10.0, i.e. the messages do not have
     * timestamps, null will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @param timeout The maximum amount of time to await retrieval of the offsets
     * @return a mapping from partition to the timestamp and offset of the first message with
     * timestamp greater than or equal to the target timestamp. `null` will be returned for the
     * partition if there is no such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
     * topic(s). See the exception for more details
     * @throws IllegalArgumentException if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
     * fetched before expiration of the passed timeout
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not
     * support looking up the offsets by timestamp
     */
    override fun offsetsForTimes(
        timestampsToSearch: Map<TopicPartition, Long>,
        timeout: Duration,
    ): Map<TopicPartition, OffsetAndTimestamp?> {
        acquireAndEnsureOpen()
        try {
            for ((key, value) in timestampsToSearch.entries) {
                // we explicitly exclude the earliest and latest offset here so the timestamp in the
                // returned OffsetAndTimestamp is always positive.
                require(value >= 0) {
                    "The target time for partition $key is $value. The target time cannot be " +
                            "negative."
                }
            }
            return fetcher.offsetsForTimes(timestampsToSearch, time.timer(timeout))
        } finally {
            release()
        }
    }

    /**
     * Get the first offset for the given partitions.
     *
     * This method does not change the current consumer position of the partitions.
     *
     * @see .seekToBeginning
     * @param partitions the partitions to get the earliest offsets.
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
     * topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
     * fetched before expiration of the configured `default.api.timeout.ms`
     */
    override fun beginningOffsets(
        partitions: Collection<TopicPartition>,
    ): Map<TopicPartition, Long> = beginningOffsets(
        partitions = partitions,
        timeout = Duration.ofMillis(defaultApiTimeoutMs.toLong()),
    )

    /**
     * Get the first offset for the given partitions.
     *
     * This method does not change the current consumer position of the partitions.
     *
     * @see seekToBeginning
     * @param partitions the partitions to get the earliest offsets
     * @param timeout The maximum amount of time to await retrieval of the beginning offsets
     *
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
     * topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
     * fetched before expiration of the passed timeout
     */
    override fun beginningOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration,
    ): Map<TopicPartition, Long> {
        acquireAndEnsureOpen()
        try {
            return fetcher.beginningOffsets(partitions, time.timer(timeout))
        } finally {
            release()
        }
    }

    /**
     * Get the end offsets for the given partitions. In the default `read_uncommitted` isolation
     * level, the end offset is the high watermark (that is, the offset of the last successfully
     * replicated message plus one). For `read_committed` consumers, the end offset is the last
     * stable offset (LSO), which is the minimum of the high watermark and the smallest offset of
     * any open transaction. Finally, if the partition has never been written to, the end offset is
     * 0.
     *
     * This method does not change the current consumer position of the partitions.
     *
     * @see seekToEnd
     * @param partitions the partitions to get the end offsets.
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
     * topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
     * fetched before the amount of time allocated by `default.api.timeout.ms` expires
     */
    override fun endOffsets(
        partitions: Collection<TopicPartition>,
    ): Map<TopicPartition, Long> = endOffsets(
        partitions = partitions,
        timeout = Duration.ofMillis(defaultApiTimeoutMs.toLong()),
    )

    /**
     * Get the end offsets for the given partitions. In the default `read_uncommitted` isolation
     * level, the end offset is the high watermark (that is, the offset of the last successfully
     * replicated message plus one). For `read_committed` consumers, the end offset is the last
     * stable offset (LSO), which is the minimum of the high watermark and the smallest offset of
     * any open transaction. Finally, if the partition has never been written to, the end offset is
     * 0.
     *
     * This method does not change the current consumer position of the partitions.
     *
     * @see seekToEnd
     * @param partitions the partitions to get the end offsets.
     * @param timeout The maximum amount of time to await retrieval of the end offsets
     *
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
     * topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offsets could not be fetched
     * before expiration of the passed timeout
     */
    override fun endOffsets(
        partitions: Collection<TopicPartition>,
        timeout: Duration,
    ): Map<TopicPartition, Long> {
        acquireAndEnsureOpen()
        try {
            return fetcher.endOffsets(partitions, time.timer(timeout))
        } finally {
            release()
        }
    }

    /**
     * Get the consumer's current lag on the partition. Returns an "empty" [OptionalLong] if the lag
     * is not known, for example if there is no position yet, or if the end offset is not known yet.
     *
     * This method uses locally cached metadata and never makes a remote call.
     *
     * @param topicPartition The partition to get the lag for.
     *
     * @return This `Consumer` instance's current lag for the given partition.
     *
     * @throws IllegalStateException if the `topicPartition` is not assigned
     */
    override fun currentLag(topicPartition: TopicPartition): Long? {
        acquireAndEnsureOpen()
        try {
            val lag = subscriptions.partitionLag(topicPartition, isolationLevel)

            // if the log end offset is not known and hence cannot return lag and there is no
            // in-flight list offset requested yet, issue a list offset request for that partition
            // so that next time we may get the answer; we do not need to wait for the return value
            // since we would not try to poll the network client synchronously
            if (lag == null) {
                if (
                    subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null
                    && !subscriptions.partitionEndOffsetRequested(topicPartition)
                ) {
                    log.info(
                        "Requesting the log end offset for {} in order to compute lag",
                        topicPartition
                    )
                    subscriptions.requestPartitionEndOffset(topicPartition)
                    fetcher.endOffsets(
                        partitions = setOf(topicPartition),
                        timer = time.timer(timeoutMs = 0L),
                    )
                }
                return null
            }
            return lag
        } finally {
            release()
        }
    }

    /**
     * Return the current group metadata associated with this consumer.
     *
     * @return consumer group metadata
     * @throws org.apache.kafka.common.errors.InvalidGroupIdException if consumer does not have a
     * group
     */
    override fun groupMetadata(): ConsumerGroupMetadata {
        acquireAndEnsureOpen()
        try {
            maybeThrowInvalidGroupIdException()
            return coordinator!!.groupMetadata()
        } finally {
            release()
        }
    }

    /**
     * Alert the consumer to trigger a new rebalance by rejoining the group. This is a nonblocking
     * call that forces the consumer to trigger a new rebalance on the next [poll] call. Note that
     * this API does not itself initiate the rebalance, so you must still call [poll]. If a
     * rebalance is already in progress this call will be a no-op. If you wish to force an
     * additional rebalance you must complete the current one by calling poll before retrying this
     * API.
     *
     * You do not need to call this during normal processing, as the consumer group will manage
     * itself automatically and rebalance when necessary. However there may be situations where the
     * application wishes to trigger a rebalance that would otherwise not occur. For example, if
     * some condition external and invisible to the Consumer and its group changes in a way that
     * would affect the userdata encoded in the
     * [Subscription][org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription], the
     * Consumer will not be notified and no rebalance will occur. This API can be used to force the
     * group to rebalance so that the assignor can perform a partition reassignment based on the
     * latest userdata. If your assignor does not use this userdata, or you do not use a custom
     * [ConsumerPartitionAssignor][org.apache.kafka.clients.consumer.ConsumerPartitionAssignor], you
     * should not use this API.
     *
     * @param reason The reason why the new rebalance is needed.
     * @throws java.lang.IllegalStateException if the consumer does not use group subscription
     */
    override fun enforceRebalance(reason: String?) {
        acquireAndEnsureOpen()
        try {
            checkNotNull(coordinator) {
                "Tried to force a rebalance but consumer does not have a group."
            }
            coordinator.requestRejoin(if (reason.isNullOrEmpty()) DEFAULT_REASON else reason)
        } finally {
            release()
        }
    }

    /**
     * @see enforceRebalance
     */
    override fun enforceRebalance() = enforceRebalance(null)

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed
     * cleanup. If auto-commit is enabled, this will commit the current offsets if possible within
     * the default timeout. See [close] for details. Note that [wakeup] cannot be used to interrupt
     * close.
     *
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is
     * interrupted before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    override fun close() = close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS))

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * `timeout` for the consumer to complete pending commits and leave the group. If auto-commit is
     * enabled, this will commit the current offsets if possible within the timeout. If the consumer
     * is unable to complete offset commits and gracefully leave the group before the timeout
     * expires, the consumer is force closed. Note that [wakeup] cannot be used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     * non-negative. Specifying a timeout of zero means do not wait for pending requests to
     * complete.
     *
     * @throws IllegalArgumentException If the `timeout` is negative.
     * @throws InterruptException If the thread is interrupted before or while this function is
     * called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    override fun close(timeout: Duration) {
        require(timeout.toMillis() >= 0) { "The timeout cannot be negative." }
        acquire()
        try {
            if (!closed) {
                // need to close before setting the flag since the close function itself may trigger
                // rebalance callback that needs the consumer to be open still
                close(timeout.toMillis(), false)
            }
        } finally {
            closed = true
            release()
        }
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long
     * poll. The thread which is blocking in an operation will throw
     * [org.apache.kafka.common.errors.WakeupException]. If no thread is blocking in a method which
     * can throw [org.apache.kafka.common.errors.WakeupException], the next call to such a method
     * will raise it instead.
     */
    override fun wakeup() = client.wakeup()

    private fun configureClusterResourceListeners(
        keyDeserializer: Deserializer<K>?,
        valueDeserializer: Deserializer<V>?,
        vararg candidateLists: List<*>,
    ): ClusterResourceListeners {
        val clusterResourceListeners = ClusterResourceListeners()
        for (candidateList in candidateLists) clusterResourceListeners.maybeAddAll(candidateList)

        clusterResourceListeners.maybeAdd(keyDeserializer)
        clusterResourceListeners.maybeAdd(valueDeserializer)

        return clusterResourceListeners
    }

    private fun close(timeoutMs: Long, swallowException: Boolean) {
        log.trace("Closing the Kafka consumer")
        val firstException = AtomicReference<Throwable?>()
        try {
            coordinator?.close(time.timer(min(timeoutMs, requestTimeoutMs)))
        } catch (t: Throwable) {
            firstException.compareAndSet(null, t)
            log.error("Failed to close coordinator", t)
        }
        closeQuietly(fetcher, "fetcher", firstException)
        closeQuietly(interceptors, "consumer interceptors", firstException)
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException)
        closeQuietly(metrics, "consumer metrics", firstException)
        closeQuietly(client, "consumer network client", firstException)
        closeQuietly(keyDeserializer, "consumer key deserializer", firstException)
        closeQuietly(valueDeserializer, "consumer value deserializer", firstException)
        unregisterAppInfo(JMX_PREFIX, clientId, metrics)
        log.debug("Kafka consumer has been closed")
        val exception = firstException.get()
        if (exception != null && !swallowException) {
            if (exception is InterruptException) throw exception
            throw KafkaException("Failed to close kafka consumer", exception)
        }
    }

    /**
     * Set the fetch position to the committed position (if there is one) or reset it using the
     * offset reset policy the user has configured.
     *
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See
     * the exception for more details
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no
     * offset reset policy is defined
     * @return `true` iff the operation completed without timing out
     */
    private fun updateFetchPositions(timer: Timer): Boolean {
        // If any partitions have been truncated due to a leader change, we need to validate the
        // offsets
        fetcher.validateOffsetsIfNeeded()
        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions()
        if (cachedSubscriptionHasAllFetchPositions) return true

        // If there are any partitions which do not have a valid position and are not awaiting
        // reset, then we need to fetch committed offsets. We will only do a coordinator lookup if
        // there are partitions which have missing positions, so a consumer with manually assigned
        // partitions can avoid a coordinator dependence by always ensuring that assigned partitions
        // have an initial position.
        if (coordinator?.refreshCommittedOffsetsIfNeeded(timer) != true) return false

        // If there are partitions still needing a position and a reset policy is defined, request
        // reset using the default policy. If no reset strategy is defined and there are partitions
        // with a missing position, then we will raise an exception.
        subscriptions.resetInitializingPositions()

        // Finally send an asynchronous request to lookup and update the positions of any partitions
        // which are awaiting reset.
        fetcher.resetOffsetsIfNeeded()
        return true
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the consumer has been closed
     */
    private fun acquireAndEnsureOpen() {
        acquire()
        if (this.closed) {
            release()
            error("This consumer has already been closed.")
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multi-threaded access. Instead of
     * blocking when the lock is not available, however, we just throw an exception (since
     * multi-threaded usage is not supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private fun acquire() {
        val threadId = Thread.currentThread().id
        if (
            threadId != currentThread.get()
            && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)
        ) throw ConcurrentModificationException(
            "KafkaConsumer is not safe for multi-threaded access"
        )
        refcount.incrementAndGet()
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private fun release() {
        if (refcount.decrementAndGet() == 0) currentThread.set(NO_CURRENT_THREAD)
    }

    private fun throwIfNoAssignorsConfigured() {
        check(!assignors.isNullOrEmpty()) {
            "Must configure at least one partition assigner class name to " +
                    "${ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG} configuration property"
        }
    }

    private fun maybeThrowInvalidGroupIdException() {
        if (groupId == null) throw InvalidGroupIdException(
            "To use the group management or offset commit APIs, you must provide a valid " +
                    "${ConsumerConfig.GROUP_ID_CONFIG} in the consumer configuration."
        )
    }

    private fun updateLastSeenEpochIfNewer(
        topicPartition: TopicPartition,
        offsetAndMetadata: OffsetAndMetadata?,
    ) {
        offsetAndMetadata?.leaderEpoch()?.let { epoch ->
            metadata.updateLastSeenEpochIfNewer(
                topicPartition = topicPartition,
                leaderEpoch = epoch,
            )
        }
    }

    // Functions below are for testing only
    fun getClientId(): String? = clientId

    companion object {

        private const val CLIENT_ID_METRIC_TAG = "client-id"

        private const val NO_CURRENT_THREAD = -1L

        private const val JMX_PREFIX = "kafka.consumer"

        const val DEFAULT_CLOSE_TIMEOUT_MS = (30 * 1000).toLong()

        const val DEFAULT_REASON = "rebalance enforced by user"

        private fun buildMetrics(config: ConsumerConfig, time: Time, clientId: String): Metrics {
            val metricsTags = mapOf(CLIENT_ID_METRIC_TAG to clientId)

            val metricConfig = MetricConfig().apply {
                samples = config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG)!!
                timeWindowMs = config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG)!!
                recordingLevel =Sensor.RecordingLevel.forName(
                    config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)!!
                )
                tags = metricsTags
            }

            val reporters = metricsReporters(clientId, config)

            val metricsContext = KafkaMetricsContext(
                namespace = JMX_PREFIX,
                contextLabels =
                config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX),
            )
            return Metrics(
                config = metricConfig,
                reporters = reporters.toMutableList(),
                time = time,
                metricsContext = metricsContext,
            )
        }
    }
}
