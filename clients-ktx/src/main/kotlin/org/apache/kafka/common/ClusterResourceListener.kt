package org.apache.kafka.common

/**
 * A callback interface that users can implement when they wish to get notified about changes in the
 * Cluster metadata.
 *
 * Users who need access to cluster metadata in interceptors, metric reporters, serializers and
 * deserializers can implement this interface. The order of method calls for each of these types is
 * described below.
 *
 * <h4>Clients</h4>
 * There will be one invocation of [ClusterResourceListener.onUpdate] after each metadata response.
 * Note that the cluster id may be null when the Kafka broker version is below 0.10.1.0. If you
 * receive a null cluster id, you can expect it to always be null unless you have a cluster with
 * multiple broker versions which can happen if the cluster is being upgraded while the client is
 * running.
 *
 * [org.apache.kafka.clients.producer.ProducerInterceptor] : The [ClusterResourceListener.onUpdate]
 * method will be invoked after [org.apache.kafka.clients.producer.ProducerInterceptor.onSend] but
 * before [org.apache.kafka.clients.producer.ProducerInterceptor.onAcknowledgement].
 *
 * [org.apache.kafka.clients.consumer.ConsumerInterceptor] : The [ClusterResourceListener.onUpdate]
 * method will be invoked before [org.apache.kafka.clients.consumer.ConsumerInterceptor.onConsume]
 *
 * [org.apache.kafka.common.serialization.Serializer] : The [ClusterResourceListener.onUpdate]
 * method will be invoked before [org.apache.kafka.common.serialization.Serializer.serialize]
 *
 * [org.apache.kafka.common.serialization.Deserializer] : The [ClusterResourceListener.onUpdate]
 * method will be invoked before [org.apache.kafka.common.serialization.Deserializer.deserialize]
 *
 * [org.apache.kafka.common.metrics.MetricsReporter] : The [ClusterResourceListener.onUpdate]
 * method will be invoked after first [org.apache.kafka.clients.producer.KafkaProducer.send]
 * invocation for Producer metrics reporter and after first
 * [org.apache.kafka.clients.consumer.KafkaConsumer.poll] invocation for Consumer metrics
 * reporters. The reporter may receive metric events from the network layer before this method is
 * invoked.
 *
 * <h4>Broker</h4>
 * There is a single invocation [ClusterResourceListener.onUpdate] on broker start-up and the
 * cluster metadata will never change.
 *
 * KafkaMetricsReporter : The [ClusterResourceListener.onUpdate] method will be invoked during the
 * bootup of the Kafka broker. The reporter may receive metric events from the network layer before
 * this method is invoked.
 *
 * [org.apache.kafka.common.metrics.MetricsReporter] : The [ClusterResourceListener.onUpdate] method
 * will be invoked during the bootup of the Kafka broker. The reporter may receive metric events
 * from the network layer before this method is invoked.
 */
interface ClusterResourceListener {
    /**
     * A callback method that a user can implement to get updates for [ClusterResource].
     * @param clusterResource cluster metadata
     */
    fun onUpdate(clusterResource: ClusterResource?)
}
