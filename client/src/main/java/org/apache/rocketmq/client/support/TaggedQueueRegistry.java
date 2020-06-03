package org.apache.rocketmq.client.support;

import java.util.Set;

/**
 * @author Yang Lifan
 */
public interface TaggedQueueRegistry {
    /**
     * Register a tagged consumer and get the tag index. First, store the tag and get the index. Then, store the ID of
     * the tagged consumer.
     * <p>
     * TODO: Too much string params
     *
     * @param topic         the message topic
     * @param consumerGroup RMQ consumer group
     * @param consumerId    RMQ consumer ID
     * @param tag           the tag of the consumer
     * @return the tag sorted index.
     */
    Integer registerConsumer(String topic, String consumerGroup, String consumerId, String tag);

    /**
     * Get the index of the tag.
     *
     * @param topic the topic of the message
     * @param tag   the value of the tag
     * @return the index of the tag
     */
    Integer getTagIndex(String topic, String tag);

    /**
     * Get the number of the all tags under a message topic.
     *
     * @return the tag list size
     */
    Integer getSizeOfTags(String topic);

    /**
     * Get the id of all tagged consumers.
     *
     * @param consumerGroup the consumer group of the current consumer.
     * @return the set of the consumer id.
     */
    Set<String> getAllTaggedConsumerIds(String consumerGroup);

    Set<Integer> getIndicesByConsumerGroup(String consumerGroup);
}
