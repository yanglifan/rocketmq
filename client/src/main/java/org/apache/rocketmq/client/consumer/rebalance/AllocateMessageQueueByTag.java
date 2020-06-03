package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.netty.util.internal.ThreadLocalRandom;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.support.TaggedQueueRegistry;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * @author Yang Lifan
 */
public class AllocateMessageQueueByTag implements AllocateMessageQueueStrategy {
    private static final InternalLogger LOG = ClientLogger.getLog();

    private AllocateMessageQueueStrategy defaultAllocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    private String tag;
    private String topic;
    private TaggedQueueRegistry taggedQueueRegistry;

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (isRetryQueue(mqAll)) {
            return defaultAllocateMessageQueueStrategy.allocate(consumerGroup, currentCID, mqAll, cidAll);
        }

        try {
            Integer index = taggedQueueRegistry.registerConsumer(topic, consumerGroup, currentCID, tag);
            if (isTaggedConsumer()) {
                MessageQueue taggedQueue = selectOneByTag(mqAll, tag, topic, index);
                return Collections.singletonList(taggedQueue);
            } else {
                List<MessageQueue> mainMessageQueueList = removeTaggedMessageQueues(mqAll, consumerGroup);
                List<String> mainConsumerIdList = removeTaggedConsumerIds(consumerGroup, cidAll);

                // TODO Sometimes mainMessageQueueList or mainConsumerIdList is empty
                return defaultAllocateMessageQueueStrategy.allocate(consumerGroup, currentCID, mainMessageQueueList,
                        mainConsumerIdList);
            }
        } catch (Exception e) {
            LOG.error("Allocate failed", e);
            return defaultAllocateMessageQueueStrategy.allocate(consumerGroup, currentCID, mqAll, cidAll);
        }
    }

    @Override
    public String getName() {
        return null;
    }

    private List<String> removeTaggedConsumerIds(String consumerGroup, List<String> cidAll) {
        Set<String> taggedConsumerIds = taggedQueueRegistry.getAllTaggedConsumerIds(consumerGroup);
        if (taggedConsumerIds == null) {
            return cidAll;
        }

        List<String> results = new ArrayList<String>(cidAll);
        for (String taggedCid : taggedConsumerIds) {
            results.remove(taggedCid);
        }

        return results;
    }

    private List<MessageQueue> removeTaggedMessageQueues(List<MessageQueue> mqAll, String consumerGroup) {
        Set<Integer> indices = taggedQueueRegistry.getIndicesByConsumerGroup(consumerGroup);
        if (indices == null) {
            return Collections.emptyList();
        }

        int allQueueSize = mqAll.size();
        Set<Integer> taggedQueueIndexes = new HashSet<Integer>();
        for (Integer rank : indices) {
            Integer index = allQueueSize - 1 - rank;
            taggedQueueIndexes.add(index);
        }

        List<MessageQueue> mainQueues = new ArrayList<MessageQueue>();
        for (int i = 0; i < mqAll.size(); i++) {
            if (!taggedQueueIndexes.contains(i)) {
                mainQueues.add(mqAll.get(i));
            }
        }

        return mainQueues;
    }

    private MessageQueue selectOneByTag(List<MessageQueue> queueList, String currentTag,
                                        String currentTopic, Integer consumerRank) {
        int queueMaxSize = queueList.size();
        int sizeOfTags = taggedQueueRegistry.getSizeOfTags(currentTopic);

        if (queueMaxSize <= sizeOfTags) {
            throw new RuntimeException();
        }

        int index;
        if (StringUtils.isEmpty(currentTag)) {
            // 主干消息 Producer
            index = ThreadLocalRandom.current().nextInt(queueMaxSize - sizeOfTags);
        } else {
            if (consumerRank != null) {
                // Consumer
                index = queueMaxSize - 1 - consumerRank;
                LOG.info("The consumer with tag {} will be allocated the queue #{}", tag, index);
                return queueList.get(index);
            } else {
                // Producer
                Integer rank = taggedQueueRegistry.getTagIndex(currentTopic, currentTag);
                if (rank == null) {
                    index = ThreadLocalRandom.current().nextInt(queueMaxSize);
                } else {
                    int intRank = rank.intValue();
                    if (queueMaxSize - 1 <= intRank) {
                        throw new RuntimeException();
                    }

                    index = queueMaxSize - 1 - intRank;
                }
            }
        }

        LOG.info("The message with tag {} will send to the queue #{}", currentTag, index);
        return queueList.get(index);
    }

    private boolean isTaggedConsumer() {
        return StringUtils.isNotEmpty(tag);
    }

    private boolean isRetryQueue(List<MessageQueue> mqAll) {
        return mqAll.get(0).getTopic().contains("RETRY");
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setTaggedQueueRegistry(TaggedQueueRegistry taggedQueueRegistry) {
        this.taggedQueueRegistry = taggedQueueRegistry;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public void setDefaultAllocateMessageQueueStrategy(AllocateMessageQueueStrategy defaultAllocateMessageQueueStrategy) {
        this.defaultAllocateMessageQueueStrategy = defaultAllocateMessageQueueStrategy;
    }
}
