package com.yevhenii.usingspringkafka.ext;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/**
 * Assigns offsets to (now-delta).
 */
public class RelativeOffsetAssigningRebalanceListener implements ConsumerAwareRebalanceListener {

  private final Duration delta;

  public RelativeOffsetAssigningRebalanceListener(Duration delta) {
    this.delta = delta.isNegative() ? delta : delta.negated();
  }

  @Override
  public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
    // could happen e.g. in case previous call to onPartitionsAssigned failed with exception
    if (partitions.isEmpty()) {
      return;
    }

    Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
    long timestampToSearchMs = ZonedDateTime.now().plus(delta).toInstant().toEpochMilli();

    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions));
    for (TopicPartition partition : partitions) {
      if (committed.get(partition) == null) {
        timestampsToSearch.put(partition, timestampToSearchMs);
      }
    }

    if (!timestampsToSearch.isEmpty()) {
      List<TopicPartition> seekToBeginningPartitions = new ArrayList<>();

      Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampsToSearch);
      for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsets.entrySet()) {
        if (entry.getValue() != null) {
          consumer.seek(entry.getKey(), entry.getValue().offset());
        } else {
          seekToBeginningPartitions.add(entry.getKey());
        }
      }

      if (!seekToBeginningPartitions.isEmpty()) {
        consumer.seekToBeginning(seekToBeginningPartitions);
      }
    }
  }
}
