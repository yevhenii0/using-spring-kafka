package com.yevhenii.usingspringkafka.ext;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/**
 * Assigns offsets to (now-delta).
 * Same as used in tw-tasks: https://github.com/transferwise/tw-tasks-executor/blob/da752f09b6a342a81c96a30d901327f1688ddccf/tw-tasks-core/src/main/java/com/transferwise/tasks/triggering/SeekToDurationOnRebalance.java#L29
 */
public class RelativeOffsetAssigningRebalanceListener implements ConsumerAwareRebalanceListener {

  private final Duration delta;

  public RelativeOffsetAssigningRebalanceListener(Duration delta) {
    this.delta = delta.isNegative() ? delta : delta.negated();
  }

  @Override
  public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
    long timestampToSearchMs = ZonedDateTime.now().plus(delta).toInstant().toEpochMilli();

    for (TopicPartition partition : partitions) {
      if (consumer.committed(Collections.singleton(partition)).get(partition) == null) {
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
