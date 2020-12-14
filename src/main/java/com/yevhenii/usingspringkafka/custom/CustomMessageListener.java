package com.yevhenii.usingspringkafka.custom;

import java.util.List;

public interface CustomMessageListener<T> {

   Class<T> messageType();

   void onMessage(T message);

   List<String> topics();

   default ShardId shardId() {
      return ShardId.DEFAULT;
   }
}
