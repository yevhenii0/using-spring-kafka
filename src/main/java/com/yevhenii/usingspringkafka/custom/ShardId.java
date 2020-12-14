package com.yevhenii.usingspringkafka.custom;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class ShardId {

  public static final ShardId DEFAULT = new ShardId("default");

  @NonNull
  private final String id;

  public String value() {
    return id;
  }
}
