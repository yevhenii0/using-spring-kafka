package com.yevhenii.usingspringkafka.util;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Supplier;

public final class Names {

  private static final Set<String> issuedNames = new CopyOnWriteArraySet<>();

  public static String randTopic() {
    return unique(() -> "topic-" + randString());
  }

  public static String randGroupId() {
    return unique(() -> "group-" + randString());
  }

  private static String randString() {
    return UUID.randomUUID().toString().replaceAll("-", "").substring(0, 10);
  }

  private static String unique(Supplier<String> sup) {
    String str = sup.get();
    if (issuedNames.add(str)) {
      return str;
    } else {
      return unique(sup);
    }
  }
}
