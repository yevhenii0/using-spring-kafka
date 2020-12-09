package com.yevhenii.usingspringkafka.util;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Executes arbitrary pieces of code after test method completes.
 * Cleans up registered execution once those executed.
 */
public class Defer implements AfterEachCallback {

  public static void defer(DeferExecution runnable) {
    deferExecution.add(runnable);
  }

  public static <T extends AutoCloseable> T deferClose(T c) {
    defer(c::close);
    return c;
  }

  private static final List<DeferExecution> deferExecution = new ArrayList<>();

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    for (DeferExecution runnable : deferExecution) {
      runnable.execute();
    }
    deferExecution.clear();
  }

  public interface DeferExecution {
    void execute() throws Exception;
  }
}
