package com.yevhenii.usingspringkafka.util;

public class ContainerProperties {

  public static String tcpPort(String serviceName, int port) {
    return String.valueOf(System.getProperties().get(serviceName + ".tcp." + port));
  }

  public static String host(String serviceName) {
    return String.valueOf(System.getProperties().get(serviceName + ".host"));
  }

  public static String containerId(String serviceName) {
    return String.valueOf(System.getProperties().get(serviceName + ".containerId"));
  }
}
