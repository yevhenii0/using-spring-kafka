package com.yevhenii.usingspringkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class Application {
  public static void main(String[] args) {
    new SpringApplication(Application.class).run(args);
  }
}
