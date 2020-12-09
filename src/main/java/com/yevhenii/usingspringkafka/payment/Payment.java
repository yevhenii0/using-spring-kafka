package com.yevhenii.usingspringkafka.payment;

import java.time.Instant;
import java.util.UUID;
import lombok.Data;

@Data
public class Payment {
  private UUID uuid;
  private String method;
  private Double amount;
  private Instant timeCreated;
}
