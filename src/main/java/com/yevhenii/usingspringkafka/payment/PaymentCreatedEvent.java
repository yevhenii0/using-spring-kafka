package com.yevhenii.usingspringkafka.payment;

import lombok.Data;

@Data
public class PaymentCreatedEvent {
  private Payment payment;
}
