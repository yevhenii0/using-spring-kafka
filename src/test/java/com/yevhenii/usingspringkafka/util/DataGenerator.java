package com.yevhenii.usingspringkafka.util;

import com.yevhenii.usingspringkafka.payment.Payment;
import com.yevhenii.usingspringkafka.payment.PaymentCreatedEvent;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public interface DataGenerator<T> {

  static DataGenerator<PaymentCreatedEvent> paymentCreatedEvents() {
    return idx -> {
      var payment = new Payment();
      payment.setUuid(UUID.randomUUID());
      payment.setMethod("CARD");
      payment.setAmount((double) (100 + idx));
      payment.setTimeCreated(Instant.now());

      var paymentCreated = new PaymentCreatedEvent();
      paymentCreated.setPayment(payment);
      return paymentCreated;
    };
  }

  T gen(int idx);

  default T gen() {
    return gen(ThreadLocalRandom.current().nextInt(1000));
  }
}
