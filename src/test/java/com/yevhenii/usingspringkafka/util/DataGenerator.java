package com.yevhenii.usingspringkafka.util;

import com.yevhenii.usingspringkafka.payment.Payment;
import com.yevhenii.usingspringkafka.payment.PaymentCreatedEvent;
import java.time.Instant;
import java.util.UUID;

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
}
