package com.yevhenii.usingspringkafka.payment;

import static org.assertj.core.api.Assertions.assertThat;


import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class PaymentRepositoryTest {

  @Autowired
  private PaymentRepository repository;

  @AfterEach
  void clean() {
    repository.deleteAll();
  }

  @Test
  void saveWorks() {
    Payment payment = new Payment();
    payment.setUuid(UUID.randomUUID());
    payment.setMethod("CARD");
    payment.setAmount(100.0);
    payment.setTimeCreated(LocalDateTime.now().withNano(0).toInstant(ZoneOffset.UTC));
    repository.save(payment);

    assertThat(repository.getOne(payment.getUuid())).isEqualTo(payment);
  }

  @Test
  void batchSaveWorks() {
    Payment payment1 = new Payment();
    payment1.setUuid(UUID.randomUUID());
    payment1.setMethod("CARD");
    payment1.setAmount(100.0);
    payment1.setTimeCreated(LocalDateTime.now().withNano(0).toInstant(ZoneOffset.UTC));

    Payment payment2 = new Payment();
    payment2.setUuid(UUID.randomUUID());
    payment2.setMethod("CARD");
    payment2.setAmount(100.0);
    payment2.setTimeCreated(LocalDateTime.now().withNano(0).toInstant(ZoneOffset.UTC));

    repository.save(List.of(payment1, payment2));

    assertThat(repository.getOne(payment1.getUuid())).isEqualTo(payment1);
    assertThat(repository.getOne(payment2.getUuid())).isEqualTo(payment2);
  }
}
