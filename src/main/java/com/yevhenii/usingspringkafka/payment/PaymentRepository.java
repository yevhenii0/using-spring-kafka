package com.yevhenii.usingspringkafka.payment;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@RequiredArgsConstructor
public class PaymentRepository {

  private final JdbcTemplate jdbcTemplate;

  public Long count() {
    return DataAccessUtils.singleResult(
        jdbcTemplate.query(
            "SELECT COUNT(*) FROM payment",
            (rs, rowNum) -> rs.getLong(1)
        )
    );
  }

  public Payment getOne(UUID id) {
    return DataAccessUtils.singleResult(
        jdbcTemplate.query(
            "SELECT id, method, amount, time_created FROM payment where id = ?",
            new Object[] {id},
            (rs, rowNum) -> {
              Payment payment = new Payment();
              payment.setUuid(UUID.fromString(rs.getString(1)));
              payment.setMethod(rs.getString(2));
              payment.setAmount(rs.getDouble(3));
              payment.setTimeCreated(rs.getTimestamp(4).toInstant());
              return payment;
            }
        )
    );
  }

  @Transactional
  public void save(Payment payment) {
    jdbcTemplate.update(
        "INSERT INTO payment (id, method, amount, time_created) VALUES (?, ?, ?, ?)",
        payment.getUuid(), payment.getMethod(), payment.getAmount(), Timestamp.from(payment.getTimeCreated())
    );
  }

  @Transactional
  public void save(List<Payment> payments) {
    jdbcTemplate.batchUpdate(
        "INSERT INTO payment (id, method, amount, time_created) VALUES (?, ?, ?, ?)",
        payments.stream()
            .map(p -> new Object[] {p.getUuid(), p.getMethod(), p.getAmount(), Timestamp.from(p.getTimeCreated())})
            .collect(Collectors.toList())
    );
  }

  @Transactional
  public void deleteAll() {
    jdbcTemplate.execute("DELETE FROM payment");
  }
}
