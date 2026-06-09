package com.zs.kafka_order_tracking.repository;

import com.zs.kafka_order_tracking.model.FailedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FailedEventRepository extends JpaRepository<FailedEvent, Long> {
}
