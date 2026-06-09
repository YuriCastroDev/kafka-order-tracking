package com.zs.kafka_order_tracking.repository;

import com.zs.kafka_order_tracking.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
