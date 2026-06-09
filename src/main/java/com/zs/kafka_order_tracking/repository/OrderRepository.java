package com.zs.kafka_order_tracking.repository;

import com.zs.kafka_order_tracking.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {
}
