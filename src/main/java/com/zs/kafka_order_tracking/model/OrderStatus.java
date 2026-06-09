package com.zs.kafka_order_tracking.model;

public enum OrderStatus {
    CREATED,
    PROCESSING,
    SHIPPED,
    DELIVERED,
    CANCELLED
}
