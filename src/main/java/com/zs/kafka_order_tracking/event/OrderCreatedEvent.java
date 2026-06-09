package com.zs.kafka_order_tracking.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {
    private UUID orderId;
    private String customerName;
    private String productName;
    private Integer quantity;
    private Double price;
    private String status;
    private LocalDateTime createdAt;
}
