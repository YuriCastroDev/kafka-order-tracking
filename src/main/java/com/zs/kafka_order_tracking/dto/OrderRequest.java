package com.zs.kafka_order_tracking.dto;

import lombok.Data;

@Data
public class OrderRequest {
    private String customerName;
    private String productName;
    private Integer quantity;
    private Double price;
}