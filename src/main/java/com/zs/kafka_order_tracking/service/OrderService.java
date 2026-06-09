package com.zs.kafka_order_tracking.service;

import com.zs.kafka_order_tracking.dto.OrderRequest;
import com.zs.kafka_order_tracking.model.Order;
import com.zs.kafka_order_tracking.model.OrderStatus;
import com.zs.kafka_order_tracking.event.OrderCreatedEvent;
import com.zs.kafka_order_tracking.producer.OrderEventProducer;
import com.zs.kafka_order_tracking.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;

    public Order createOrder(OrderRequest request) {
        Order order = Order.builder()
                .customerName(request.getCustomerName())
                .productName(request.getProductName())
                .quantity(request.getQuantity())
                .price(request.getPrice())
                .build();

        Order saved = orderRepository.save(order);
        log.info("Order saved with id: {}", saved.getId());

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(saved.getId())
                .customerName(saved.getCustomerName())
                .productName(saved.getProductName())
                .quantity(saved.getQuantity())
                .price(saved.getPrice())
                .status(saved.getStatus().name())
                .createdAt(saved.getCreatedAt())
                .build();

        orderEventProducer.publishOrderCreated(event);

        return saved;
    }

    public Order getOrder(UUID id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Order not found: " + id));
    }

    public Order updateStatus(UUID id, OrderStatus status) {
        Order order = getOrder(id);
        order.setStatus(status);
        return orderRepository.save(order);
    }
}
