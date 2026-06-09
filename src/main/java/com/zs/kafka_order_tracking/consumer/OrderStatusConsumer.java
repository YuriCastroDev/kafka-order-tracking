package com.zs.kafka_order_tracking.consumer;

import com.zs.kafka_order_tracking.model.OrderStatus;
import com.zs.kafka_order_tracking.model.ProcessedEvent;
import com.zs.kafka_order_tracking.event.OrderCreatedEvent;
import com.zs.kafka_order_tracking.repository.OrderRepository;
import com.zs.kafka_order_tracking.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderStatusConsumer {

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = "order-events", groupId = "order-status-group")
    public void consume(OrderCreatedEvent event,
                        @Header("kafka_receivedMessageKey") String eventId) {

        log.info("Received event for orderId: {}", event.getOrderId());

        if (processedEventRepository.existsById(eventId)) {
            log.warn("Event {} already processed, skipping", eventId);
            return;
        }

        orderRepository.findById(event.getOrderId()).ifPresentOrElse(order -> {
            order.setStatus(OrderStatus.PROCESSING);
            orderRepository.save(order);
            log.info("Order {} status updated to PROCESSING", order.getId());
        }, () -> log.warn("Order {} not found", event.getOrderId()));

        processedEventRepository.save(ProcessedEvent.builder()
                .eventId(eventId)
                .eventType("OrderCreatedEvent")
                .build());

        log.info("Event {} marked as processed", eventId);
    }
}
