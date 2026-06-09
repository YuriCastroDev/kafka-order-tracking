package com.zs.kafka_order_tracking.producer;

import com.zs.kafka_order_tracking.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private static final String TOPIC = "order-events";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public void publishOrderCreated(OrderCreatedEvent event) {
        log.info("Publishing OrderCreatedEvent for orderId: {}", event.getOrderId());
        kafkaTemplate.send(TOPIC, event.getOrderId().toString(), event);
        log.info("OrderCreatedEvent published successfully for orderId: {}", event.getOrderId());
    }
}
