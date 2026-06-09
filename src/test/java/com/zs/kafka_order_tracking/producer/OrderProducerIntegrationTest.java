package com.zs.kafka_order_tracking.producer;

import com.zs.kafka_order_tracking.event.OrderCreatedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatNoException;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"order-events"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class OrderProducerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Test
    void shouldPublishEventToKafkaWithoutErrors() {
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(UUID.randomUUID())
                .customerName("Carlos")
                .productName("Monitor")
                .quantity(1)
                .price(1200.00)
                .status("CREATED")
                .createdAt(LocalDateTime.now())
                .build();

        assertThatNoException().isThrownBy(() ->
                kafkaTemplate.send("order-events", event.getOrderId().toString(), event).get()
        );
    }
}