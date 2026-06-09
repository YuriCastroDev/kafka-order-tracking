package com.zs.kafka_order_tracking.consumer;

import com.zs.kafka_order_tracking.model.Order;
import com.zs.kafka_order_tracking.model.OrderStatus;
import com.zs.kafka_order_tracking.event.OrderCreatedEvent;
import com.zs.kafka_order_tracking.repository.OrderRepository;
import com.zs.kafka_order_tracking.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"order-events", "order-events.DLT"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=order-status-group"
})
class OrderConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    private Order savedOrder;

    @BeforeEach
    void setUp() {
        processedEventRepository.deleteAll();

        savedOrder = orderRepository.save(Order.builder()
                .customerName("Ana")
                .productName("Headset")
                .quantity(1)
                .price(300.00)
                .status(OrderStatus.CREATED)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build());
    }

    @Test
    void shouldConsumeEventAndUpdateOrderStatusToProcessing() {
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(savedOrder.getId())
                .customerName(savedOrder.getCustomerName())
                .productName(savedOrder.getProductName())
                .quantity(savedOrder.getQuantity())
                .price(savedOrder.getPrice())
                .status(OrderStatus.CREATED.name())
                .createdAt(savedOrder.getCreatedAt())
                .build();

        kafkaTemplate.send("order-events", savedOrder.getId().toString(), event);

        // Aguarda o consumer processar o evento (assíncrono)
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<Order> updated = orderRepository.findById(savedOrder.getId());
            assertThat(updated).isPresent();
            assertThat(updated.get().getStatus()).isEqualTo(OrderStatus.PROCESSING);
        });
    }

    @Test
    void shouldNotProcessDuplicateEvent() {
        String eventId = savedOrder.getId().toString();

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(savedOrder.getId())
                .customerName(savedOrder.getCustomerName())
                .productName(savedOrder.getProductName())
                .quantity(savedOrder.getQuantity())
                .price(savedOrder.getPrice())
                .status(OrderStatus.CREATED.name())
                .createdAt(savedOrder.getCreatedAt())
                .build();

        // Publica o mesmo evento duas vezes
        kafkaTemplate.send("order-events", eventId, event);
        kafkaTemplate.send("order-events", eventId, event);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            long count = processedEventRepository.count();
            // Deve ter processado apenas uma vez
            assertThat(count).isEqualTo(1);
        });
    }
}
