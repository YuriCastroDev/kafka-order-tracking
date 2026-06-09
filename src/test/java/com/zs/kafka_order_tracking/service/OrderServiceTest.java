package com.zs.kafka_order_tracking.service;

import com.zs.kafka_order_tracking.dto.OrderRequest;
import com.zs.kafka_order_tracking.model.Order;
import com.zs.kafka_order_tracking.model.OrderStatus;
import com.zs.kafka_order_tracking.event.OrderCreatedEvent;
import com.zs.kafka_order_tracking.producer.OrderEventProducer;
import com.zs.kafka_order_tracking.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderEventProducer orderEventProducer;

    @InjectMocks
    private OrderService orderService;

    private OrderRequest request;
    private Order savedOrder;

    @BeforeEach
    void setUp() {
        request = new OrderRequest();
        request.setCustomerName("João");
        request.setProductName("Teclado");
        request.setQuantity(1);
        request.setPrice(150.00);

        savedOrder = Order.builder()
                .id(UUID.randomUUID())
                .customerName("João")
                .productName("Teclado")
                .quantity(1)
                .price(150.00)
                .status(OrderStatus.CREATED)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    @Test
    void shouldCreateOrderAndPublishEvent() {
        when(orderRepository.save(any(Order.class))).thenReturn(savedOrder);

        Order result = orderService.createOrder(request);

        assertThat(result).isNotNull();
        assertThat(result.getCustomerName()).isEqualTo("João");
        assertThat(result.getStatus()).isEqualTo(OrderStatus.CREATED);

        verify(orderRepository, times(1)).save(any(Order.class));
        verify(orderEventProducer, times(1)).publishOrderCreated(any(OrderCreatedEvent.class));
    }

    @Test
    void shouldGetOrderById() {
        when(orderRepository.findById(savedOrder.getId())).thenReturn(Optional.of(savedOrder));

        Order result = orderService.getOrder(savedOrder.getId());

        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(savedOrder.getId());
    }

    @Test
    void shouldThrowExceptionWhenOrderNotFound() {
        UUID randomId = UUID.randomUUID();
        when(orderRepository.findById(randomId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.getOrder(randomId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Order not found");
    }

    @Test
    void shouldUpdateOrderStatus() {
        when(orderRepository.findById(savedOrder.getId())).thenReturn(Optional.of(savedOrder));
        when(orderRepository.save(any(Order.class))).thenReturn(savedOrder);

        Order result = orderService.updateStatus(savedOrder.getId(), OrderStatus.SHIPPED);

        assertThat(result).isNotNull();
        verify(orderRepository, times(1)).save(any(Order.class));
    }
}
