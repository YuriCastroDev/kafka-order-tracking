package com.zs.kafka_order_tracking.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zs.kafka_order_tracking.model.FailedEvent;
import com.zs.kafka_order_tracking.repository.FailedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterConsumer {

    private final FailedEventRepository failedEventRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order-events.DLT", groupId = "order-dlt-group")
    public void consume(ConsumerRecord<String, Object> record) {
        log.error("Message arrived at DLT — topic: {}, key: {}", record.topic(), record.key());

        try {
            String payload = objectMapper.writeValueAsString(record.value());

            FailedEvent failedEvent = FailedEvent.builder()
                    .topic(record.topic())
                    .eventKey(record.key())
                    .payload(payload)
                    .errorMessage("Message sent to DLT after exhausting retries")
                    .build();

            failedEventRepository.save(failedEvent);
            log.error("Failed event persisted to database for manual review. Key: {}", record.key());

        } catch (Exception e) {
            log.error("Error persisting failed event: {}", e.getMessage());
        }
    }
}
