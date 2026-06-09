package com.zs.kafka_order_tracking.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "failed_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FailedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String topic;
    private String eventKey;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private String errorMessage;
    private LocalDateTime failedAt;

    @PrePersist
    public void prePersist() {
        this.failedAt = LocalDateTime.now();
    }
}
