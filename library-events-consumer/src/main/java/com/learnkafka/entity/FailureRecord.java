package com.learnkafka.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
@Table(name = "failure_records")
public class FailureRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name = "topic")
    private String topic;

    @Column(name = "key")
    private Integer key;

    @Column(name = "error_record")
    private String errorRecord;

    @Column(name = "partition")
    private Integer partition;

    @Column(name = "offset_value")
    private Long offsetValue;

    @Column(name = "exception")
    private String exception;

    @Column(name = "status")
    private String status;
}
