package com.learnkafka.scheduler;

import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {

    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    @Scheduled(fixedRate = 100000)
    public void retryFailedRecords() {
        log.info("Retrying Failed Records Started!");
        var status = LibraryEventsConsumerConfig.DEAD;
        failureRecordRepository.findAllByStatus(status)
                .forEach(failureRecord -> {
                    try {
                        var consumerRecord = buildConsumerRecord(failureRecord);
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecords : {}", e.getMessage());
                    }
                });
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic(),
                failureRecord.getPartition(), failureRecord.getOffsetValue(), failureRecord.getKey(),
                failureRecord.getErrorRecord());
    }
}
