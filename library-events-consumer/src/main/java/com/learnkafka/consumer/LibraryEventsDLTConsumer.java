package com.learnkafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import com.learnkafka.service.LibraryEventsService;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;

@Component
@Slf4j
public class LibraryEventsDLTConsumer {

    @Autowired
    private LibraryEventsService libraryEventsService;

    @Autowired
    private FailureRecordRepository failureRecordRepository;

    @KafkaListener(topics = { "${topics.dlt}" }, groupId = "dlt-listener-group")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord in DLT Consumer: {} ", consumerRecord);
        try {
            libraryEventsService.processLibraryEvent(consumerRecord);
        } catch (Exception e) {
            FailureRecord failureRecord = new FailureRecord(
                    null,
                    consumerRecord.topic(),
                    consumerRecord.key(),
                    consumerRecord.value(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    e.getMessage(),
                    LibraryEventsConsumerConfig.DEAD);
            failureRecordRepository.save(failureRecord);
        }
    }
}
