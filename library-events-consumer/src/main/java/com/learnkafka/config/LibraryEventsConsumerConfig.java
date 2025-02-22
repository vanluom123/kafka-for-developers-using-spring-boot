package com.learnkafka.config;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String SUCCESS = "SUCCESS";
    public static final String DEAD = "DEAD";

    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterTopic;

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (r, e) -> {
            log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        });

        return recoverer;
    }

    public DefaultErrorHandler errorHandler() {
        // List of exceptions that should not be retried
        var exceptiopnToIgnorelist = List.of(
                IllegalArgumentException.class);

        // Configure retry interval (1000ms) and max retry attempts (2 times)
        var fixedBackOff = new FixedBackOff(1000L, 2L);

        // Create a new DefaultErrorHandler to handle errors in Kafka consumer
        // - publishingRecoverer(): Handles failed messages by sending them to retry
        // topic or DLT topic
        // - fixedBackOff: Configures retry interval (1000ms) and max retry attempts (2
        // times)
        var defaultErrorHandler = new DefaultErrorHandler(
                publishingRecoverer(),
                fixedBackOff);

        // Add exceptions that should not be retried to the default error handler
        exceptiopnToIgnorelist.forEach(defaultErrorHandler::addNotRetryableExceptions);

        // Set retry listeners to handle failed records
        defaultErrorHandler.setRetryListeners(
                (record, ex, deliveryAttempt) -> log.info(
                        "Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(),
                        deliveryAttempt));

        return defaultErrorHandler;
    }

    @Primary
    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(
                        () -> new DefaultKafkaConsumerFactory<>(this.kafkaProperties.buildConsumerProperties())));
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }
}