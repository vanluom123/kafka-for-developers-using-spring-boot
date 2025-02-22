package com.learnkafka.config;

import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

@Configuration
@Slf4j
public class LibraryEventsConsumerConfigLegacy {

    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    KafkaProperties kafkaProperties;

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
        CommonErrorHandler commonErrorHandler = new CommonErrorHandler() {
            @SuppressWarnings("unchecked")
            @Override
            public void handleRecord(Exception thrownException, ConsumerRecord<?, ?> data, Consumer<?, ?> consumer,
                    MessageListenerContainer container) {
                log.info("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), data);
                if (thrownException instanceof RecoverableDataAccessException) {
                    log.info("Inside the recoverable logic");
                    libraryEventsService.handleRecovery((ConsumerRecord<Integer, String>) data);
                } else {
                    log.info("Inside the non recoverable logic");
                    throw new RuntimeException(thrownException.getMessage());
                }
            }
        };
        factory.setCommonErrorHandler(commonErrorHandler);
        return factory;
    }
}