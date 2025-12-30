package org.example.mytestprojectmvc.AppConfig;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topic.employee-sync}")
    private String employeeSyncTopic;

    @Value("${app.kafka.topic.employee-bulk-sync}")
    private String employeeBulkSyncTopic;

    @Value("${app.service.name}")
    private String serviceName;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class);

        // Основные настройки продюсера
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // Идентификатор клиента
        config.put(ProducerConfig.CLIENT_ID_CONFIG, serviceName + "-producer");

        // Настройки для Spring Kafka JsonSerializer
        // Если используете custom классы для сериализации
        config.put(JsonSerializer.TYPE_MAPPINGS,
                "employee:org.example.mytestprojectmvc.entity.Employee," +
                        "employeeKafkaDto:org.example.mytestprojectmvc.kafka.producer.EmployeeKafkaProducer$EmployeeKafkaDto," +
                        "employeeEvent:org.example.mytestprojectmvc.kafka.producer.EmployeeKafkaProducer$EmployeeEvent");

        // Дополнительные настройки Jackson, если нужно
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Топик для синхронизации отдельных событий
     */
    @Bean
    public NewTopic employeeSyncTopic() {
        return TopicBuilder.name(employeeSyncTopic)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "604800000") // 7 дней
                .build();
    }

    /**
     * Топик для массовой синхронизации (шедулер)
     */
    @Bean
    public NewTopic employeeBulkSyncTopic() {
        return TopicBuilder.name(employeeBulkSyncTopic)
                .partitions(5)
                .replicas(1)
                .config("retention.ms", "86400000") // 1 день
                .config("cleanup.policy", "delete")
                .build();
    }
}
