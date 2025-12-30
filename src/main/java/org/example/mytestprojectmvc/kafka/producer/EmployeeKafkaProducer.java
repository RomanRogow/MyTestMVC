package org.example.mytestprojectmvc.kafka.producer;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.example.mytestprojectmvc.entity.Employee;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topic.employee-sync}")
    private String employeeSyncTopic;

    @Value("${app.kafka.topic.employee-bulk-sync}")
    private String employeeBulkSyncTopic;

    @Value("${app.service.name}")
    private String serviceName;

    /**
     * Отправляет событие о создании/обновлении сотрудника в Kafka
     */
    public void sendEmployeeEvent(Employee employee, String eventType, String targetTopic) {
        try {
            // Создаем ключ для партиционирования (например, по ID или отделу)
            String key = employee.getDepartment() != null ?
                    employee.getDepartment().hashCode() + "" :
                    "default";

            // Создаем DTO для отправки (исключаем бинарные данные)
            EmployeeKafkaDto kafkaDto = EmployeeKafkaDto.fromEntity(employee);

            EmployeeEvent event = EmployeeEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .eventType(eventType)
                    .timestamp(java.time.LocalDateTime.now())
                    .employee(kafkaDto)
                    .sourceService(serviceName)
                    .build();

            // Отправляем асинхронно с обработкой результата
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(targetTopic, key, event);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("✅ Employee event sent successfully. Type: {}, Employee ID: {}, Topic: {}, Partition: {}",
                            eventType,
                            employee.getId(),
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition());
                } else {
                    log.error("❌ Failed to send employee event. Employee ID: {}, Error: {}",
                            employee.getId(), ex.getMessage());
                    // Здесь можно добавить retry логику
                }
            });

        } catch (Exception e) {
            log.error("❌ Error sending employee event to Kafka. Employee ID: {}", employee.getId(), e);
            // Не бросаем исключение, чтобы не ломать основной поток
        }
    }

    /**
     * Отправка события создания в основной топик
     */
    public void sendEmployeeCreated(Employee employee) {
        sendEmployeeEvent(employee, "EMPLOYEE_CREATED", employeeSyncTopic);
    }

    /**
     * Отправка события в топик для массовой синхронизации
     */
    public void sendEmployeeForBulkSync(Employee employee) {
        sendEmployeeEvent(employee, "EMPLOYEE_BULK_SYNC", employeeBulkSyncTopic);
    }

    /**
     * DTO для Kafka (без бинарных данных и лишней информации)
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EmployeeKafkaDto {
        private Long id;
        private String personalCode;
        private String firstName;
        private String lastName;
        private Integer age;
        private String department;
        private String post;
        private String qrCodeBase64; // Только base64, без бинарных данных
        private LocalDateTime createdAt;
        private LocalDateTime updatedAt;

        public static EmployeeKafkaDto fromEntity(Employee employee) {
            return EmployeeKafkaDto.builder()
                    .id(employee.getId())
                    .personalCode(employee.getPersonalCode())
                    .firstName(employee.getFirstName())
                    .lastName(employee.getLastName())
                    .age(employee.getAge())
                    .department(employee.getDepartment())
                    .post(employee.getPost())
                    .qrCodeBase64(employee.getQrCodeBase64())
                    .createdAt(employee.getCreatedAt())
                    .updatedAt(employee.getUpdatedAt())
                    .build();
        }
    }

    /**
     * Событие для Kafka
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EmployeeEvent {
        private String eventId;
        private String eventType;
        private LocalDateTime timestamp;
        private EmployeeKafkaDto employee;
        private String sourceService;
    }
}
