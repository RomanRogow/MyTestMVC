package org.example.mytestprojectmvc.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.mytestprojectmvc.entity.Employee;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EmployeeEventDTO {
    private String eventId;
    private String eventType;
    private java.time.LocalDateTime timestamp;
    private Employee employee;
    private String sourceService;
}