package org.example.mytestprojectmvc.comand;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mytestprojectmvc.entity.DTO.EmployeeDTO;
import org.example.mytestprojectmvc.entity.Employee;
import org.example.mytestprojectmvc.entity.EmployeeMapper;
import org.example.mytestprojectmvc.kafka.producer.EmployeeKafkaProducer;
import org.example.mytestprojectmvc.repository.EmployeeRepository;
import org.example.mytestprojectmvc.service.EmployeeQrCodeGenerator;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AddLocalCommand implements AddEmployeeCommand {

    private final EmployeeRepository employeeRepository;
    private final EmployeeMapper mapper;
    private final EmployeeQrCodeGenerator qrCodeGenerator;
    private final EmployeeKafkaProducer kafkaProducer;

    @Override
    @Transactional
    public Employee execute(EmployeeDTO employeeDTO) {
        log.debug("Начало сохранения струдника: {} {} ",
                employeeDTO.getLastName(), employeeDTO.getFirstName())
        ;
        // Конвертируем DTO в сущность
        Employee employee = mapper.toEntity(employeeDTO);

        employee = qrCodeGenerator.generateAndSaveQrCodeToDb(employee);

        Employee savedEmployee = employeeRepository.save(employee);

        try {
            String qrCodeBase64 = qrCodeGenerator.generateQrCodeBase64ForEmployee(savedEmployee);
            employeeDTO.setQrCodeBase64(qrCodeBase64);
            employeeDTO.setId(savedEmployee.getId());
            employeeDTO.setPersonalCode(savedEmployee.getPersonalCode());
        } catch (Exception e) {
            log.warn("Не удалось обновить DTO: {}", e.getMessage());
        }

        log.info("✅ Сотрудник успешно создан: {} {} (ID: {}, Personal Code: {})",
                savedEmployee.getFirstName(),
                savedEmployee.getLastName(),
                savedEmployee.getId(),
                savedEmployee.getPersonalCode());

        sendToKafka(savedEmployee);

        return savedEmployee;
    }

    private void sendToKafka(Employee employee) {
        try {
            kafkaProducer.sendEmployeeCreated(employee);
            log.info("📤 Событие о создании сотрудника отправлено в Kafka. Employee ID: {}",
                    employee.getId());
        } catch (Exception e) {
            log.error("❌ Ошибка при отправке в Kafka для сотрудника ID: {}. Ошибка: {}",
                    employee.getId(), e.getMessage());
        }

    }
}
