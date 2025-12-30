package org.example.mytestprojectmvc.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mytestprojectmvc.entity.Employee;
import org.example.mytestprojectmvc.repository.EmployeeRepository;
import org.example.mytestprojectmvc.service.EmployeeQrCodeGenerator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class QrEmpLocalSync {

    private final EmployeeRepository employeeRepository;
    private final EmployeeQrCodeGenerator qrCodeGenerator;

    @Scheduled(fixedRate = 30000)
    public void updateQrLocalEmployee() {
        List<Employee> employee = employeeRepository.findByQrCodeImageIsNull();
        for (Employee emp : employee) {

            if(emp.getCreatedAt() == null){
                emp.setCreatedAt(LocalDateTime.now());
            }
            emp.setUpdatedAt(LocalDateTime.now());

            // 1. Сначала сохраняем personal code
            if (emp.getPersonalCode() == null || emp.getPersonalCode().trim().isEmpty()) {
                String persCode = emp.generatePersonalCode();
                emp.setPersonalCode(persCode);
                employeeRepository.saveAndFlush(emp); // Сразу сохраняем в БД
                log.info("Сохранен personal code: {}", persCode);
            }

            // 2. Потом генерируем QR
            if (emp.getQrCodeData() == null && emp.getQrCodeImage() == null) {
                qrCodeGenerator.generateQrCodeForEmployee(emp);
                employeeRepository.save(emp);
                log.info("Сгенерирован QR для: {}", emp.getPersonalCode());
            }
        }
    }
}
