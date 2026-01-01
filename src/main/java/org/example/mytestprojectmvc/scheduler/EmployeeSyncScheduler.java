package org.example.mytestprojectmvc.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mytestprojectmvc.entity.Employee;
import org.example.mytestprojectmvc.repository.EmployeeRepository;
import org.example.mytestprojectmvc.service.EmployeeApiService;
import org.example.mytestprojectmvc.service.EmployeeQrCodeGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeSyncScheduler {

    private final RestTemplate restTemplate;
    private final EmployeeApiService service;
    private final EmployeeRepository repository;
    private final EmployeeQrCodeGenerator qrCodeGenerator;

    @Value("${employee.api.ful-name}")
    private String url;

    @Scheduled(fixedRate = 900000)
    public void syncEmployees() {
        log.info("Запускаю синхронизацию сотрудников...");

        try {
            Employee[] remoteEmployees = restTemplate.getForObject(url, Employee[].class);

            if (remoteEmployees == null || remoteEmployees.length == 0) {
                log.warn("Не получилось получить сотрудников");
            }

            log.info("Получил {} сотрудников", remoteEmployees.length);

            saveNewEmployees(remoteEmployees);

        } catch (RestClientException e) {
            log.error("Ошибка при вызове API: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Неожиданная ошибка синхронизации: {}", e.getMessage(), e);
        }
    }

    private void saveNewEmployees(Employee[] remoteEmployees) {
        int saved = 0;
        for (Employee remote : remoteEmployees) {
            if (!repository.existsByFirstNameAndLastNameAndAgeAndDepartment(
                    remote.getFirstName(),
                    remote.getLastName(),
                    remote.getAge(),
                    remote.getDepartment()
            )) {
                Employee savedEmployee = service.save(remote);

                savedEmployee = qrCodeGenerator.generateQrCodeForEmployee(savedEmployee);
                service.save(savedEmployee);

                saved++;
                log.debug("Сохранил: {} {}", remote.getFirstName(), remote.getLastName());
            }
        }
        log.info("Синхронизация завершена. Сохранено новых: {}", saved);
    }
}
