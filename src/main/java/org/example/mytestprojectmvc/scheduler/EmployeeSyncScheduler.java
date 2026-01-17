package org.example.mytestprojectmvc.scheduler;

import jakarta.transaction.Transactional;
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

import java.util.*;
import java.util.stream.Collectors;

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

    @Scheduled(fixedRate = 300000)
    public void syncEmployees() {
        log.info("Запускаю синхронизацию сотрудников...");

        try {
            Employee[] remoteEmployees = restTemplate.getForObject(url, Employee[].class);

            if (remoteEmployees == null || remoteEmployees.length == 0) {
                log.warn("Не получилось получить сотрудников");
            }

            log.info("Получил {} сотрудников", remoteEmployees.length);

            this.saveNewEmployees(remoteEmployees);

        } catch (RestClientException e) {
            log.error("Ошибка при вызове API: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Неожиданная ошибка синхронизации: {}", e.getMessage(), e);
        }
    }

    @Transactional
    protected void saveNewEmployees(Employee[] remoteEmployees) {
        if (remoteEmployees == null || remoteEmployees.length == 0) {
            log.info("Нет сотрудников для сохранения");
            return;
        }

        log.info("Начинаю сохранение {} сотрудников", remoteEmployees.length);

        // 1. Преобразуем массив в список
        List<Employee> employeesToSave = Arrays.asList(remoteEmployees);

        // 2. Проверяем, какие сотрудники уже существуют
        List<Employee> existingEmployees = findExistingEmployees(employeesToSave);

        // 3. Фильтруем новых сотрудников
        List<Employee> newEmployees = filterNewEmployees(employeesToSave, existingEmployees);

        if (newEmployees.isEmpty()) {
            log.info("Все сотрудники уже существуют в БД");
            return;
        }

        log.info("Найдено {} новых сотрудников для сохранения", newEmployees.size());

        // 4. Сохраняем новых сотрудников пакетно
        List<Employee> savedEmployees = repository.saveAll(newEmployees);

        // 5. Генерируем QR-коды для новых сотрудников
        List<Employee> employeesWithQr = generateQrCodesForEmployees(savedEmployees);

        // 6. Сохраняем сотрудников с QR-кодами
        repository.saveAll(employeesWithQr);

        log.info("Успешно сохранено {} новых сотрудников", savedEmployees.size());
    }

    // Метод для поиска существующих сотрудников одним запросом
    private List<Employee> findExistingEmployees(List<Employee> employees) {
        if (employees.isEmpty()) {
            return Collections.emptyList();
        }

        // Собираем уникальные ключи для проверки
        List<String> firstNames = new ArrayList<>();
        List<String> lastNames = new ArrayList<>();
        List<Integer> ages = new ArrayList<>();
        List<String> departments = new ArrayList<>();

        for (Employee emp : employees) {
            firstNames.add(emp.getFirstName());
            lastNames.add(emp.getLastName());
            ages.add(emp.getAge());
            departments.add(emp.getDepartment());
        }

        // Выполняем один запрос для проверки всех сотрудников
        return repository.findExistingEmployeesBatch(firstNames, lastNames, ages, departments);
    }

    // Метод для фильтрации новых сотрудников
    private List<Employee> filterNewEmployees(List<Employee> allEmployees, List<Employee> existingEmployees) {
        // Создаем Set существующих сотрудников для быстрой проверки
        Set<String> existingKeys = new HashSet<>();
        for (Employee emp : existingEmployees) {
            String key = createEmployeeKey(emp);
            existingKeys.add(key);
        }

        // Фильтруем только новых
        List<Employee> newEmployees = new ArrayList<>();
        for (Employee emp : allEmployees) {
            String key = createEmployeeKey(emp);
            if (!existingKeys.contains(key)) {
                newEmployees.add(emp);
            }
        }

        return newEmployees;
    }

    // Создание уникального ключа для сотрудника
    private String createEmployeeKey(Employee employee) {
        return String.format("%s|%s|%d|%s",
                employee.getFirstName(),
                employee.getLastName(),
                employee.getAge(),
                employee.getDepartment());
    }

    // Генерация QR-кодов для списка сотрудников
    private List<Employee> generateQrCodesForEmployees(List<Employee> employees) {
        return employees.stream()
                .map(qrCodeGenerator::generateQrCodeForEmployee)
                .collect(Collectors.toList());
    }
}
