package org.example.mytestprojectmvc.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mytestprojectmvc.entity.Employee;
import org.example.mytestprojectmvc.exceptions.EmployeeNotFoundException;
import org.example.mytestprojectmvc.exceptions.ExternalApiException;
import org.example.mytestprojectmvc.repository.EmployeeRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmployeeApiService {

    private final RestTemplate restTemplate;
    private final EmployeeRepository repository;

    @Value("${employee.api.base-url}")
    private String remoteApiBaseUrl;

    /**
     * Получить всех сотрудников из локальной БД
     */
    public List<Employee> getAllEmployees() {
        return repository.findAll();
    }

    /**
     * Найти сотрудника по ID в локальной БД
     */
    public Employee getLocalEmployeeById(Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new EmployeeNotFoundException("Локальный сотрудник с ID: {} не найден.", id));
    }

    /**
     * Получить сотрудника из удалённого сервиса по ID
     * и сохранить в локальную БД
     */
    @Transactional
    public Employee getRemoteEmployeeById(Long id) throws ExternalApiException {
        String url = UriComponentsBuilder.fromUriString(remoteApiBaseUrl)
                .path("/api/employees/{id}")
                .buildAndExpand(id)
                .toUriString();

        log.info("Запрос сотрудника по ID {} c URL {}", id, url);

        try {
            Employee employee = restTemplate.getForObject(url, Employee.class);

            if (employee != null) {
                log.info("Получен сотрудник {} {}", employee.getFirstName(), employee.getLastName());

                // Проверяем на дубликаты
                Optional<Employee> duplicate = repository.findAll().stream()
                        .filter(e -> e.equals(employee))
                        .findFirst();

                if (duplicate.isPresent()) {
                    log.info("Дубликат найден, ID: {}", duplicate.get().getId());
                    return duplicate.get();
                }

                return repository.save(employee);
            } else {
                log.warn("Сотрудник с ID {} не найден", id);
                throw new EmployeeNotFoundException("Сотрудник с ID: {} не найден.", id);
            }
        } catch (Exception e) {
            log.error("Ошибка при получении сотрудника с ID {}: {}", id, e.getMessage());
            throw new ExternalApiException("Ошибка соединения с внешним API или запись отсутствует.", e);
        }
    }

    /**
     * Удалить сотрудника из локальной БД
     */
    @Transactional
    public boolean deleteLocalEmployee(Long id) {
        try {
            if (repository.existsById(id)) {
                // Можно сначала получить сотрудника для логов
                Optional<Employee> employee = repository.findById(id);
                employee.ifPresent(emp -> {
                    log.info("Удаление сотрудника: {} {} (ID: {})",
                            emp.getFirstName(), emp.getLastName(), emp.getId());
                });

                repository.deleteById(id);
                log.info("Сотрудник с ID {} удалён из локальной БД", id);
                return true;
            } else {
                log.warn("Сотрудник с ID {} не найден в локальной БД", id);
                return false;
            }
        } catch (Exception e) {
            log.error("Ошибка при удалении сотрудника с ID {}: {}", id, e.getMessage());
            throw new RuntimeException("Не удалось удалить сотрудника: " + e.getMessage(), e);
        }
    }

    public Employee save(Employee employee) {
        return repository.save(employee);
    }
}
