package org.example.mytestprojectmvc.scheduler;

import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.example.mytestprojectmvc.entity.Employee;
import org.example.mytestprojectmvc.kafka.producer.EmployeeKafkaProducer;
import org.example.mytestprojectmvc.repository.EmployeeRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeBulkSyncToKafkaScheduler {

    private final EmployeeRepository employeeRepository;
    private final EmployeeKafkaProducer employeeKafkaProducer;

    @Value("${app.kafka.bulk-sync.enabled:true}")
    private boolean bulkSyncEnabled;

    // Флаг для контроля выполнения
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    // Счетчик для отслеживания количества отправленных сообщений
    private volatile long lastSyncCount = 0;
    private volatile String lastSyncStatus = "NOT_STARTED";

    @Scheduled(
            initialDelayString = "${app.kafka.bulk-sync.initial-delay}",
            fixedDelayString = "${app.kafka.bulk-sync.fixed-delay}"
    )
    @Transactional
    public void syncAllEmployeesToKafkaScheduled() {
        if (!bulkSyncEnabled) {
            log.info("Массовая синхронизация отключена в настройках");
            return;
        }

        if (isRunning.get()) {
            log.warn("Синхронизация уже выполняется, пропускаем запланированный запуск");
            return;
        }

        syncAllEmployeesToKafka();
    }

    @Transactional
    public synchronized void syncAllEmployeesToKafka() {
        if (!bulkSyncEnabled) {
            log.info("Массовая синхронизация отключена в настройках");
            return;
        }

        // Проверяем, не выполняется ли уже синхронизация
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("❌ Синхронизация уже выполняется, новый запуск невозможен");
            return;
        }

        StopWatch stopWatch = StopWatch.createStarted();
        log.info("🚀 Начало массовой синхронизации сотрудников в Kafka");

        try {
            long totalEmployees = employeeRepository.count();
            log.info("Общее количество записей в БД: {}", totalEmployees);

            if (totalEmployees == 0) {
                log.info("В БД нет записей для синхронизации");
                lastSyncStatus = "NO_DATA";
                return;
            }
            boolean hasMore = true;

            int processed = 0;
            lastSyncStatus = "IN_PROGRESS";

            while (hasMore && !Thread.currentThread().isInterrupted()) {
                List<Employee> employees = employeeRepository.findAllSyncedToKafkaIsFalse();
                List<Long> collect = employees.stream()
                        .map(Employee::getId)
                        .collect(Collectors.toList());
                employeeRepository.updateSyncStatusByIds(collect);

                if (employees.isEmpty()) {
                    log.debug("Не переданных в кафку записей нет.");
                    hasMore = false;
                }
                Long empCount = employees.stream()
                                .count();
                log.info("Обрабатываем список сотрудников, которые не переданы в Кафка {}", empCount);

                for (Employee employee : employees){
                employeeKafkaProducer.sendEmployeeForBulkSync(employee);
                processed++;

                    // Логируем прогресс каждые 5 сотрудников
                    if (processed % 5 == 0) {
                        log.info("Прогресс синхронизации: {}/{}", processed, totalEmployees);
                    }

                    Thread.sleep(10);

                    String threadName = Thread.currentThread().getName();
                    int threadId =(int) Thread.currentThread().getId();
                    log.info("Имя потока выполнения {}, Его ID {}", threadName, threadId);

                    Set<Thread> threads = Thread.getAllStackTraces().keySet();
                    for(Thread th : threads){
                        log.info(" {} - {} = {} - {} - {}",
                        th.getId(),
                        th.getName(),
                        th.getPriority(),
                        th.getState(),
                        th.getThreadGroup().getName());
                    }
                    log.info("Всего потоков {}", threads.size());

                }
                if (processed == collect.size()){
                    log.info("Передано {}/{} записей в кафку", processed, totalEmployees);
                }
            }
            stopWatch.stop();

            log.info("Передача записей в кафку завершина. " +
                    " Время выполнения {} мс.", stopWatch.getTime());
        } catch (Exception e) {
            lastSyncStatus = "FAILED: " + e.getMessage();
            log.error("❌ Критическая ошибка при массовой синхронизации: {}", e.getMessage(), e);
        } finally {
            // Всегда сбрасываем флаг выполнения
            isRunning.set(false);
        }

    }
//
//            int processed = 0;
//            int page = 0;
//            lastSyncStatus = "IN_PROGRESS";
//
//            // ОСНОВНОЙ ЦИКЛ ПАГИНАЦИИ
//            while (hasMore && !Thread.currentThread().isInterrupted()) {
//                // Получаем следующую пачку данных
//                List<Employee> employees = employeeRepository.findAllWithPagination(page, batchSize);
//
//                // Если пачка пустая - заканчиваем
//                if (employees.isEmpty()) {
//                    log.debug("Пачка {} пустая, завершение обработки", page + 1);
//                    hasMore = false;
//                    continue;
//                }
//
//                log.info("Обрабатываем пачку {}. Количество: {}", page + 1, employees.size());
//
//                // Отправляем каждого сотрудника из пачки в Kafka
//                for (Employee employee : employees) {
//                    try {
//                        employeeKafkaProducer.sendEmployeeForBulkSync(employee);
//                        processed++;
//
//                        // Логируем прогресс каждые 5 сотрудников
//                        if (processed % 5 == 0) {
//                            log.info("Прогресс синхронизации: {}/{}", processed, totalEmployees);
//                        }
//
//                        // Небольшая пауза между отправками, чтобы не засорять Kafka
//                        Thread.sleep(10); // 10ms пауза
//
//                    } catch (Exception e) {
//                        log.error("Ошибка при отправке сотрудника ID: {} в Kafka. Ошибка: {}",
//                                employee.getId(), e.getMessage());
//                        // Продолжаем со следующим сотрудником
//                    }
//                }
//
//                // Переходим к следующей странице
//                page++;
//
//                // Если полученная пачка меньше размера batch - значит это последняя пачка
//                if (employees.size() < batchSize) {
//                    log.debug("Полученная пачка меньше batch-size, это последняя пачка");
//                    hasMore = false;
//                }
//
//                // Небольшая пауза между пачками
//                if (hasMore) {
//                    Thread.sleep(50); // 50ms пауза между пачками
//                }
//            }
//
//            stopWatch.stop();
//            lastSyncCount = processed;
//            lastSyncStatus = "COMPLETED";
//
//            log.info("✅ Массовая синхронизация завершена УСПЕШНО. " +
//                            "Обработано: {}/{} сотрудников. Время выполнения: {} мс",
//                    processed, totalEmployees, stopWatch.getTime());
//
//        } catch (Exception e) {
//            lastSyncStatus = "FAILED: " + e.getMessage();
//            log.error("❌ Критическая ошибка при массовой синхронизации: {}", e.getMessage(), e);
//        } finally {
//            // Всегда сбрасываем флаг выполнения
//            isRunning.set(false);
//        }
//    }

    /**
     * Ручной запуск синхронизации с проверкой
     */
    public synchronized void triggerManualSync() {
        log.info("🔄 Запрос на ручной запуск синхронизации сотрудников");

        if (isRunning.get()) {
            log.warn("⚠️ Синхронизация уже выполняется, повторный запуск невозможен");
            throw new IllegalStateException("Синхронизация уже выполняется");
        }

        // Запускаем в отдельном потоке, но контролируем через флаг
        new Thread(() -> {
            try {
                syncAllEmployeesToKafka();

            } catch (Exception e) {
                log.error("Ошибка при выполнении ручной синхронизации", e);
            }
        }, "kafka-sync-thread").start();

        log.info("✅ Запущена фоновая синхронизация");
    }

    /**
     * Получить статус текущей синхронизации
     */
    public SyncStatus getSyncStatus() {
        return new SyncStatus(
                isRunning.get(),
                lastSyncStatus,
                lastSyncCount,
                employeeRepository.count()
        );
    }

    /**
     * Остановить текущую синхронизацию
     */
    public void stopSync() {
        if (isRunning.get()) {
            log.info("🛑 Остановка текущей синхронизации");
            Thread.currentThread().interrupt();
        }
    }

    @Data
    @AllArgsConstructor
    public static class SyncStatus {
        private boolean isRunning;
        private String status;
        private long processedCount;
        private long totalCount;
    }
}