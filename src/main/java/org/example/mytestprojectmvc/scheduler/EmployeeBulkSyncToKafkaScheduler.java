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

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmployeeBulkSyncToKafkaScheduler {

    private final EmployeeRepository employeeRepository;
    private final EmployeeKafkaProducer employeeKafkaProducer;

    @Value("${app.kafka.bulk-sync.enabled:true}")
    private boolean bulkSyncEnabled;

    // Флаги и счетчики для статуса
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger totalCount = new AtomicInteger(0);
    private volatile String currentStatus = "NOT_STARTED";
    private volatile String currentMessage = "";

    @Scheduled(
            initialDelayString = "${app.kafka.bulk-sync.initial-delay}",
            fixedDelayString = "${app.kafka.bulk-sync.fixed-delay}"
    )
    @Transactional
    public void syncAllEmployeesToKafkaScheduled() {

        log.info("Запуск синхронизации в кафку по шедулеру, время: {}", LocalDateTime.now());
        syncAllEmployeesToKafka();
    }

    @Transactional
    public synchronized void syncAllEmployeesToKafka() {
        if (!bulkSyncEnabled) {
            log.info("Массовая синхронизация отключена в настройках");
            currentStatus = "DISABLED";
            return;
        }

        // Проверяем, не выполняется ли уже синхронизация
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("❌ Синхронизация уже выполняется, новый запуск невозможен");
            throw new IllegalStateException("Синхронизация уже выполняется");
        }

        // Сбрасываем счетчики
        processedCount.set(0);
        totalCount.set(0);
        currentStatus = "RUNNING";
        currentMessage = "Начало синхронизации...";

        StopWatch stopWatch = StopWatch.createStarted();
        log.info("🚀 Начало массовой синхронизации сотрудников в Kafka");

        try {
            // Получаем общее количество сотрудников
            long totalEmployees = employeeRepository.count();
            totalCount.set((int) totalEmployees);
            log.info("Общее количество записей в БД: {}", totalEmployees);

            if (totalEmployees == 0) {
                log.info("В БД нет записей для синхронизации");
                currentStatus = "NO_DATA";
                currentMessage = "Нет сотрудников для синхронизации";
                return;
            }

            currentMessage = String.format("Найдено %d сотрудников для обработки", totalEmployees);

            boolean hasMore = true;
            int processed = 0;

            while (hasMore && !Thread.currentThread().isInterrupted()) {
                // Получаем сотрудников, которых еще не синхронизировали
                List<Employee> employees = employeeRepository.findAllSyncedToKafkaIsFalse();

                if (employees.isEmpty()) {
                    log.debug("Не переданных в кафку записей нет.");
                    hasMore = false;
                    break;
                }

                List<Long> employeeIds = employees.stream()
                        .map(Employee::getId)
                        .collect(Collectors.toList());
                log.info("НАЙДЕНЫ СОТРУДНИКИ ДЛЯ СИНХРОНИЗАЦИИ В КАФКУ С ID: {}", employeeIds);

                log.info("Обрабатываем список из {} сотрудников, которые не переданы в Kafka", employees.size());
                currentMessage = String.format("Обработка пачки из %d сотрудников", employees.size());

                // Отправляем каждого сотрудника в Kafka
                for (Employee employee : employees) {
                    if (Thread.currentThread().isInterrupted()) {
                        log.info("Получен сигнал прерывания, останавливаем синхронизацию");
                        currentStatus = "STOPPED";
                        currentMessage = "Синхронизация остановлена пользователем";
                        return;
                    }

                    try {
                        employeeKafkaProducer.sendEmployeeForBulkSync(employee);
                        processed++;
                        processedCount.set(processed);

                        // Обновляем статус в реальном времени
                        if (processed % 5 == 0) {
                            float progress = (float) processed / totalEmployees * 100;
                            currentMessage = String.format("Обработано %d из %d (%.1f%%)",
                                    processed, totalEmployees, progress);
                            log.info("Прогресс синхронизации: {}/{} ({}%)",
                                    processed, totalEmployees, String.format("%.1f", progress));
                        }

                        // Небольшая пауза для корректной работы UI
                        Thread.sleep(10);

                    } catch (Exception e) {
                        log.error("Ошибка при отправке сотрудника ID: {} в Kafka. Ошибка: {}",
                                employee.getId(), e.getMessage());
                        // Продолжаем со следующим сотрудником
                    }
                }

                // Обновляем статус для обработанных сотрудников
                log.info("Обновляем статус сотрудников c ID: {}", employeeIds);
                employeeRepository.updateSyncStatusByIds(employeeIds);
                log.info("Обновлен статус для {} сотрудников", employeeIds.size());

                // Проверяем, есть ли еще необработанные сотрудники
                long remainingCount = employeeRepository.countBySyncedToKafkaFalse();
                log.info("Проверка, есть ли еще необработанные сотрудники: {}", remainingCount);
                if (remainingCount == 0) {
                    hasMore = false;
                    log.info("Все сотрудники обработаны");
                }
            }

            stopWatch.stop();

            if (Thread.currentThread().isInterrupted()) {
                currentStatus = "STOPPED";
                currentMessage = String.format("Синхронизация остановлена. Обработано: %d/%d",
                        processed, totalEmployees);
            } else {
                currentStatus = "COMPLETED";
                currentMessage = String.format("Синхронизация завершена успешно. Обработано: %d/%d сотрудников. Время: %d мс",
                        processed, totalEmployees, stopWatch.getTime());
            }

            log.info("Передача записей в кафку завершена. Обработано: {}/{}. Время выполнения: {} мс.",
                    processed, totalEmployees, stopWatch.getTime());

        } catch (Exception e) {
            currentStatus = "FAILED";
            currentMessage = "Ошибка при синхронизации: " + e.getMessage();
            log.error("❌ Критическая ошибка при массовой синхронизации: {}", e.getMessage(), e);
        } finally {
            // Всегда сбрасываем флаг выполнения
            isRunning.set(false);
        }
    }

    /**
     * Ручной запуск синхронизации с проверкой
     */
    public synchronized void triggerManualSync() {
        log.info("🔄 Запрос на ручной запуск синхронизации сотрудников");

        if (isRunning.get()) {
            log.warn("⚠️ Синхронизация уже выполняется, повторный запуск невозможен");
            throw new IllegalStateException("Синхронизация уже выполняется");
        }

        // Сбрасываем старые данные
        processedCount.set(0);
        totalCount.set(0);
        currentStatus = "STARTING";
        currentMessage = "Подготовка к синхронизации...";

        // Запускаем в отдельном потоке
        Thread syncThread = new Thread(() -> {
            try {
                this.syncAllEmployeesToKafka();
            } catch (Exception e) {
                log.error("Ошибка при выполнении ручной синхронизации", e);
                currentStatus = "FAILED";
                currentMessage = "Ошибка: " + e.getMessage();
                isRunning.set(false);
            }
        }, "kafka-sync-thread");

        syncThread.setDaemon(true);
        syncThread.start();

        log.info("✅ Запущена фоновая синхронизация в потоке: {}", syncThread.getName());
    }

    /**
     * Получить статус текущей синхронизации
     */
    public SyncStatus getSyncStatus() {
        return new SyncStatus(
                isRunning.get(),
                currentStatus,
                processedCount.get(),
                totalCount.get(),
                currentMessage
        );
    }

    /**
     * Остановить текущую синхронизацию
     */
    public void stopSync() {
        if (isRunning.get()) {
            log.info("🛑 Остановка текущей синхронизации");

            // Прерываем поток синхронизации
            Thread.getAllStackTraces().keySet().stream()
                    .filter(thread -> "kafka-sync-thread".equals(thread.getName()))
                    .forEach(Thread::interrupt);

            currentStatus = "STOPPING";
            currentMessage = "Остановка синхронизации...";
        }
    }

    /**
     * Проверяет, выполняется ли сейчас синхронизация
     */
    public boolean isSyncRunning() {
        return isRunning.get();
    }

    @Data
    @AllArgsConstructor
    public static class SyncStatus {
        private boolean running;
        private String status;
        private int processedCount;
        private int totalCount;
        private String message;

        public SyncStatus() {
            this.running = false;
            this.status = "NOT_STARTED";
            this.processedCount = 0;
            this.totalCount = 0;
            this.message = "";
        }
    }
}