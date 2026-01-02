package org.example.mytestprojectmvc.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * КОНФИГУРАЦИЯ ШЕДУЛЕРА (ПЛАНИРОВЩИКА)
 *
 * Этот класс включает поддержку автоматического выполнения задач по расписанию.
 * Благодаря аннотации @EnableScheduling Spring будет автоматически запускать методы,
 * помеченные @Scheduled в определённое время.
 *
 * Аннотация @ConditionalOnProperty проверяет настройку из application.yaml:
 * - Если scheduling.enabled=true - конфигурация включается
 * - Если scheduling.enabled=false или отсутствует - конфигурация игнорируется
 */
@Configuration
@EnableScheduling  // ВКЛЮЧАЕМ поддержку шедулеров (планировщиков задач)
@ConditionalOnProperty(
        name = "scheduling.enabled",  // Имя свойства в application.yaml
        havingValue = "true",         // Ожидаемое значение
        matchIfMissing = true         // Если свойство отсутствует - считать true (включено по умолчанию)
)
public class SchedulerConfig {
}
