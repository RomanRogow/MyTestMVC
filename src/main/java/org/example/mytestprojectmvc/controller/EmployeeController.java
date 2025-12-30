package org.example.mytestprojectmvc.controller;

import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mytestprojectmvc.entity.DTO.EmployeeDTO;
import org.example.mytestprojectmvc.entity.Employee;
import org.example.mytestprojectmvc.scheduler.EmployeeBulkSyncToKafkaScheduler;
import org.example.mytestprojectmvc.service.EmployeeApiService;
import org.example.mytestprojectmvc.service.EmployeeCommandService;
import org.example.mytestprojectmvc.service.EmployeeQrCodeGenerator;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.List;

@Slf4j
@Controller
@RequestMapping("/employees")
@RequiredArgsConstructor
public class EmployeeController {

    private final EmployeeApiService employeeService;
    private final EmployeeCommandService commandService;
    private final EmployeeQrCodeGenerator qrCodeGenerator;
    private final HttpSession session; // Добавляем сессию
    private final EmployeeBulkSyncToKafkaScheduler bulkSyncScheduler;



    // 1. ГЛАВНАЯ СТРАНИЦА - список сотрудников
    @GetMapping
    public String showEmployees(Model model) {
        List<Employee> employees = employeeService.getAllEmployees();
        model.addAttribute("employees", employees);
        return "employees-list";
    }

    // 2. ФОРМА ДОБАВЛЕНИЯ
    @GetMapping("/add")
    public String showAddForm(Model model) {
        model.addAttribute("employeeDTO", new EmployeeDTO());
        return "add-employee";
    }

    // 3. ДОБАВЛЕНИЕ с выбором опции
    @PostMapping("/add")
    public String addEmployee(@ModelAttribute EmployeeDTO employeeDTO,
                              BindingResult bindingResult,
                              @RequestParam(defaultValue = "BOTH") String saveOption,
                              RedirectAttributes redirectAttributes) {

        if (bindingResult.hasErrors()) {
            log.warn("Ошибки валидации при добавлении сотрудника: {}", bindingResult.getAllErrors());
            return "add-employee";
        }

        try {
            // Выполняем команду сохранения
            Employee employee = commandService.addEmployee(employeeDTO, saveOption);

            // Формируем сообщение об успехе
            String message = getSuccessMessage(saveOption, employee);

            // Добавляем информацию о QR-коде если он сгенерирован
            if (employee.getPersonalCode() != null) {
                message += String.format(" (Personal Code: %s)", employee.getPersonalCode());
            }

            redirectAttributes.addFlashAttribute("success", message);

            log.info("Успешно добавлен сотрудник: {} {} (ID: {}, Personal Code: {})",
                    employee.getFirstName(), employee.getLastName(),
                    employee.getId(), employee.getPersonalCode());

        } catch (Exception e) {
            log.error("Ошибка при добавлении сотрудника", e);
            redirectAttributes.addFlashAttribute("error",
                    "Ошибка при добавлении сотрудника: " + e.getMessage());
        }

        return "redirect:/employees";
    }

    // 4. ПОИСК ПО ID в удалённом сервисе
    @GetMapping("/search")
    public String searchRemoteEmployee(@RequestParam Long id, Model model) {
        log.debug("Поиск удаленного сотрудника по ID: {}", id);

        try {
            Employee employee = employeeService.getRemoteEmployeeById(id);

            if (employee == null) {
                model.addAttribute("error", "Сотрудник с ID " + id + " не найден в удаленном сервисе");
                return "employee-details";
            }

            model.addAttribute("employee", employee);
            model.addAttribute("isRemoteEmployee", true); // Флаг что это удаленный сотрудник

            // Генерируем QR-код для отображения
            try {
                // Для удаленных сотрудников генерируем предпросмотр QR-кода
                byte[] qrCodeImage = qrCodeGenerator.generatePreviewQrCodeForEmployee(employee);
                String qrCodeBase64 = java.util.Base64.getEncoder().encodeToString(qrCodeImage);
                model.addAttribute("qrCodeBase64", qrCodeBase64);
                log.debug("Сгенерирован QR-код предпросмотра для удаленного сотрудника ID: {}", id);
            } catch (Exception e) {
                log.warn("Не удалось сгенерировать QR-код для удаленного сотрудника", e);
            }

            log.info("Найден удаленный сотрудник: {} {} (ID: {})",
                    employee.getFirstName(), employee.getLastName(), employee.getId());

        } catch (Exception e) {
            log.error("Ошибка при поиске удаленного сотрудника ID: {}", id, e);
            model.addAttribute("error", "Ошибка при подключении к удаленному сервису: " + e.getMessage());
        }

        return "employee-details";
    }

    // 5. ПРОСМОТР локального сотрудника (по ID)
    @GetMapping("/{id}")
    public String viewLocalEmployee(@PathVariable Long id, Model model) {
        log.debug("Просмотр локального сотрудника по ID: {}", id);

        try {
            Employee employee = employeeService.getLocalEmployeeById(id);

            if (employee == null) {
                model.addAttribute("error", "Сотрудник с ID " + id + " не найден в локальной БД");
                return "employee-details";
            }

            model.addAttribute("employee", employee);
            model.addAttribute("isLocalEmployee", true); // Флаг что это локальный сотрудник
            model.addAttribute("personalCode", employee.getPersonalCode()); // Табельный номер

            // Генерируем QR-код для отображения
            try {
                if (employee.getQrCodeData() != null) {
                    // Если QR-код уже сгенерирован и сохранен в БД
                    String qrCodeBase64 = qrCodeGenerator.generateQrCodeBase64ForEmployee(employee);
                    model.addAttribute("qrCodeBase64", qrCodeBase64);
                    log.debug("Использован сохраненный QR-код для сотрудника ID: {}", id);
                } else {
                    // Если QR-код не сгенерирован (старые записи)
                    log.warn("У сотрудника ID: {} отсутствует QR-код в БД, генерируем...", id);
                    employee = qrCodeGenerator.generateQrCodeForEmployee(employee);
                    String qrCodeBase64 = qrCodeGenerator.generateQrCodeBase64ForEmployee(employee);
                    model.addAttribute("qrCodeBase64", qrCodeBase64);

                    // Можно сохранить обновленного сотрудника в БД
                    // employeeService.save(employee);
                }
            } catch (Exception e) {
                log.error("Ошибка при генерации QR-кода для сотрудника ID: {}", id, e);
                model.addAttribute("qrError", "Не удалось сгенерировать QR-код");
            }

            log.info("Показан локальный сотрудник: {} {} (ID: {}, Personal Code: {})",
                    employee.getFirstName(), employee.getLastName(),
                    employee.getId(), employee.getPersonalCode());

        } catch (Exception e) {
            log.error("Ошибка при получении локального сотрудника ID: {}", id, e);
            model.addAttribute("error", "Ошибка при получении данных сотрудника: " + e.getMessage());
        }

        return "employee-details";
    }

    // 6. ПОИСК СОТРУДНИКА ПО ТАБЕЛЬНОМУ НОМЕРУ (Personal Code)
    @GetMapping("/by-code/{personalCode}")
    public String viewEmployeeByPersonalCode(@PathVariable String personalCode, Model model) {
        log.debug("Поиск сотрудника по табельному номеру: {}", personalCode);

        try {
            // Нужно добавить метод в EmployeeApiService для поиска по personalCode
            // Employee employee = employeeService.getEmployeeByPersonalCode(personalCode);

            // Временная заглушка - ищем в списке
            List<Employee> allEmployees = employeeService.getAllEmployees();
            Employee employee = allEmployees.stream()
                    .filter(e -> personalCode.equals(e.getPersonalCode()))
                    .findFirst()
                    .orElse(null);

            if (employee == null) {
                model.addAttribute("error", "Сотрудник с табельным номером '" + personalCode + "' не найден");
                return "employee-details";
            }

            // Перенаправляем на страницу просмотра по ID
            return "redirect:/employees/" + employee.getId();

        } catch (Exception e) {
            log.error("Ошибка при поиске сотрудника по табельному номеру: {}", personalCode, e);
            model.addAttribute("error", "Ошибка при поиске сотрудника: " + e.getMessage());
            return "employee-details";
        }
    }

    // 7. УДАЛЕНИЕ СОТРУДНИКА
    @PostMapping("/delete/{id}")
    public String deleteEmployee(@PathVariable Long id,
                                 RedirectAttributes redirectAttributes) {
        log.info("Удаление сотрудника с ID: {}", id);

        try {
            // Сначала получаем сотрудника чтобы удалить файл QR-кода
            Employee employee = employeeService.getLocalEmployeeById(id);

            // Удаляем сотрудника из БД
            employeeService.deleteLocalEmployee(id);

            redirectAttributes.addFlashAttribute("success",
                    String.format("✅ Сотрудник с ID %d удалён (Personal Code: %s)",
                            id, employee != null ? employee.getPersonalCode() : "N/A"));

            log.info("Успешно удален сотрудник ID: {}", id);

        } catch (Exception e) {
            log.error("Ошибка при удалении сотрудника ID: {}", id, e);
            redirectAttributes.addFlashAttribute("error",
                    "Ошибка при удалении сотрудника: " + e.getMessage());
        }

        return "redirect:/employees";
    }

    // 8. ПОЛУЧЕНИЕ QR-КОДА КАК ИЗОБРАЖЕНИЯ (PNG)
    @GetMapping("/{id}/qrcode")
    public ResponseEntity<byte[]> getEmployeeQrCode(@PathVariable Long id) {
        log.debug("Запрос QR-кода для сотрудника ID: {}", id);

        try {
            Employee employee = employeeService.getLocalEmployeeById(id);

            if (employee == null) {
                return ResponseEntity.notFound().build();
            }

            byte[] qrCodeBytes;
            String fileName;

            if (employee.getQrCodeData() != null && employee.getPersonalCode() != null) {
                // Используем сохраненный QR-код
                qrCodeBytes = qrCodeGenerator.generateQrCodeImageForEmployee(employee);
                fileName = String.format("employee_%s_qrcode.png", employee.getPersonalCode());
                log.debug("Использован сохраненный QR-код для скачивания");
            } else {
                // Генерируем новый QR-код
                qrCodeBytes = qrCodeGenerator.generatePreviewQrCodeForEmployee(employee);
                fileName = String.format("employee_%d_qrcode_preview.png", employee.getId());
                log.debug("Сгенерирован QR-код предпросмотра для скачивания");
            }

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION,
                            "inline; filename=\"" + fileName + "\"")
                    .contentType(MediaType.IMAGE_PNG)
                    .body(qrCodeBytes);

        } catch (Exception e) {
            log.error("Ошибка при генерации QR-кода для скачивания ID: {}", id, e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // 9. СКАНИРОВАНИЕ QR-КОДА (поиск сотрудника по данным из QR-кода)
    @PostMapping("/scan-qrcode")
    public String scanQrCode(@RequestParam String qrData,
                             RedirectAttributes redirectAttributes) {
        log.debug("Сканирование QR-кода: {}", qrData);

        try {
            // Парсим данные из QR-кода
            // Формат: "PERSONAL_CODE:IJ-543210-A7F|NAME:Иван Петров|DEPT:IT..."
            String personalCode = extractPersonalCodeFromQrData(qrData);

            if (personalCode == null) {
                redirectAttributes.addFlashAttribute("error",
                        "Не удалось распознать табельный номер в QR-коде");
                return "redirect:/employees";
            }

            // Ищем сотрудника по personalCode
            List<Employee> allEmployees = employeeService.getAllEmployees();
            Employee employee = allEmployees.stream()
                    .filter(e -> personalCode.equals(e.getPersonalCode()))
                    .findFirst()
                    .orElse(null);

            if (employee != null) {
                // Перенаправляем на страницу сотрудника
                redirectAttributes.addFlashAttribute("success",
                        String.format("Найден сотрудник: %s (Personal Code: %s)",
                                employee.getFullName(), employee.getPersonalCode()));
                return "redirect:/employees/" + employee.getId();
            } else {
                redirectAttributes.addFlashAttribute("error",
                        String.format("Сотрудник с табельным номером '%s' не найден", personalCode));
                return "redirect:/employees";
            }

        } catch (Exception e) {
            log.error("Ошибка при обработке сканированного QR-кода", e);
            redirectAttributes.addFlashAttribute("error",
                    "Ошибка при обработке QR-кода: " + e.getMessage());
            return "redirect:/employees";
        }
    }

    // ============= ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ =============

    private String getSuccessMessage(String saveOption, Employee employee) {
        return switch (saveOption.toUpperCase()) {
            case "LOCAL" -> String.format("✅ Сотрудник %s %s сохранён локально (ID: %d)",
                    employee.getFirstName(), employee.getLastName(), employee.getId());

            case "REMOTE" -> String.format("☁️ Сотрудник %s %s отправлен в удалённый сервис",
                    employee.getFirstName(), employee.getLastName());

            case "BOTH" -> String.format("🎯 Сотрудник %s %s добавлен в оба сервиса (ID: %d)",
                    employee.getFirstName(), employee.getLastName(), employee.getId());

            default -> String.format("Сотрудник %s %s добавлен",
                    employee.getFirstName(), employee.getLastName());
        };
    }

    /**
     * Извлечение personalCode из данных QR-кода
     * Формат: "PERSONAL_CODE:IJ-543210-A7F|NAME:Иван Петров|DEPT:IT..."
     */
    private String extractPersonalCodeFromQrData(String qrData) {
        try {
            if (qrData == null || qrData.isEmpty()) {
                return null;
            }

            // Разделяем по символу "|"
            String[] parts = qrData.split("\\|");

            for (String part : parts) {
                if (part.startsWith("PERSONAL_CODE:")) {
                    return part.substring("PERSONAL_CODE:".length());
                }
            }

            // Для обратной совместимости с старым форматом
            if (qrData.startsWith("ID:")) {
                // Старый формат: "ID:123|NAME:..."
                // Можно конвертировать или вернуть null
                log.warn("Обнаружен старый формат QR-кода: {}", qrData);
            }

            return null;

        } catch (Exception e) {
            log.error("Ошибка при парсинге QR данных: {}", qrData, e);
            return null;
        }
    }
    @PostMapping("/trigger-bulk")
    public String triggerBulkSync(Model model) {
        try {
            bulkSyncScheduler.triggerManualSync();

            // Устанавливаем флаг в сессии
            session.setAttribute("syncStarted", true);
            session.setAttribute("syncMessage", "Массовая синхронизация запущена успешно!");

        } catch (IllegalStateException e) {
            session.setAttribute("syncError", e.getMessage());
            log.warn("Попытка запустить уже выполняющуюся синхронизацию: {}", e.getMessage());
        } catch (Exception e) {
            session.setAttribute("syncError", "Ошибка при запуске синхронизации: " + e.getMessage());
            log.error("Ошибка при запуске синхронизации", e);
        }

        return "redirect:/employees";
    }

    @GetMapping("/sync-status")
    @ResponseBody
    public EmployeeBulkSyncToKafkaScheduler.SyncStatus getSyncStatus() {
        return bulkSyncScheduler.getSyncStatus();
    }

    @PostMapping("/stop-sync")
    @ResponseBody
    public String stopSync() {
        bulkSyncScheduler.stopSync();
        return "Синхронизация остановлена";
    }

    @GetMapping("/list")
    public String listEmployees(Model model) {
        model.addAttribute("employees", employeeService.getAllEmployees());

        // Добавляем сообщение о синхронизации, если она была запущена
        if (session.getAttribute("syncStarted") != null) {
            model.addAttribute("success", session.getAttribute("syncMessage"));
            session.removeAttribute("syncStarted");
            session.removeAttribute("syncMessage");
        }

        if (session.getAttribute("syncError") != null) {
            model.addAttribute("error", session.getAttribute("syncError"));
            session.removeAttribute("syncError");
        }

        // Добавляем текущий статус синхронизации
        model.addAttribute("syncStatus", bulkSyncScheduler.getSyncStatus());

        return "employees/list";
    }
}