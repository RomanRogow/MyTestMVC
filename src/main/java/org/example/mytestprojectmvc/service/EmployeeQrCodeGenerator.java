package org.example.mytestprojectmvc.service;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.EncodeHintType;
import com.google.zxing.client.j2se.MatrixToImageConfig;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mytestprojectmvc.entity.Employee;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.EnumMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmployeeQrCodeGenerator {

    @Value("${qr.code.width:250}")
    private int width;

    @Value("${qr.code.height:250}")
    private int height;

    private final QRCodeWriter qrCodeWriter = new QRCodeWriter();

    /**
     * Основной метод: Генерация QR-кода для сотрудника (с personalCode)
     */
    public Employee generateQrCodeForEmployee(Employee employee) {
        try {
            // 1. Проверяем/генерируем personalCode если его нет
            if (employee.getPersonalCode() == null || employee.getPersonalCode().isEmpty()) {
                employee.setPersonalCode(generatePersonalCode());
            }

            // 2. Генерируем данные для QR-кода
            String qrData = generateEmployeeQrData(employee);
            employee.setQrCodeData(qrData);

            byte[] qrCodeImage = generateQrCode(qrData);

            employee.setQrCodeFromBytes(qrCodeImage);

            log.info("QR-код сгенерирован для сотрудника: {} (Personal Code: {})",
                    employee.getFullName(), employee.getPersonalCode());

            employee.setUpdatedAt(LocalDateTime.now());

            return employee;

        } catch (Exception e) {
            log.error("Ошибка при генерации QR-кода для сотрудника: {}",
                    employee.getFullName(), e);
            throw new RuntimeException("Не удалось сгенерировать QR-код", e);
        }
    }

    /**
     * Генерация изображения QR-кода для сотрудника (без сохранения в файл)
     */
    public byte[] generateQrCodeImageForEmployee(Employee employee) throws Exception {
        String qrData = generateEmployeeQrData(employee);
        return generateQrCode(qrData);
    }

    /**
     * Генерация Base64 строки QR-кода для сотрудника
     */
    public String generateQrCodeBase64ForEmployee(Employee employee) throws Exception {
        String qrData = generateEmployeeQrData(employee);
        byte[] qrCodeBytes = generateQrCode(qrData);
        return Base64.getEncoder().encodeToString(qrCodeBytes);
    }

    // ============= НОВЫЕ МЕТОДЫ ДЛЯ ПРЕДПРОСМОТРА =============

    /**
     * Генерация QR-кода для предпросмотра (без personalCode, для удаленных сотрудников)
     * Используется когда сотрудник еще не сохранен в локальной БД
     */
    public byte[] generatePreviewQrCodeForEmployee(Employee employee) throws Exception {
        String qrData = generatePreviewEmployeeQrData(employee);
        return generateQrCode(qrData);
    }
    // ============= ПРИВАТНЫЕ ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ =============

    /**
     * Генерация данных для QR-кода (основной, с personalCode)
     */
    private String generateEmployeeQrData(Employee employee) {
        return String.format(
                "PERSONAL_CODE:%s|NAME:%s %s|DEPT:%s|AGE:%d|POST:%s|TIMESTAMP:%d",
                employee.getPersonalCode() != null ? employee.getPersonalCode() : "N/A",
                employee.getFirstName(),
                employee.getLastName(),
                employee.getDepartment(),
                employee.getAge() != null ? employee.getAge() : 0,
                employee.getPost() != null ? employee.getPost() : "N/A",
                System.currentTimeMillis() / 1000
        );
    }

    /**
     * Генерация данных для предпросмотра (без personalCode)
     */
    private String generatePreviewEmployeeQrData(Employee employee) {
        return String.format(
                "NAME:%s %s|DEPT:%s|AGE:%d|POST:%s|ID:%d",
                employee.getFirstName(),
                employee.getLastName(),
                employee.getDepartment(),
                employee.getAge() != null ? employee.getAge() : 0,
                employee.getPost() != null ? employee.getPost() : "N/A",
                employee.getId() != null ? employee.getId() : 0
        );
    }

    /**
     * Генерация персонального кода
     */
        public String generatePersonalCode() {
            long timestamp = System.currentTimeMillis() / 1000;
            String random = Integer.toHexString((int) (Math.random() * 65536)).toUpperCase();
            return String.format("EMP-%d-%s", timestamp, random);
        }

    /**
     * Базовая генерация QR-кода из строки данных
     */
    private byte[] generateQrCode(String data) throws Exception {
        Map<EncodeHintType, Object> hints = new EnumMap<>(EncodeHintType.class);
        hints.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.H);
        hints.put(EncodeHintType.MARGIN, 1);
        hints.put(EncodeHintType.CHARACTER_SET, "UTF-8");

        BitMatrix bitMatrix = qrCodeWriter.encode(
                data,
                BarcodeFormat.QR_CODE,
                width,
                height,
                hints
        );

        MatrixToImageConfig config = new MatrixToImageConfig(
                Color.BLACK.getRGB(),
                Color.WHITE.getRGB()
        );

        BufferedImage bufferedImage = MatrixToImageWriter.toBufferedImage(bitMatrix, config);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bufferedImage, "PNG", baos);

        return baos.toByteArray();
    }

    /**
     * Генерация QR-кода с немедленным сохранением в БД
     * Используется когда нужно сразу сохранить в репозитории
     */
    public Employee generateAndSaveQrCodeToDb(Employee employee) {
        try {
            // Генерируем QR-код
            employee = generateQrCodeForEmployee(employee);

            // Логируем успешное сохранение
            log.debug("QR-код подготовлен для сохранения в БД для сотрудника ID: {}",
                    employee.getId() != null ? employee.getId() : "NEW");

            return employee;

        } catch (Exception e) {
            log.error("Ошибка при подготовке QR-кода для сохранения в БД", e);
            throw e;
        }
    }
}
