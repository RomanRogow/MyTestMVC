package org.example.mytestprojectmvc.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Base64;

@Entity
@Data
@Table(name = "employees")
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Employee {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "personal_code", unique = true, nullable = false)
    private String personalCode;

    @Column(name = "first_name", nullable = false)
    private String firstName;

    @Column(name = "last_name", nullable = false)
    private String lastName;

    @Column(name = "age", nullable = false)
    private Integer age;

    @Column(name = "department", nullable = false)
    private String department;

    @Column(name = "post")
    private String post;

    @Column(name = "qr_data", columnDefinition = "TEXT")
    private String qrCodeData;

    @Column(name = "qr_code_image", columnDefinition = "BYTEA")
    private byte[] qrCodeImage;

    @Column(name = "qr_code_base64", columnDefinition = "TEXT")
    private String qrCodeBase64;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    // ИСПРАВЛЕНО: правильное имя колонки для флага
    @Column(name = "synced_to_kafka", nullable = false)
    @Builder.Default
    private Boolean syncedToKafka = false;

    @Column(name = "kafka_sync_date")
    private LocalDateTime kafkaSyncDate;

    @PrePersist
    protected void onCreate(){
        if (personalCode == null){
            personalCode = this.generatePersonalCode();
        }
        if(createdAt == null){
            createdAt = LocalDateTime.now();
        }
        updatedAt = LocalDateTime.now();

        // Инициализируем флаг синхронизации
        if (syncedToKafka == null) {
            syncedToKafka = false;
        }
    }

    @PreUpdate
    protected void onUpdate(){
        updatedAt = LocalDateTime.now();
    }

    public String generatePersonalCode(){
        long timestamp = System.currentTimeMillis() / 1000;
        String random = Integer.toHexString((int) (Math.random() * 65536)).toUpperCase();
        return String.format("EMP-%d-%s", timestamp, random);
    }

    /**
     * Установка QR-кода в Base64 формате
     */
    public void setQrCodeFromBytes(byte[] qrCodeBytes) {
        this.qrCodeImage = qrCodeBytes;
        if (qrCodeBytes != null) {
            this.qrCodeBase64 = "data:image/png;base64," +
                    Base64.getEncoder().encodeToString(qrCodeBytes);
        } else {
            this.qrCodeBase64 = null;
        }
    }

    /**
     * Получение Base64 строки QR-кода
     */
    public String getQrCodeBase64() {
        if (this.qrCodeBase64 != null) {
            return this.qrCodeBase64;
        }
        if (this.qrCodeImage != null) {
            return "data:image/png;base64," +
                    Base64.getEncoder().encodeToString(this.qrCodeImage);
        }
        return null;
    }

    public String getFullName(){
        return firstName + " " + lastName;
    }

    /**
     * Метод для пометки сотрудника как синхронизированного с Kafka
     */
    public void markAsSyncedToKafka() {
        this.syncedToKafka = true;
        this.kafkaSyncDate = LocalDateTime.now();
    }

    /**
     * Метод для сброса флага синхронизации (если нужно переотправить)
     */
    public void resetKafkaSyncFlag() {
        this.syncedToKafka = false;
        this.kafkaSyncDate = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Employee employee = (Employee) o;
        return firstName.equals(employee.firstName) &&
                lastName.equals(employee.lastName) &&
                age.equals(employee.age) &&
                department.equals(employee.department) &&
                (post == null ? employee.post == null : post.equals(employee.post));
    }

    @Override
    public int hashCode() {
        int result = firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + age.hashCode();
        result = 31 * result + department.hashCode();
        result = 31 * result + (post != null ? post.hashCode() : 0);
        return result;
    }
}