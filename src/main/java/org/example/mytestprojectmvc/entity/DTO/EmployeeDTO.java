package org.example.mytestprojectmvc.entity.DTO;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.*;
import lombok.Data;

@Data
public class EmployeeDTO {

    private Long id;

    @Pattern(regexp = "^[A-Z0-9-]{3,20}$", message = "Табельный номер должен содержать только буквы, цифры и дефисы")
    @Nullable
    private String personalCode;

    @NotBlank(message = "Имя обязательно")
    @Size(min = 1, max = 50, message = "Имя должно быть от 1 до 50 символов")
    private String firstName;

    @NotBlank(message = "Фамилия обязательна")
    @Size(min = 1, max = 50, message = "Фамилия должна быть от 1 до 50 символов")
    private String lastName;

    @NotBlank(message = "Отдел обязателен")
    private String department;

    @NotNull(message = "Возраст обязателен")
    @Min(value = 18, message = "Возраст должен быть не менее 18 лет")
    @Max(value = 100, message = "Возраст должен быть не более 100 лет")
    private Integer age;

    private String post;

    @Nullable
    private String qrCodeBase64;
}
