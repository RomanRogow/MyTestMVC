package org.example.mytestprojectmvc.comand;

import lombok.RequiredArgsConstructor;
import org.example.mytestprojectmvc.entity.DTO.EmployeeDTO;
import org.example.mytestprojectmvc.entity.Employee;
import org.example.mytestprojectmvc.entity.EmployeeMapper;
import org.example.mytestprojectmvc.repository.EmployeeRepository;
import org.example.mytestprojectmvc.service.EmployeeQrCodeGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

@Component
@RequiredArgsConstructor
public class AddRemoteCommand implements AddEmployeeCommand{

    private final RestTemplate restTemplate;
    private final EmployeeMapper mapper;
    private final EmployeeQrCodeGenerator qrCodeGenerator;
    private final EmployeeRepository employeeRepository;

    @Value("${employee.api.base-url}")
    private String baseUrl;

    @Override
    public Employee execute(EmployeeDTO employeeDTO) {
        Employee employee = mapper.toEntity(employeeDTO);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

        HttpEntity<Employee> request = new HttpEntity<>(employee, headers);

        ResponseEntity<Employee> response = restTemplate.exchange(
                baseUrl + "/api/employees",
                HttpMethod.POST,
                request,
                Employee.class
        );
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("Error creating employee");
        }

        Employee emp = response.getBody();

        emp = qrCodeGenerator.generateAndSaveQrCodeToDb(emp);

        return emp;
    }
}
