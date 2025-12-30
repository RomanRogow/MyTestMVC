package org.example.mytestprojectmvc.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.mytestprojectmvc.comand.AddEmployeeCommand;
import org.example.mytestprojectmvc.comand.AddEmployeeCommandFactory;
import org.example.mytestprojectmvc.entity.DTO.EmployeeDTO;
import org.example.mytestprojectmvc.entity.Employee;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmployeeCommandService {

    private final AddEmployeeCommandFactory commandFactory;

    public Employee addEmployee(EmployeeDTO employeeDTO, String saveOption) {
        AddEmployeeCommand command = commandFactory.getCommand(saveOption);
        return command.execute(employeeDTO);
    }
}
