package org.example.mytestprojectmvc.comand;

import org.example.mytestprojectmvc.entity.DTO.EmployeeDTO;
import org.example.mytestprojectmvc.entity.Employee;

public interface AddEmployeeCommand {
    Employee execute(EmployeeDTO employeeDTO);
}
