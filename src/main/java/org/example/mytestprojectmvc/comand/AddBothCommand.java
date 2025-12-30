package org.example.mytestprojectmvc.comand;

import lombok.RequiredArgsConstructor;
import org.example.mytestprojectmvc.entity.DTO.EmployeeDTO;
import org.example.mytestprojectmvc.entity.Employee;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AddBothCommand implements AddEmployeeCommand {

    private final AddLocalCommand local;
    private final AddRemoteCommand remote;

    @Override
    public Employee execute(EmployeeDTO employeeDTO) {

        Employee localEmployee = local.execute(employeeDTO);

        remote.execute(employeeDTO);

        return localEmployee;
    }
}
