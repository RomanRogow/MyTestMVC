package org.example.mytestprojectmvc.entity;

import org.example.mytestprojectmvc.entity.DTO.EmployeeDTO;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.mapstruct.factory.Mappers;

@Mapper(componentModel = "spring")
public interface EmployeeMapper {

    EmployeeMapper INSTANCE = Mappers.getMapper(EmployeeMapper.class);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "qrCodeData", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    Employee toEntity(EmployeeDTO dto);

    // Правильный маппинг - если в DTO есть firstName и lastName
    @Mapping(source = "firstName", target = "firstName")
    @Mapping(source = "lastName", target = "lastName")
    EmployeeDTO toDTO(Employee employee);

    @Mapping(target = "id", ignore = true)
    void updateEntity(@MappingTarget Employee entity, EmployeeDTO dto);

}