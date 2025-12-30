package org.example.mytestprojectmvc.repository;

import org.example.mytestprojectmvc.entity.Employee;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Long> {

    List<Employee> findByQrCodeImageIsNull();

    boolean existsByFirstNameAndLastNameAndAgeAndDepartment(
            String firstName, String lastName, int age, String department);

    @Query("SELECT e FROM Employee e ORDER BY e.id")
    List<Employee> findAllWithPagination(@Param("page") int page, @Param("size") int size);


}
