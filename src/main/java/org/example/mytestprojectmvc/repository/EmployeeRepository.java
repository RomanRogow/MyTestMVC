package org.example.mytestprojectmvc.repository;

import org.example.mytestprojectmvc.entity.Employee;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Long> {

    List<Employee> findByQrCodeImageIsNull();

    @Query("SELECT e FROM Employee e WHERE " +
            "e.firstName IN :firstNames AND " +
            "e.lastName IN :lastNames AND " +
            "e.age IN :ages AND " +
            "e.department IN :departments")
    List<Employee> findExistingEmployeesBatch(
            @Param("firstNames") List<String> firstNames,
            @Param("lastNames") List<String> lastNames,
            @Param("ages") List<Integer> ages,
            @Param("departments") List<String> departments);

    @Query("SELECT e from Employee e where e.syncedToKafka = false")
    List<Employee> findAllSyncedToKafkaIsFalse();

    @Modifying
    @Query("UPDATE Employee e SET e.syncedToKafka = true, e.kafkaSyncDate = CURRENT_TIMESTAMP WHERE e.id IN :ids")
    @Transactional
    int updateSyncStatusByIds(@Param("ids") List<Long> ids);

    @Query("SELECT COUNT(e) FROM Employee e WHERE e.syncedToKafka = false")
    long countBySyncedToKafkaFalse();
}
