package org.example.mytestprojectmvc.exceptions;

public class EmployeeNotFoundException extends RuntimeException {
    public EmployeeNotFoundException(String message, Object id) {
        super(message, (Throwable) id);
    }
}
