package org.example.mytestprojectmvc.comand;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AddEmployeeCommandFactory {

    private final AddLocalCommand addLocalCommand;
    private final AddRemoteCommand addRemoteCommand;
    private final AddBothCommand addBothCommand;

    public AddEmployeeCommand getCommand(String command) {
        return switch (command.toUpperCase()) {
            case "LOCAL" -> addLocalCommand;
            case "REMOTE" -> addRemoteCommand;
            case "BOTH" -> addBothCommand;
            default -> throw new IllegalArgumentException("Неизвестная опция: " + command);
        };
    }
}