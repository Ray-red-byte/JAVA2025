package edu.uob.queryProcess;

import edu.uob.queryProcess.query.*;

public class QueryParser {
    public static DBCommand parse(String command) throws IllegalArgumentException {
        if (command == null || command.trim().isEmpty()) {
            throw new IllegalArgumentException("[ERROR] Command is empty");
        }

        String cleanedCommand = preprocessCommand(command);
        String[] tokens = cleanedCommand.split("\\s+");
        String trigger = tokens[0].toUpperCase();

        switch (trigger) {
            case "USE": return new UseCommand(cleanedCommand, tokens);
            case "CREATE": return new CreateCommand(cleanedCommand, tokens);
            case "INSERT": return new InsertCommand(cleanedCommand, tokens);
            case "SELECT": return new SelectCommand(cleanedCommand, tokens);
            case "UPDATE": return new UpdateCommand(cleanedCommand, tokens);
            case "DELETE": return new DeleteCommand(cleanedCommand, tokens);
            case "ALTER": return new AlterCommand(cleanedCommand, tokens);
            case "DROP": return new DropCommand(cleanedCommand, tokens);
            default:
                throw new IllegalArgumentException("[ERROR] Unknown command: " + trigger);
        }
    }

    private static String preprocessCommand(String command) {
        String cleanedCommand = command.trim();
        if (cleanedCommand.endsWith(";")) {
            cleanedCommand = cleanedCommand.substring(0, cleanedCommand.length() - 1);
        }
        return cleanedCommand;
    }
}