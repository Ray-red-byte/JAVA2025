package edu.uob.queryProcess.query;

import edu.uob.DatabaseManager;
import edu.uob.entityManager.Table;
import edu.uob.queryProcess.DBCommand;

import java.io.File;
import java.io.IOException;

public class CreateCommand extends DBCommand {
    public CreateCommand(String fullCommand, String[] tokens) { super(fullCommand, tokens); }

    @Override
    public String execute(DatabaseManager dbManager) {
        if (tokens.length < 3) return "[ERROR] Invalid CREATE format";

        String entityType = tokens[1].toUpperCase();
        String entityName = tokens[2].toLowerCase();

        // BLOCK RESERVED KEYWORDS
        if (isReservedKeyword(entityName)) {
            return "[ERROR] Cannot use reserved keyword '" + entityName + "' as a name";
        }

        if (entityType.equals("DATABASE")) {
            File dbDir = new File(dbManager.getStorageFolderPath() + File.separator + entityName);
            if (dbDir.exists()) return "[ERROR] Database already exists";
            if (dbDir.mkdir()) return "[OK] Database " + entityName + " created successfully";
            return "[ERROR] Failed to create database";

        } else if (entityType.equals("TABLE")) {
            if (dbManager.getCurrentDatabase() == null || dbManager.getCurrentDatabase().isEmpty()) {
                return "[ERROR] No database selected";
            }
            if (dbManager.getTable(entityName) != null) return "[ERROR] Table already exists";

            Table newTable = new Table(entityName);
            newTable.addColumn("id"); // Always need an ID column

            // Check if there are columns specified in parenthesis
            if (fullCommand.contains("(") && fullCommand.contains(")")) {
                int start = fullCommand.indexOf("(");
                int end = fullCommand.lastIndexOf(")");
                String[] cols = fullCommand.substring(start + 1, end).split(",");
                for (String col : cols) {
                    String cleanCol = col.trim();
                    if (isReservedKeyword(cleanCol)) {
                        return "[ERROR] Cannot use reserved keyword '" + cleanCol + "' as a column name";
                    }
                    // TASK 9 FIX: Check for duplicate columns
                    if (newTable.getColumnNames().contains(cleanCol)) {
                        return "[ERROR] Duplicate column name '" + cleanCol + "' is not allowed";
                    }
                    newTable.addColumn(cleanCol);
                }
            }

            dbManager.addTable(entityName, newTable);
            try {
                newTable.saveToFile(dbManager.getStorageFolderPath() + File.separator + dbManager.getCurrentDatabase());
                return "[OK] Table " + entityName + " created successfully";
            } catch (IOException e) {
                return "[ERROR] " + e.getMessage();
            }
        }
        return "[ERROR] Unknown entity type: " + entityType;
    }
}