package edu.uob.queryProcess.query;
import edu.uob.DatabaseManager;
import edu.uob.entityManager.Row;
import edu.uob.entityManager.Table;
import edu.uob.queryProcess.DBCommand;

import java.io.File;
import java.io.IOException;

public class InsertCommand extends DBCommand {
    public InsertCommand(String fullCommand, String[] tokens) { super(fullCommand, tokens); }

    @Override
    public String execute(DatabaseManager dbManager) {
        if (dbManager.getCurrentDatabase() == null || dbManager.getCurrentDatabase().isEmpty()) {
            return "[ERROR] No database selected";
        }
        String tableName = tokens[2].toLowerCase();
        Table table = dbManager.getTable(tableName);

        if (table == null) return "[ERROR] Table " + tableName + " does not exist";

        int start = fullCommand.indexOf("(");
        int end = fullCommand.lastIndexOf(")");
        if (start == -1 || end == -1) return "[ERROR] Missing values in parentheses";

        String[] values = fullCommand.substring(start + 1, end).split(",");

        // TASK 9 FIX: Check if they provided the correct number of values
        // We do size() - 1 because the user does not provide the 'id' value
        if (values.length != (table.getColumnNames().size() - 1)) {
            return "[ERROR] Incorrect number of values. Expected " + (table.getColumnNames().size() - 1) + " but got " + values.length;
        }

        Row newRow = new Row(table.getNextId());

        for (String val : values) {
            String cleanVal = val.trim();
            if (cleanVal.startsWith("'") && cleanVal.endsWith("'")) {
                cleanVal = cleanVal.substring(1, cleanVal.length() - 1);
            }
            newRow.addValue(cleanVal);
        }

        table.addRow(newRow);
        try {
            table.saveToFile(dbManager.getStorageFolderPath() + File.separator + dbManager.getCurrentDatabase());
            return "[OK]\nRecord inserted successfully";
        } catch (IOException e) {
            return "[ERROR] " + e.getMessage();
        }
    }
}