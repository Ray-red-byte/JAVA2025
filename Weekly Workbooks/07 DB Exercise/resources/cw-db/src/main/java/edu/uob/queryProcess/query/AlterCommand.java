package edu.uob.queryProcess.query;

import edu.uob.DatabaseManager;
import edu.uob.entityManager.Row;
import edu.uob.entityManager.Table;
import edu.uob.queryProcess.DBCommand;

import java.io.File;
import java.io.IOException;

public class AlterCommand extends DBCommand {
    public AlterCommand(String fullCommand, String[] tokens) { super(fullCommand, tokens); }

    @Override
    public String execute(DatabaseManager dbManager) {
        if (dbManager.getCurrentDatabase() == null) return "[ERROR] No database selected";
        if (tokens.length < 5 || !tokens[1].equalsIgnoreCase("TABLE")) return "[ERROR] Invalid ALTER format";

        String tableName = tokens[2].toLowerCase();
        String action = tokens[3].toUpperCase(); // ADD or DROP
        String columnName = tokens[4];

        Table table = dbManager.getTable(tableName);
        if (table == null) return "[ERROR] Table does not exist";

        if (action.equals("ADD")) {
            // BLOCK RESERVED KEYWORDS
            if (isReservedKeyword(columnName)) {
                return "[ERROR] Cannot use reserved keyword '" + columnName + "' as a column name";
            }

            if (table.getColumnNames().contains(columnName)) return "[ERROR] Column already exists";
            table.addColumn(columnName);
            for (Row row : table.getRows()) {
                row.addValue(""); // Add empty value for existing rows
            }
        } else if (action.equals("DROP")) {
            int colIndex = table.getColumnNames().indexOf(columnName);
            if (colIndex <= 0) return "[ERROR] Cannot drop column (Does not exist or is ID)";

            table.getColumnNames().remove(colIndex);
            for (Row row : table.getRows()) {
                row.getValues().remove(colIndex - 1);
            }
        } else {
            return "[ERROR] Invalid ALTER action: " + action;
        }

        try {
            table.saveToFile(dbManager.getStorageFolderPath() + File.separator + dbManager.getCurrentDatabase());
            return "[OK] Table altered successfully";
        } catch (IOException e) {
            return "[ERROR] " + e.getMessage();
        }
    }
}