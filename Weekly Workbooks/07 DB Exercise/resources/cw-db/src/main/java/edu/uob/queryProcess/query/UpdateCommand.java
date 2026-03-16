package edu.uob.queryProcess.query;

import edu.uob.DatabaseManager;
import edu.uob.entityManager.Row;
import edu.uob.entityManager.Table;
import edu.uob.queryProcess.ConditionEvaluator;
import edu.uob.queryProcess.DBCommand;

import java.io.File;
import java.io.IOException;

public class UpdateCommand extends DBCommand {
    public UpdateCommand(String fullCommand, String[] tokens) { super(fullCommand, tokens); }

    @Override
    public String execute(DatabaseManager dbManager) {
        if (dbManager.getCurrentDatabase() == null) return "[ERROR] No database selected";
        if (tokens.length < 6 || !tokens[2].equalsIgnoreCase("SET")) return "[ERROR] Invalid UPDATE format";

        String tableName = tokens[1].toLowerCase();
        Table table = dbManager.getTable(tableName);
        if (table == null) return "[ERROR] Table does not exist";

        int whereIndex = fullCommand.toUpperCase().indexOf(" WHERE ");
        if (whereIndex == -1) return "[ERROR] UPDATE requires WHERE clause";

        // Parse SET clause (e.g., SET Name = 'Bob')
        String setClause = fullCommand.substring(fullCommand.toUpperCase().indexOf(" SET ") + 5, whereIndex).trim();
        String[] setParts = setClause.split("=");
        if (setParts.length != 2) return "[ERROR] Invalid SET format";

        String updateCol = setParts[0].trim();
        String updateVal = setParts[1].trim().replace("'", "");

        int updateColIndex = table.getColumnNames().indexOf(updateCol);
        if (updateColIndex <= 0) return "[ERROR] Cannot update column " + updateCol + " (Does not exist or is ID)";

        try {
            ConditionEvaluator evaluator = new ConditionEvaluator(fullCommand.substring(whereIndex + 7).trim());
            int updatedCount = 0;

            for (Row row : table.getRows()) {
                if (evaluator.matches(row, table)) {
                    row.getValues().set(updateColIndex - 1, updateVal);
                    updatedCount++;
                }
            }

            table.saveToFile(dbManager.getStorageFolderPath() + File.separator + dbManager.getCurrentDatabase());
            return "[OK] Updated " + updatedCount + " rows";

        } catch (IllegalArgumentException | IOException e) {
            return "[ERROR] " + e.getMessage();
        }
    }
}