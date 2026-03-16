package edu.uob.queryProcess.query;
import edu.uob.DatabaseManager;
import edu.uob.entityManager.Row;
import edu.uob.entityManager.Table;
import edu.uob.queryProcess.ConditionEvaluator;
import edu.uob.queryProcess.DBCommand;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class DeleteCommand extends DBCommand {
    public DeleteCommand(String fullCommand, String[] tokens) { super(fullCommand, tokens); }

    @Override
    public String execute(DatabaseManager dbManager) {
        if (dbManager.getCurrentDatabase() == null) return "[ERROR] No database selected";

        String tableName = tokens[2].toLowerCase();
        Table table = dbManager.getTable(tableName);
        if (table == null) return "[ERROR] Table does not exist";

        int whereIndex = fullCommand.toUpperCase().indexOf(" WHERE ");
        if (whereIndex == -1) return "[ERROR] DELETE requires WHERE clause";

        try {
            // Use the Evaluator you built to handle the condition logic!
            ConditionEvaluator evaluator = new ConditionEvaluator(fullCommand.substring(whereIndex + 7).trim());
            ArrayList<Row> rowsToDelete = new ArrayList<>();

            for (Row row : table.getRows()) {
                if (evaluator.matches(row, table)) {
                    rowsToDelete.add(row);
                }
            }

            table.getRows().removeAll(rowsToDelete);
            table.saveToFile(dbManager.getStorageFolderPath() + File.separator + dbManager.getCurrentDatabase());
            return "[OK] Deleted " + rowsToDelete.size() + " rows.";

        } catch (IllegalArgumentException | IOException e) {
            return "[ERROR] " + e.getMessage();
        }
    }
}