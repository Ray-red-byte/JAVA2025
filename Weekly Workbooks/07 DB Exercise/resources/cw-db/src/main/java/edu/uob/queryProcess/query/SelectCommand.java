package edu.uob.queryProcess.query;

import edu.uob.DatabaseManager;
import edu.uob.entityManager.Row;
import edu.uob.entityManager.Table;
import edu.uob.queryProcess.ConditionEvaluator;
import edu.uob.queryProcess.DBCommand;

import java.util.ArrayList;
import java.util.List;

public class SelectCommand extends DBCommand {
    public SelectCommand(String fullCommand, String[] tokens) { super(fullCommand, tokens); }

    @Override
    public String execute(DatabaseManager dbManager) {
        if (dbManager.getCurrentDatabase() == null) return "[ERROR] No database selected";

        int fromIndex = -1;
        for (int i = 0; i < tokens.length; i++) {
            if (tokens[i].equalsIgnoreCase("FROM")) {
                fromIndex = i;
                break;
            }
        }
        if (fromIndex == -1 || fromIndex + 1 >= tokens.length) return "[ERROR] Invalid SELECT format";

        String tableName = tokens[fromIndex + 1].toLowerCase();
        Table table = dbManager.getTable(tableName);
        if (table == null) return "[ERROR] Table " + tableName + " does not exist";

        // 1. Figure out which columns were requested (between SELECT and FROM)
        String colsSubstring = fullCommand.substring(fullCommand.toUpperCase().indexOf("SELECT") + 6, fullCommand.toUpperCase().indexOf(" FROM ")).trim();
        boolean selectAll = colsSubstring.equals("*");
        String[] requestedCols = colsSubstring.split("\\s*,\\s*"); // Split by comma

        // Parse WHERE clause if exists
        int whereIndex = fullCommand.toUpperCase().indexOf(" WHERE ");
        ConditionEvaluator evaluator = null;
        if (whereIndex != -1) {
            try {
                evaluator = new ConditionEvaluator(fullCommand.substring(whereIndex + 7).trim());
            } catch (IllegalArgumentException e) {
                return "[ERROR] " + e.getMessage();
            }
        }

        StringBuilder result = new StringBuilder("[OK]\n");
        List<String> tableCols = table.getColumnNames();
        List<Integer> colIndices = new ArrayList<>();

        // 2. Build the Header
        if (selectAll) {
            result.append(String.join("\t", tableCols)).append("\n");
        } else {
            List<String> validReqCols = new ArrayList<>();
            for (String col : requestedCols) {
                int idx = tableCols.indexOf(col);
                if (idx != -1) {
                    validReqCols.add(col);
                    colIndices.add(idx);
                } else {
                    return "[ERROR] Column " + col + " does not exist";
                }
            }
            result.append(String.join("\t", validReqCols)).append("\n");
        }

        // 3. Build the Rows
        for (Row row : table.getRows()) {
            if (evaluator == null || evaluator.matches(row, table)) {
                if (selectAll) {
                    result.append(row.toString()).append("\n");
                } else {
                    List<String> rowOutput = new ArrayList<>();
                    for (int idx : colIndices) {
                        if (idx == 0) { // ID column is a special case since it's not in the values array
                            rowOutput.add(String.valueOf(row.getId()));
                        } else {
                            rowOutput.add(row.getValues().get(idx - 1));
                        }
                    }
                    result.append(String.join("\t", rowOutput)).append("\n");
                }
            }
        }

        return result.toString().trim();
    }
}