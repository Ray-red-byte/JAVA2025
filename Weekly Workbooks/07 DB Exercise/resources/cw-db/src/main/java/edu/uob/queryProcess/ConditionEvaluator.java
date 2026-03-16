package edu.uob.queryProcess;

import edu.uob.entityManager.Row;
import edu.uob.entityManager.Table;

public class ConditionEvaluator {
    private String columnName;
    private String expectedValue;
    private String operator;

    public ConditionEvaluator(String whereClause) throws IllegalArgumentException {
        // Define operators in order of length so ">=" is found before ">"
        String[] operators = {"==", "!=", ">=", "<=", ">", "<", "=", " LIKE "};

        for (String op : operators) {
            if (whereClause.toUpperCase().contains(op)) {
                // Use regex with Pattern.quote to safely split by the operator
                String[] parts = whereClause.split(java.util.regex.Pattern.quote(op) + "(?i)"); // (?i) makes LIKE case-insensitive for splitting
                if (parts.length == 2) {
                    this.columnName = parts[0].trim();
                    this.expectedValue = parts[1].trim();
                    this.operator = op.trim();

                    // Strip quotes from string literals
                    if (expectedValue.startsWith("'") && expectedValue.endsWith("'")) {
                        expectedValue = expectedValue.substring(1, expectedValue.length() - 1);
                    }
                    return;
                }
            }
        }
        throw new IllegalArgumentException("Invalid WHERE clause or unsupported operator.");
    }

    public boolean matches(Row row, Table table) {
        int colIndex = table.getColumnNames().indexOf(columnName);
        if (colIndex == -1) return false;

        String rowValue = (colIndex == 0) ? String.valueOf(row.getId()) : row.getValues().get(colIndex - 1);

        // Handle LIKE operator (Simple substring match)
        if (operator.equalsIgnoreCase("LIKE")) {
            return rowValue.contains(expectedValue);
        }

        // Handle standard operators (Try numerical first, fallback to string)
        try {
            float val1 = Float.parseFloat(rowValue);
            float val2 = Float.parseFloat(expectedValue);
            switch (operator) {
                case "==": case "=": return val1 == val2;
                case "!=": return val1 != val2;
                case ">": return val1 > val2;
                case ">=": return val1 >= val2;
                case "<": return val1 < val2;
                case "<=": return val1 <= val2;
            }
        } catch (NumberFormatException e) {
            // If they aren't numbers, do a string comparison
            int cmp = rowValue.compareTo(expectedValue);
            switch (operator) {
                case "==": case "=": return cmp == 0;
                case "!=": return cmp != 0;
                case ">": return cmp > 0;
                case ">=": return cmp >= 0;
                case "<": return cmp < 0;
                case "<=": return cmp <= 0;
            }
        }
        return false;
    }
}