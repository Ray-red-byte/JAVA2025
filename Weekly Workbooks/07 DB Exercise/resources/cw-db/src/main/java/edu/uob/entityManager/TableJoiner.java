package edu.uob.entityManager;

public class TableJoiner {

    public static Table performJoin(Table leftTable, Table rightTable, String leftCol, String rightCol) throws Exception {
        int leftIndex = leftTable.getColumnNames().indexOf(leftCol);
        int rightIndex = rightTable.getColumnNames().indexOf(rightCol);

        if (leftIndex == -1) throw new Exception("JOIN column '" + leftCol + "' not found");
        if (rightIndex == -1) throw new Exception("JOIN column '" + rightCol + "' not found");

        Table joinedTable = new Table("temp_join_result");
        joinedTable.addColumn("id"); // Fresh ID column

        // Add headers from the left table with prefix
        for (int i = 1; i < leftTable.getColumnNames().size(); i++) {
            joinedTable.addColumn(leftTable.getTableName() + "." + leftTable.getColumnNames().get(i));
        }
        // Add headers from the right table with prefix
        for (int i = 1; i < rightTable.getColumnNames().size(); i++) {
            joinedTable.addColumn(rightTable.getTableName() + "." + rightTable.getColumnNames().get(i));
        }

        // Nested Loop to find matches
        for (Row leftRow : leftTable.getRows()) {
            String leftVal = (leftIndex == 0) ? String.valueOf(leftRow.getId()) : leftRow.getValues().get(leftIndex - 1);

            for (Row rightRow : rightTable.getRows()) {
                String rightVal = (rightIndex == 0) ? String.valueOf(rightRow.getId()) : rightRow.getValues().get(rightIndex - 1);

                if (leftVal.equals(rightVal)) {
                    Row newCombinedRow = new Row(joinedTable.getNextId()); // Auto-generates fresh ID

                    // Copy values (skipping old IDs)
                    for (String val : leftRow.getValues()) newCombinedRow.addValue(val);
                    for (String val : rightRow.getValues()) newCombinedRow.addValue(val);

                    joinedTable.addRow(newCombinedRow);
                }
            }
        }
        return joinedTable;
    }
}