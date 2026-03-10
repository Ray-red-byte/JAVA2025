package edu.uob;

public class TableJoiner {

    /**
     * Performs an inner join on two tables based on matching column values.
     */
    public static Table performJoin(Table leftTable, Table rightTable, String leftCol, String rightCol) throws Exception {
        // 1. Validate Columns
        int leftIndex = leftTable.getColumnNames().indexOf(leftCol);
        int rightIndex = rightTable.getColumnNames().indexOf(rightCol);

        if (leftIndex == -1) {
            throw new Exception("JOIN column '" + leftCol + "' not found in table '" + leftTable.getColumnNames() + "'");
        }
        if (rightIndex == -1) {
            throw new Exception("JOIN column '" + rightCol + "' not found in table '" + rightTable.getColumnNames() + "'");
        }

        // 2. Create the new temporary table
        Table joinedTable = new Table("temp_join_result");
        joinedTable.addColumn("id"); // Create a fresh ID column for the new table

        // Add headers from the left table (skipping its original ID)
        for (int i = 1; i < leftTable.getColumnNames().size(); i++) {
            joinedTable.addColumn(leftTable.getColumnNames().get(i));
        }
        // Add headers from the right table (skipping its original ID)
        for (int i = 1; i < rightTable.getColumnNames().size(); i++) {
            joinedTable.addColumn(rightTable.getColumnNames().get(i));
        }

        // 3. Perform the Nested Loop to find matches
        for (Row leftRow : leftTable.getRows()) {
            // Get the value to compare (handle ID column specially since it's not in the 'values' array)
            String leftVal = (leftIndex == 0) ? String.valueOf(leftRow.getId()) : leftRow.getValues().get(leftIndex - 1);

            for (Row rightRow : rightTable.getRows()) {
                String rightVal = (rightIndex == 0) ? String.valueOf(rightRow.getId()) : rightRow.getValues().get(rightIndex - 1);

                // If the values match, combine the rows!
                if (leftVal.equals(rightVal)) {
                    Row newCombinedRow = new Row(joinedTable.getNextId());

                    // Copy left row data
                    for (String val : leftRow.getValues()) {
                        newCombinedRow.addValue(val);
                    }
                    // Copy right row data
                    for (String val : rightRow.getValues()) {
                        newCombinedRow.addValue(val);
                    }

                    joinedTable.addRow(newCombinedRow);
                }
            }
        }

        return joinedTable;
    }
}