package edu.uob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.ArrayList;

/** This class implements the DB server. */
public class DBServer {

    private static final char END_OF_TRANSMISSION = 4;
    private String storageFolderPath;
    private String currentDatabase;
    private java.util.HashMap<String, Table> activeTables;

    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
    * KEEP this signature otherwise we won't be able to mark your submission correctly.
    */
    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
        currentDatabase = "";
        activeTables = new java.util.HashMap<>();
    }

    private Table loadDataIntoObject(String dbName, String tableName) throws IOException {
        Table table = new Table(tableName);
        String filePath = storageFolderPath + File.separator + dbName.toLowerCase() +
                File.separator + tableName.toLowerCase() + ".tab";
        File file = new File(filePath);

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // 1. Read the first line (Headers)
            String headerLine = reader.readLine();
            if (headerLine != null) {
                String[] headers = headerLine.split("\t");
                for (String header : headers) {
                    table.addColumn(header); // You'll need to add this method to Table.java
                }
            }

            // 2. Read the data rows
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                int id = Integer.parseInt(parts[0]);
                Row row = new Row(id);
                for (int i = 1; i < parts.length; i++) {
                    row.addValue(parts[i]);
                }
                table.addRow(row); // You'll need to add this method to Table.java
            }
        }
        return table;
    }

    /**
    * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise we won't be
    * able to mark your submission correctly.
    *
    * <p>This method handles all incoming DB commands and carries out the required actions.
    */
    public String handleCommand(String command) {
        // TODO implement your server logic here
        if (command == null){
            return "[ERROR] command is empty";
        }
        String cleanedCommand = preprocessCommand(command);
        String[] tokens = cleanedCommand.split("\\s+");
        if (tokens.length == 0 || tokens[0].isEmpty()){
            return "[ERROR] Empty command";
        }
        String trigger = tokens[0];
        try {
            switch (trigger) {
                case "SELECT":
                    return handleSelectCommand(tokens, cleanedCommand);
                case "INSERT":
                    return handleInsertCommand(tokens, cleanedCommand);
                case "USE":
                    return handleUseCommand(tokens);
                case "CREATE":
                    return handleCreateCommand(tokens, cleanedCommand);
                case "UPDATE":
                    return handleUpdateCommand(tokens, cleanedCommand);
                case "ALTER":
                    return handleAlterCommand(tokens, cleanedCommand);
                case "DELETE":
                    return handleDeleteCommand(tokens, cleanedCommand);
                case "DROP":
                    return handleDropCommand(tokens, cleanedCommand);
                default:
                    return "[ERROR] Unknown command: " + trigger;
            }
        } catch (Exception e) {
            return "[ERROR] " + e.getMessage();
        }
    }

    private String preprocessCommand(String command){
        String cleanedCommand = command.trim();
        if (cleanedCommand.endsWith(";")) {
            cleanedCommand = cleanedCommand.substring(0, cleanedCommand.length() - 1);
        }
        return cleanedCommand;
    }

    private String handleUseCommand(String[] tokens){
        if (tokens.length < 2){
            return "[ERROR] Require db name";
        }
        String dbName = tokens[1].toLowerCase();
        File dbDir = new File(storageFolderPath + File.separator + dbName);
        if (!dbDir.exists() || !dbDir.isDirectory()){
            return "[ERROR] Invalid db name";
        }
        this.currentDatabase = dbName;
        this.activeTables.clear();

        // Load Data for current Database
        File[] tableFiles = dbDir.listFiles();
        if (tableFiles != null) {
            for (File file : tableFiles){
                if (file.isFile() && file.getName().endsWith(".tab")) {
                    String tableName = file.getName().replace(".tab", "");
                    try {
                        Table table = loadDataIntoObject(dbName, tableName);
                        this.activeTables.put(tableName, table);
                    } catch (IOException e) {
                        return "[ERROR] " + e.getMessage();
                    }
                }
            }
        }
        return "[OK] Database changed to " + dbName;
    }

    private String handleCreateCommand(String[] tokens, String fullCommand){
        if (tokens.length < 3){return "[ERROR] Require db / table name";}
        String targetType = tokens[1].toLowerCase();
        String targetName = tokens[2].toLowerCase();
        if (targetType.equalsIgnoreCase("DATABASE")) {
            return createDatabase(targetName);
        } else if (targetType.equalsIgnoreCase("TABLE")) {
            return createTable(targetName, fullCommand);
        } else {
            return "[ERROR] Can only CREATE DATABASE or TABLE";
        }
    }

    private String createDatabase(String dbName) {
        File dbDir = new File(storageFolderPath + File.separator + dbName);
        if (dbDir.exists() || dbDir.isDirectory()){
            return "[WARNING] IDatabase already exists";
        }
        if (dbDir.mkdir()){
            return "[OK] Database created";
        } else {
            return "[ERROR] Unable to create database";
        }
    }

    private String createTable(String tableNameToken, String fullCommand) {
        // 1. Check if we are currently inside a database
        if (this.currentDatabase == null || this.currentDatabase.isEmpty()) {
            return "[ERROR] No database selected. Use USE <dbname> first.";
        }

        // Sometimes the table name token might be attached to the parenthesis e.g., "marks(name"
        String tableName = tableNameToken;
        if (tableName.contains("(")) {
            tableName = tableName.substring(0, tableName.indexOf("("));
        }

        // 2. Check if the table already exists in our active map
        if (this.activeTables.containsKey(tableName)) {
            return "[ERROR] Table " + tableName + " already exists";
        }

        // 3. Create the new Table object
        Table newTable = new Table(tableName);

        // In these DB projects, the first column is almost always an auto-incrementing "id"
        newTable.addColumn("id");

        // 4. Parse columns if they exist inside ( )
        if (fullCommand.contains("(") && fullCommand.contains(")")) {
            int startIndex = fullCommand.indexOf("(") + 1;
            int endIndex = fullCommand.indexOf(")");
            String columnsString = fullCommand.substring(startIndex, endIndex);

            // Split by comma to get individual column names
            String[] columns = columnsString.split(",");
            for (String col : columns) {
                newTable.addColumn(col.trim());
            }
        }

        // 5. Save to our in-memory data structure
        this.activeTables.put(tableName, newTable);

        // 6. Save the empty table to a .tab file immediately
        try {
            String dbPath = storageFolderPath + File.separator + this.currentDatabase;
            newTable.saveToFile(dbPath);
            return "[OK] Table " + tableName + " created successfully";
        } catch (IOException e) {
            return "[ERROR] Failed to save table to disk: " + e.getMessage();
        }
    }

    private String handleInsertCommand(String[] tokens, String fullCommand) {
        if (this.currentDatabase == null || this.currentDatabase.isEmpty()) {
            return "[ERROR] No database selected";
        }
        if (tokens.length < 5 || !tokens[1].equalsIgnoreCase("INTO")) {
            return "[ERROR] Invalid INSERT format";
        }

        String tableName = tokens[2].toLowerCase();
        if (!this.activeTables.containsKey(tableName)) {
            return "[ERROR] Table " + tableName + " does not exist";
        }

        Table table = this.activeTables.get(tableName);

        // Extract everything between ( and )
        int start = fullCommand.indexOf("(");
        int end = fullCommand.lastIndexOf(")");
        if (start == -1 || end == -1) {
            return "[ERROR] Missing values in parentheses";
        }

        String valuesString = fullCommand.substring(start + 1, end);
        String[] values = valuesString.split(",");
        int expectedValueCount = table.getColumnNames().size() - 1;
        if (values.length != expectedValueCount) {
            return "[ERROR] Column count mismatch. Expected " + expectedValueCount +
                    " values, but got " + values.length;
        }

        // Create a new Row using the table's auto-incrementing ID
        Row newRow = new Row(table.getNextId());

        // Add each value to the row, removing leading/trailing spaces and single quotes
        for (String val : values) {
            String cleanVal = val.trim();
            if (cleanVal.startsWith("'") && cleanVal.endsWith("'")) {
                cleanVal = cleanVal.substring(1, cleanVal.length() - 1);
            }
            newRow.addValue(cleanVal);
        }

        // Add the row to the table and save to disk
        table.addRow(newRow);
        try {
            String dbPath = storageFolderPath + File.separator + this.currentDatabase;
            table.saveToFile(dbPath);
            return "[OK] Record inserted successfully";
        } catch (IOException e) {
            return "[ERROR] Failed to save table: " + e.getMessage();
        }
    }

    private String handleSelectCommand(String[] tokens, String fullCommand) {
        if (this.currentDatabase == null || this.currentDatabase.isEmpty()) {return  "[ERROR] No database selected";}
        if (tokens.length < 4 || !tokens[0].equalsIgnoreCase("SELECT")) {return  "[ERROR] Invalid SELECT format";}
        int fromIndex = -1;
        for (int i = 0; i < tokens.length; i++){
            if (tokens[i].equalsIgnoreCase("FROM")) {
               fromIndex = i;
               break;
            }
        }
        if (fromIndex == -1) {return  "[ERROR] Invalid FROM";}
        String tableName = tokens[fromIndex + 1].toLowerCase();
        if (!this.activeTables.containsKey(tableName)) {return  "[ERROR] Table " + tableName + " does not exist";}
        Table table = this.activeTables.get(tableName);

        // Find the exact character position of " FROM " in the string
        int stringFromIndex = fullCommand.toUpperCase().indexOf(" FROM ");
        // Extract everything between "SELECT" (which is 6 characters long) and " FROM "
        String selectTarget = fullCommand.substring(6, stringFromIndex).trim();

        boolean selectAll = selectTarget.equals("*");

        String[] targetColumns = selectAll ? new String[0] : selectTarget.split(",");
        for (int i = 0; i < targetColumns.length; i++) {
            targetColumns[i] = targetColumns[i].trim();
            if (!selectAll && !table.getColumnNames().contains(targetColumns[i])) {
                return "[ERROR] Column " + targetColumns[i] + " does not exist in table " + tableName;
            }
        }

        // 3. Parse the WHERE clause (if it exists)
        boolean hasWhere = fullCommand.toUpperCase().contains(" WHERE ");
        String whereColumn = "";
        String whereValue = "";

        if (hasWhere) {
            String whereClause = fullCommand.substring(fullCommand.toUpperCase().indexOf(" WHERE ") + 7).trim();
            // Assuming basic format: column = 'value'
            String[] whereParts = whereClause.split("==");
            if (whereParts.length != 2) {
                return "[ERROR] Invalid WHERE clause format. Use 'column == value'";
            }
            whereColumn = whereParts[0].trim();
            whereValue = whereParts[1].trim();

            // Remove quotes if present
            if (whereValue.startsWith("'") && whereValue.endsWith("'")) {
                whereValue = whereValue.substring(1, whereValue.length() - 1);
            }

            if (!table.getColumnNames().contains(whereColumn)) {
                return "[ERROR] WHERE condition column " + whereColumn + " does not exist.";
            }
        }

        // Parse JOIN clause
        int joinIndex = -1;
        int onIndex = -1;

        for (int i = 0; i < tokens.length; i++) {
            if (tokens[i].equalsIgnoreCase("JOIN")) joinIndex = i;
            if (tokens[i].equalsIgnoreCase("ON")) onIndex = i;
        }

        if (joinIndex != -1 && onIndex != -1) {
            String rightTableName = tokens[joinIndex + 1].toLowerCase();
            if (!this.activeTables.containsKey(rightTableName)) {
                return "[ERROR] Joined table " + rightTableName + " does not exist";
            }
            Table rightTable = this.activeTables.get(rightTableName);

            // Extract the columns to join on (e.g., ON col1 == col2)
            String leftCol = tokens[onIndex + 1];
            String rightCol = tokens[onIndex + 3];

            try {
                // Magic happens here: Overwrite 'table' with the newly joined table!
                table = TableJoiner.performJoin(table, rightTable, leftCol, rightCol);
            } catch (Exception e) {
                return "[ERROR] " + e.getMessage();
            }
        }

        // 4. Build the Result
        StringBuilder result = new StringBuilder();
        result.append("[OK]\n");

        // Print Headers
        if (selectAll) {
            result.append(String.join("\t", table.getColumnNames())).append("\n");
        } else {
            result.append(String.join("\t", targetColumns)).append("\n");
        }

        // Print Rows
        int whereColIndex = table.getColumnNames().indexOf(whereColumn);

        for (Row row : table.getRows()) {
            // Check WHERE condition
            if (hasWhere) {
                String rowValue = "";
                if (whereColIndex == 0) { // Column 0 is always ID
                    rowValue = String.valueOf(row.getId());
                } else {
                    rowValue = row.getValues().get(whereColIndex - 1);
                }

                if (!rowValue.equals(whereValue)) {
                    continue;
                }
            }

            // Append row data based on selected columns
            if (selectAll) {
                result.append(row.getId()).append("\t");
                result.append(String.join("\t", row.getValues())).append("\n");
            } else {
                // Only print the requested columns
                for (int i = 0; i < targetColumns.length; i++) {
                    int colIndex = table.getColumnNames().indexOf(targetColumns[i]);
                    if (colIndex == 0) {
                        result.append(row.getId());
                    } else {
                        result.append(row.getValues().get(colIndex - 1));
                    }
                    if (i < targetColumns.length - 1) result.append("\t");
                }
                result.append("\n");
            }
        }

        return result.toString().trim(); // Trim trailing newline
    }

    private String handleUpdateCommand(String[] tokens, String fullCommand) {
        // 1. Validate Database and basic format
        if (this.currentDatabase == null || this.currentDatabase.isEmpty()) {
            return "[ERROR] No database selected";
        }
        if (tokens.length < 4) {
            return "[ERROR] Invalid UPDATE format";
        }

        String tableName = tokens[1].toLowerCase();
        if (!this.activeTables.containsKey(tableName)) {
            return "[ERROR] Table " + tableName + " does not exist";
        }

        Table table = this.activeTables.get(tableName);
        String upperCommand = fullCommand.toUpperCase();

        int setIndex = upperCommand.indexOf(" SET ");
        int whereIndex = upperCommand.indexOf(" WHERE ");

        if (setIndex == -1 || whereIndex == -1 || setIndex > whereIndex) {
            return "[ERROR] Invalid UPDATE format. Ensure SET and WHERE clauses are present.";
        }

        // 2. Parse the SET clause
        String setClause = fullCommand.substring(setIndex + 5, whereIndex).trim();
        String[] setParts = setClause.split(",");

        java.util.ArrayList<Integer> updateIndices = new java.util.ArrayList<>();
        java.util.ArrayList<String> updateValues = new java.util.ArrayList<>();

        for (String part : setParts) {
            // Support 'column=value'
            String[] colVal = part.split("=");
            if (colVal.length != 2) {
                return "[ERROR] Invalid SET format: " + part;
            }

            String colName = colVal[0].trim();
            String val = colVal[1].trim();

            // Strip quotes if they exist
            if (val.startsWith("'") && val.endsWith("'")) {
                val = val.substring(1, val.length() - 1);
            }

            int colIndex = table.getColumnNames().indexOf(colName);
            if (colIndex == -1) {
                return "[ERROR] Column " + colName + " does not exist.";
            }
            if (colIndex == 0) {
                return "[ERROR] Cannot update the ID column.";
            }

            // Store the target index (-1 because Row.values excludes the ID) and new value
            updateIndices.add(colIndex - 1);
            updateValues.add(val);
        }

        // 3. Parse the WHERE clause
        String whereClause = fullCommand.substring(whereIndex + 7).trim();

        // Support 'column == value' or 'column = value'
        String[] whereParts = whereClause.split("==");
        if (whereParts.length != 2) {
            whereParts = whereClause.split("=");
            if (whereParts.length != 2) {
                return "[ERROR] Invalid WHERE clause format. Use 'column == value'";
            }
        }

        String whereColumn = whereParts[0].trim();
        String whereValue = whereParts[1].trim();

        // Strip quotes if they exist
        if (whereValue.startsWith("'") && whereValue.endsWith("'")) {
            whereValue = whereValue.substring(1, whereValue.length() - 1);
        }

        int whereColIndex = table.getColumnNames().indexOf(whereColumn);
        if (whereColIndex == -1) {
            return "[ERROR] WHERE condition column " + whereColumn + " does not exist.";
        }

        // 4. Find matching rows and apply the updates
        int updatedCount = 0;
        for (Row row : table.getRows()) {
            String rowValue = "";
            if (whereColIndex == 0) {
                rowValue = String.valueOf(row.getId());
            } else {
                rowValue = row.getValues().get(whereColIndex - 1);
            }

            // Check if the condition is met
            if (rowValue.equals(whereValue)) {
                // Update all requested columns for this specific row
                for (int i = 0; i < updateIndices.size(); i++) {
                    int valIndexToUpdate = updateIndices.get(i);
                    String newValue = updateValues.get(i);
                    row.getValues().set(valIndexToUpdate, newValue);
                }
                updatedCount++;
            }
        }

        // 5. Save the modified table back to the filesystem
        try {
            String dbPath = storageFolderPath + File.separator + this.currentDatabase;
            table.saveToFile(dbPath);
            return "[OK] Updated " + updatedCount + " rows successfully.";
        } catch (IOException e) {
            return "[ERROR] Failed to save table to disk: " + e.getMessage();
        }
    }

    private String handleAlterCommand(String[] tokens, String fullCommand) {
        if (this.currentDatabase == null || this.currentDatabase.isEmpty()) {
            return "[ERROR] No database selected";
        }
        // Example syntax: ALTER TABLE <TableName> DROP <ColumnName>
        if (tokens.length < 5) {
            return "[ERROR] Invalid ALTER format";
        }
        if (!tokens[1].equalsIgnoreCase("TABLE")) {
            return "[ERROR] Invalid ALTER format, expected TABLE";
        }

        String tableName = tokens[2].toLowerCase();
        if (!this.activeTables.containsKey(tableName)) {
            return "[ERROR] Table " + tableName + " does not exist";
        }
        Table table = this.activeTables.get(tableName);

        String action = tokens[3].toUpperCase();
        if (!action.equals("ADD") && !action.equals("DROP")) {
            return "[ERROR] Invalid ALTER action: " + action + ". Use ADD or DROP.";
        }

        String columnName = tokens[4].toLowerCase();

        if (action.equals("ADD")) {
            if (table.getColumnNames().contains(columnName)) {
                return "[ERROR] Column " + columnName + " already exists.";
            }
            table.addColumn(columnName);

            // Backfill the existing rows with an empty string for the new column
            for (Row row : table.getRows()) {
                row.addValue("");
            }

        } else if (action.equals("DROP")) {
            int colIndex = table.getColumnNames().indexOf(columnName);
            if (colIndex == -1) {
                return "[ERROR] Column " + columnName + " does not exist.";
            }
            if (colIndex == 0) { // Column 0 is the ID
                return "[ERROR] Cannot drop the ID column.";
            }

            // Remove from headers
            table.getColumnNames().remove(colIndex);

            // Remove from every row (offset by -1 because Row.values excludes the ID)
            for (Row row : table.getRows()) {
                row.getValues().remove(colIndex - 1);
            }
        }

        // Save the structural changes back to the .tab file
        try {
            String dbPath = storageFolderPath + File.separator + this.currentDatabase;
            table.saveToFile(dbPath);
            return "[OK] Table " + tableName + " altered successfully";
        } catch (IOException e) {
            return "[ERROR] Failed to save table to disk: " + e.getMessage();
        }
    }

    private String handleDeleteCommand(String[] tokens, String fullCommand) {
        // 1. Validate Database and basic format
        if (this.currentDatabase == null || this.currentDatabase.isEmpty()) {
            return "[ERROR] No database selected";
        }
        if (tokens.length < 4 || !tokens[1].equalsIgnoreCase("FROM")) {
            return "[ERROR] Invalid DELETE format. Expected: DELETE FROM <table> WHERE <condition>";
        }

        String tableName = tokens[2].toLowerCase();
        if (!this.activeTables.containsKey(tableName)) {
            return "[ERROR] Table " + tableName + " does not exist";
        }
        Table table = this.activeTables.get(tableName);

        // 2. Extract and parse the WHERE clause
        int whereIndex = fullCommand.toUpperCase().indexOf(" WHERE ");
        if (whereIndex == -1) {
            return "[ERROR] DELETE command requires a WHERE clause";
        }

        String whereClause = fullCommand.substring(whereIndex + 7).trim();

        // Support 'column == value' or 'column = value'
        String[] whereParts = whereClause.split("==");
        if (whereParts.length != 2) {
            whereParts = whereClause.split("=");
            if (whereParts.length != 2) {
                return "[ERROR] Invalid WHERE clause format. Use 'column == value'";
            }
        }

        String whereColumn = whereParts[0].trim();
        String whereValue = whereParts[1].trim();

        // Strip quotes if they exist
        if (whereValue.startsWith("'") && whereValue.endsWith("'")) {
            whereValue = whereValue.substring(1, whereValue.length() - 1);
        }

        int whereColIndex = table.getColumnNames().indexOf(whereColumn);
        if (whereColIndex == -1) {
            return "[ERROR] WHERE condition column " + whereColumn + " does not exist.";
        }

        // 3. Find matching rows and collect them in a separate list
        java.util.ArrayList<Row> rowsToDelete = new java.util.ArrayList<>();

        for (Row row : table.getRows()) {
            String rowValue = "";

            if (whereColIndex == 0) {
                rowValue = String.valueOf(row.getId());
            } else {
                rowValue = row.getValues().get(whereColIndex - 1);
            }

            // Check if the condition is met
            if (rowValue.equals(whereValue)) {
                // Add the matching row to our temporary deletion list
                rowsToDelete.add(row);
            }
        }

        // Safely remove all the collected rows at once
        table.getRows().removeAll(rowsToDelete);
        int deletedCount = rowsToDelete.size();

        // 4. Save the modified table back to the filesystem
        try {
            String dbPath = storageFolderPath + File.separator + this.currentDatabase;
            table.saveToFile(dbPath);
            return "[OK] Deleted " + deletedCount + " rows successfully.";
        } catch (IOException e) {
            return "[ERROR] Failed to save table to disk: " + e.getMessage();
        }
    }

    private String handleDropCommand(String[] tokens, String fullCommand) {
        if (this.currentDatabase == null || this.currentDatabase.isEmpty()) {
            return "[ERROR] No database selected";
        }
        // FIX 1: Change to !tokens[1].equalsIgnoreCase to allow "TABLE"
        if (tokens.length < 3 || !tokens[1].equalsIgnoreCase("TABLE")) {
            return "[ERROR] Invalid DROP command. Expected: DROP TABLE <tablename>";
        }

        String tableName = tokens[2].toLowerCase();
        if (!this.activeTables.containsKey(tableName)) {
            return "[ERROR] Table " + tableName + " does not exist";
        }

        Table table = this.activeTables.get(tableName);

        try {
            String dbPath = storageFolderPath + File.separator + this.currentDatabase;
            table.removeTable(dbPath);
            this.activeTables.remove(tableName);
            return "[OK] Dropped " + tableName + " successfully";
        } catch (IOException e) {
            return "[ERROR] Failed to delete table file: " + e.getMessage();
        }
    }

    //  === Methods below handle networking aspects of the project - you will not need to change these ! ===
    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }
}
