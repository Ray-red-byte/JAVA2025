package edu.uob;
import edu.uob.entityManager.Row;
import edu.uob.entityManager.Table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class DatabaseManager {
    private String storageFolderPath;
    private String currentDatabase;
    private HashMap<String, Table> activeTables;

    public DatabaseManager() {
        this.storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        this.activeTables = new HashMap<>();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }

    }

    public String getCurrentDatabase() { return currentDatabase; }
    public void setCurrentDatabase(String dbName) { this.currentDatabase = dbName; }
    public String getStorageFolderPath() { return storageFolderPath; }

    public Table getTable(String tableName) {
        return activeTables.get(tableName.toLowerCase());
    }

    public void addTable(String tableName, Table table) {
        activeTables.put(tableName.toLowerCase(), table);
    }

    public Table loadDataIntoObject(String dbName, String tableName) throws IOException {
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

    public void clearActiveTables() { activeTables.clear(); }
}