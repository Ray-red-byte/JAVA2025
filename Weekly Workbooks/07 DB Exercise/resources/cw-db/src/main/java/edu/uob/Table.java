package edu.uob;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

/*
Example

columnNames : id , name, ...
row_1:        1  , Ray, ...
row_2:        2  , Nina, ...
* */

public class Table {
    private String tableName;
    private List<String> columnNames;
    private ArrayList<Row> rows;
    private int nextId;

    public Table(String name) {
        this.tableName = name;
        this.columnNames = new ArrayList<>();
        this.rows = new ArrayList<>();
        this.nextId = 1;
    }

    public void addColumn(String columnName) {
        this.columnNames.add(columnName);
    }

    public void addRow(Row row) {
        this.rows.add(row);
        // Requirement: ensure nextId is always higher than any existing ID
        if (row.getId() >= nextId) {
            nextId = row.getId() + 1;
        }
    }

    // Method to save the current in-memory state back to a .tab file
    public void saveToFile(String folderPath) throws IOException {
        String filePath = folderPath + File.separator + tableName.toLowerCase() + ".tab";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            // Write column headers
            writer.write(String.join("\t", columnNames) + "\n");
            // Write each row
            for (Row row : rows) {
                writer.write(row.toString() + "\n");
            }
        }
    }

    public int getNextId() {
        return this.nextId;
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }

    public ArrayList<Row> getRows() {
        return this.rows;
    }
}