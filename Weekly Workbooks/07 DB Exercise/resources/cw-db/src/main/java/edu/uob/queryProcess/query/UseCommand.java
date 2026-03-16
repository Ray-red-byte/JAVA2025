package edu.uob.queryProcess.query;
import edu.uob.DatabaseManager;
import edu.uob.entityManager.Table;
import edu.uob.queryProcess.DBCommand;

import java.io.File;

public class UseCommand extends DBCommand {
    public UseCommand(String fullCommand, String[] tokens) { super(fullCommand, tokens); }

    @Override
    public String execute(DatabaseManager dbManager) {
        if (tokens.length < 2) return "[ERROR] Require db name";
        String dbName = tokens[1].toLowerCase();
        File dbDir = new File(dbManager.getStorageFolderPath() + File.separator + dbName);

        if (!dbDir.exists() || !dbDir.isDirectory()) return "[ERROR] Invalid db name";

        dbManager.setCurrentDatabase(dbName);
        dbManager.clearActiveTables();

        File[] tableFiles = dbDir.listFiles();
        if (tableFiles != null) {
            for (File file : tableFiles) {
                if (file.isFile() && file.getName().endsWith(".tab")) {
                    String tableName = file.getName().replace(".tab", "");
                    try {
                        Table table = dbManager.loadDataIntoObject(dbName, tableName);
                        dbManager.addTable(tableName, table);
                    } catch (Exception e) {
                        return "[ERROR] " + e.getMessage();
                    }
                }
            }
        }
        return "[OK] Database changed to " + dbName;
    }
}