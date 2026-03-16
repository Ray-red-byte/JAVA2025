package edu.uob.queryProcess.query;

import edu.uob.DatabaseManager;
import edu.uob.entityManager.Table;
import edu.uob.queryProcess.DBCommand;

import java.io.File;

public class DropCommand extends DBCommand {
    public DropCommand(String fullCommand, String[] tokens) { super(fullCommand, tokens); }

    @Override
    public String execute(DatabaseManager dbManager) {
        if (tokens.length < 3) return "[ERROR] Invalid DROP format";

        String entityType = tokens[1].toUpperCase();
        String entityName = tokens[2].toLowerCase();

        if (entityType.equals("DATABASE")) {
            File dbDir = new File(dbManager.getStorageFolderPath() + File.separator + entityName);
            if (!dbDir.exists()) return "[ERROR] Database does not exist";

            // Delete all files inside first
            File[] files = dbDir.listFiles();
            if (files != null) {
                for (File f : files) f.delete();
            }
            if (dbDir.delete()) {
                if (entityName.equals(dbManager.getCurrentDatabase())) {
                    dbManager.setCurrentDatabase("");
                    dbManager.clearActiveTables();
                }
                return "[OK] Database dropped";
            }
            return "[ERROR] Failed to drop database";

        } else if (entityType.equals("TABLE")) {
            if (dbManager.getCurrentDatabase() == null) return "[ERROR] No database selected";
            Table table = dbManager.getTable(entityName);
            if (table == null) return "[ERROR] Table does not exist";

            try {
                table.removeTable(dbManager.getStorageFolderPath() + File.separator + dbManager.getCurrentDatabase());
                dbManager.clearActiveTables(); // Simplest way to reflect drop is to force reload next time, or specifically remove from map
                return "[OK] Table dropped";
            } catch (Exception e) {
                return "[ERROR] " + e.getMessage();
            }
        }
        return "[ERROR] Unknown entity type: " + entityType;
    }
}