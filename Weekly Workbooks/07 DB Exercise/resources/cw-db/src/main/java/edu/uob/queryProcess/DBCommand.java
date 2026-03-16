package edu.uob.queryProcess;

import edu.uob.DatabaseManager;
import java.util.Arrays;
import java.util.List;

public abstract class DBCommand {
    protected String fullCommand;
    protected String[] tokens;

    // List of all reserved SQL keywords from the BNF
    protected static final List<String> RESERVED_KEYWORDS = Arrays.asList(
            "USE", "CREATE", "DROP", "ALTER", "INSERT", "SELECT", "UPDATE", "DELETE", "JOIN",
            "INTO", "VALUES", "FROM", "SET", "WHERE", "ON", "AND", "OR", "TRUE", "FALSE", "LIKE",
            "TABLE", "DATABASE", "ADD"
    );

    public DBCommand(String fullCommand, String[] tokens) {
        this.fullCommand = fullCommand;
        this.tokens = tokens;
    }

    // Helper method to block reserved words
    protected boolean isReservedKeyword(String name) {
        return RESERVED_KEYWORDS.contains(name.toUpperCase());
    }

    public abstract String execute(DatabaseManager dbManager);
}