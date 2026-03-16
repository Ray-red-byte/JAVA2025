package edu.uob;

import edu.uob.queryProcess.DBCommand;
import edu.uob.queryProcess.QueryParser;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class DBServer {
    private static final char END_OF_TRANSMISSION = 4;
    private DatabaseManager dbManager;

    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        System.out.println("✅ DBServer is successfully running and listening on port 8888...");
        server.blockingListenOn(8888);
    }

    public DBServer() {
        this.dbManager = new DatabaseManager();
    }

    public String handleCommand(String command) {
        try {
            DBCommand parsedCommand = QueryParser.parse(command);
            return parsedCommand.execute(dbManager);
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        } catch (Exception e) {
            return "[ERROR] " + e.getMessage();
        }
    }

    // --- REPAIRED NETWORK CODE BELOW ---

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            // Infinite loop to keep listening for new client connections
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.out.println("Connection handling exception: " + e.getMessage());
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
             BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            // Read the command from the client
            String command = reader.readLine();

            // Process the command and get the response
            String response = handleCommand(command);

            // Send the response back to the client
            writer.write(response);
            writer.write("\n" + END_OF_TRANSMISSION + "\n"); // Tell the client we are done
            writer.flush();
        }
    }
}