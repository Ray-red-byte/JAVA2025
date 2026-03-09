package edu.uob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class DBClient {

    private static final char END_OF_TRANSMISSION = 4;

    public static void main(String[] args) throws IOException {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        Socket socket = new Socket("localhost", 8888);
        BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        BufferedWriter socketWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        // Keep receiving query until get interrupt
        while (!Thread.interrupted()) {
            handleNextCommand(input, socketReader, socketWriter);
        }
    }

    private static void handleNextCommand(BufferedReader commandLine, BufferedReader socketReader, BufferedWriter socketWriter) throws IOException {
        System.out.print("SQL:> ");
        // 1. GET keyboard input
        String command = commandLine.readLine();
        // 2. SEND to server
        socketWriter.write(command + "\n");
        // 3. PUSH it through the pipe
        socketWriter.flush();
        // 4. RECEIVE from server
        String incomingMessage = socketReader.readLine();
        if (incomingMessage == null) {
            throw new IOException("Server disconnected (end-of-stream)");
        }
        // END_OF_TRANSMISSION indicate it is the END
        // KEEP print out response line by line until seeing END_OF_TRANSMISSION
        while (incomingMessage != null && !incomingMessage.contains("" + END_OF_TRANSMISSION + "")) {
            System.out.println(incomingMessage);
            incomingMessage = socketReader.readLine();
        }
    }
}
