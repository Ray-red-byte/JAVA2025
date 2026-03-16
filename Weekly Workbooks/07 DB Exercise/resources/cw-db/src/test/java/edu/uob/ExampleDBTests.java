package edu.uob;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Duration;

public class ExampleDBTests {

    private DBServer server;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    // Random name generator - useful for testing "bare earth" queries (i.e. where tables don't previously exist)
    private String generateRandomName() {
        String randomName = "";
        for(int i=0; i<10 ;i++) randomName += (char)( 97 + (Math.random() * 25.0));
        return randomName;
    }

    private String sendCommandToServer(String command) {
        // Try to send a command to the server - this call will timeout if it takes too long (in case the server enters an infinite loop)
        return assertTimeoutPreemptively(Duration.ofMillis(1000), () -> { return server.handleCommand(command);},
        "Server took too long to respond (probably stuck in an infinite loop)");
    }

    // A basic test that creates a database, creates a table, inserts some test data, then queries it.
    // It then checks the response to see that a couple of the entries in the table are returned as expected
    @Test
    public void testBasicCreateAndQuery() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");
        String response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("Simon"), "An attempt was made to add Simon to the table, but they were not returned by SELECT *");
        assertTrue(response.contains("Chris"), "An attempt was made to add Chris to the table, but they were not returned by SELECT *");
    }

    // A test to make sure that querying returns a valid ID (this test also implicitly checks the "==" condition)
    // (these IDs are used to create relations between tables, so it is essential that suitable IDs are being generated and returned !)
    @Test
    public void testQueryID() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("SELECT id FROM marks WHERE name == 'Simon';");
        // Convert multi-lined responses into just a single line
        String singleLine = response.replace("\n"," ").trim();
        // Split the line on the space character
        String[] tokens = singleLine.split(" ");
        // Check that the very last token is a number (which should be the ID of the entry)
        String lastToken = tokens[tokens.length-1];
        try {
            Integer.parseInt(lastToken);
        } catch (NumberFormatException nfe) {
            fail("The last token returned by `SELECT id FROM marks WHERE name == 'Simon';` should have been an integer ID, but was " + lastToken);
        }
    }

    // A test to make sure that databases can be reopened after server restart
    @Test
    public void testTablePersistsAfterRestart() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        // Create a new server object
        server = new DBServer();
        sendCommandToServer("USE " + randomName + ";");
        String response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("Simon"), "Simon was added to a table and the server restarted - but Simon was not returned by SELECT *");
    }

    // Test to make sure that the [ERROR] tag is returned in the case of an error (and NOT the [OK] tag)
    @Test
    public void testForErrorTag() {
        String randomName = generateRandomName();
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        String response = sendCommandToServer("SELECT * FROM libraryfines;");
        assertTrue(response.contains("[ERROR]"), "An attempt was made to access a non-existent table, however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An attempt was made to access a non-existent table, however an [OK] tag was returned");
    }

    @Test
    public void testUseDatabase() {
        sendCommandToServer("CREATE DATABASE my_data;");
        String response = sendCommandToServer("USE my_data;");
        assertTrue(response.contains("[OK]"), "USE command failed for an existing database");

        String errorResponse = sendCommandToServer("USE non_existent_db;");
        assertTrue(errorResponse.contains("[ERROR]"), "Server should error when USE is called on missing database");
    }

    @Test
    public void testUpdateCommand() {
        // Setup the database first!
        server.handleCommand("CREATE DATABASE testdb;");
        server.handleCommand("USE testdb;");

        server.handleCommand("CREATE TABLE marks (name, mark, pass);");
        server.handleCommand("INSERT INTO marks VALUES ('Simon', 65, 'True');");
        server.handleCommand("INSERT INTO marks VALUES ('Chris', 40, 'False');");

        // Update Simon's mark to 100
        String updateResponse = server.handleCommand("UPDATE marks SET mark = '100' WHERE name == 'Simon';");
        assertTrue(updateResponse.startsWith("[OK]"), "Update command should succeed. Server returned: " + updateResponse);

        // Verify the update worked
        String selectResponse = server.handleCommand("SELECT mark FROM marks WHERE name == 'Simon';");
        assertTrue(selectResponse.contains("100"), "Simon's mark should now be 100");
        assertFalse(selectResponse.contains("65"), "Simon's old mark should be gone");
    }

    @Test
    public void testAlterTableCommand() {
        // Setup the database first!
        server.handleCommand("CREATE DATABASE testdb;");
        server.handleCommand("USE testdb;");

        server.handleCommand("CREATE TABLE marks (name, mark);");
        server.handleCommand("INSERT INTO marks VALUES ('Simon', 65);");

        // Add a 'grade' column
        String addColResponse = server.handleCommand("ALTER TABLE marks ADD grade;");
        assertTrue(addColResponse.startsWith("[OK]"), "Adding a column should return [OK]. Server returned: " + addColResponse);

        // Drop the 'mark' column
        String dropColResponse = server.handleCommand("ALTER TABLE marks DROP mark;");
        assertTrue(dropColResponse.startsWith("[OK]"), "Dropping a column should return [OK]. Server returned: " + dropColResponse);

        // Verify the structure changed
        String selectResponse = server.handleCommand("SELECT * FROM marks;");
        assertTrue(selectResponse.contains("grade"), "Header should contain new 'grade' column");
        assertFalse(selectResponse.contains("mark"), "Header should NOT contain dropped 'mark' column");
    }

    @Test
    public void testDropTable() {
        // Setup the database first!
        server.handleCommand("CREATE DATABASE testdb;");
        server.handleCommand("USE testdb;");

        server.handleCommand("CREATE TABLE marks (name, mark);");
        server.handleCommand("INSERT INTO marks VALUES ('Simon', 65);");

        // Drop the table
        String dropResponse = server.handleCommand("DROP TABLE marks;");
        assertTrue(dropResponse.startsWith("[OK]"), "Dropping a table should return [OK]. Server returned: " + dropResponse);

        // Attempting to select from the dropped table should throw an error
        String selectResponse = server.handleCommand("SELECT * FROM marks;");
        assertTrue(selectResponse.startsWith("[ERROR]"), "Selecting from a dropped table should fail");
    }

    @Test
    public void testRobustErrorHandling() {
        server.handleCommand("CREATE TABLE marks (name, mark);");

        // 1. Missing WHERE clause on UPDATE
        String badUpdate = server.handleCommand("UPDATE marks SET mark = '100';");
        assertTrue(badUpdate.startsWith("[ERROR]"), "UPDATE without WHERE should fail");

        // 2. Inserting into a table that doesn't exist
        String badInsert = server.handleCommand("INSERT INTO ghost_table VALUES (1, 2);");
        assertTrue(badInsert.startsWith("[ERROR]"), "Inserting into missing table should fail");

        // 3. Gibberish command
        String gibberish = server.handleCommand("MAKE ME A SANDWICH;");
        assertTrue(gibberish.startsWith("[ERROR]"), "Unknown commands should be rejected");

        // 4. Missing parentheses on INSERT
        String badFormatInsert = server.handleCommand("INSERT INTO marks VALUES 'Simon', 65;");
        assertTrue(badFormatInsert.startsWith("[ERROR]"), "INSERT with missing parentheses should fail");
    }

    @Test
    public void testReservedKeywords() {
        server.handleCommand("CREATE DATABASE testdb;");
        server.handleCommand("USE testdb;");

        // 1. Try to create a table named after a keyword
        String badTable = server.handleCommand("CREATE TABLE select (id, name);");
        assertTrue(badTable.startsWith("[ERROR]"), "Should not allow 'select' as a table name. Response: " + badTable);

        // 2. Try to create a database named after a keyword
        String badDatabase = server.handleCommand("CREATE DATABASE where;");
        assertTrue(badDatabase.startsWith("[ERROR]"), "Should not allow 'where' as a database name. Response: " + badDatabase);

        // 3. Try to add a column named after a keyword
        server.handleCommand("CREATE TABLE marks (name, mark);");
        String badColumn = server.handleCommand("ALTER TABLE marks ADD insert;");
        assertTrue(badColumn.startsWith("[ERROR]"), "Should not allow 'insert' as a column name. Response: " + badColumn);
    }

    @Test
    public void testRelationalOperators() {
        // Use a UNIQUE database name for this test so it doesn't read old data!
        server.handleCommand("DROP DATABASE testdb_relational;"); // Clean up just in case
        server.handleCommand("CREATE DATABASE testdb_relational;");
        server.handleCommand("USE testdb_relational;");

        server.handleCommand("CREATE TABLE marks (name, mark);");
        server.handleCommand("INSERT INTO marks VALUES ('Simon', 65);");
        server.handleCommand("INSERT INTO marks VALUES ('Chris', 40);");
        server.handleCommand("INSERT INTO marks VALUES ('Zack', 80);");

        // 1. Test greater than (Numerical)
        String greaterThan = server.handleCommand("SELECT * FROM marks WHERE mark > 50;");
        assertTrue(greaterThan.contains("Simon"), "Simon (65) should be > 50");
        assertTrue(greaterThan.contains("Zack"), "Zack (80) should be > 50");
        assertFalse(greaterThan.contains("Chris"), "Chris (40) should NOT be > 50");

        // 2. Test less than or equal to (Numerical)
        String lessThanEq = server.handleCommand("SELECT * FROM marks WHERE mark <= 65;");
        assertTrue(lessThanEq.contains("Simon"), "Simon (65) should be <= 65");
        assertTrue(lessThanEq.contains("Chris"), "Chris (40) should be <= 65");
        assertFalse(lessThanEq.contains("Zack"), "Zack (80) should NOT be <= 65");

        // 3. Test not equal (String)
        String notEqual = server.handleCommand("SELECT * FROM marks WHERE name != 'Simon';");
        assertFalse(notEqual.contains("Simon"), "Simon should be excluded");
        assertTrue(notEqual.contains("Chris"), "Chris should be included");

        // 4. Test greater than (String comparison - alphabetical)
        String stringGreater = server.handleCommand("SELECT * FROM marks WHERE name > 'D';");
        assertTrue(stringGreater.contains("Simon"), "Simon > D");
        assertTrue(stringGreater.contains("Zack"), "Zack > D");
        assertFalse(stringGreater.contains("Chris"), "Chris is NOT > D (C comes before D)");
    }

    @Test
    public void testLikeOperator() {
        // Use a UNIQUE database name
        server.handleCommand("DROP DATABASE testdb_like;");
        server.handleCommand("CREATE DATABASE testdb_like;");
        server.handleCommand("USE testdb_like;");

        server.handleCommand("CREATE TABLE marks (name, mark);");
        server.handleCommand("INSERT INTO marks VALUES ('Simon', 65);");
        server.handleCommand("INSERT INTO marks VALUES ('Simone', 75);");
        server.handleCommand("INSERT INTO marks VALUES ('Chris', 40);");

        // 1. Test standard string substring
        String likeSim = server.handleCommand("SELECT * FROM marks WHERE name LIKE 'Sim';");
        assertTrue(likeSim.contains("Simon"), "Should match Simon");
        assertTrue(likeSim.contains("Simone"), "Should match Simone");
        assertFalse(likeSim.contains("Chris"), "Should NOT match Chris");

        // 2. Test case sensitivity (capital 'O' vs lowercase 'o')
        String likeCase = server.handleCommand("SELECT * FROM marks WHERE name LIKE 'mOn';");
        assertFalse(likeCase.contains("Simon"), "LIKE should be case sensitive");

        // 3. Test LIKE on numerical data (should treat it like a string)
        String likeNumber = server.handleCommand("SELECT * FROM marks WHERE mark LIKE '5';");
        assertTrue(likeNumber.contains("65"), "65 contains 5");
        assertTrue(likeNumber.contains("75"), "75 contains 5");
        assertFalse(likeNumber.contains("40"), "40 does not contain 5");
    }
}
