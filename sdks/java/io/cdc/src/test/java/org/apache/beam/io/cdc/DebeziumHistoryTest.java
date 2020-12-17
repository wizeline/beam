package org.apache.beam.io.cdc;

import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParserSql2003;
import io.debezium.relational.ddl.LegacyDdlParser;
import io.debezium.util.Collect;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class DebeziumHistoryTest {
    private DebeziumHistory history;
    private Map<String, String> source;
    private Map<String, Object> position;

    @Before
    public void beforeEach() throws Exception {
        source = Collect.hashMapOf("server", "my-server");
        setLogPosition(0);
        DebeziumOffsetHolder restriction = new DebeziumOffsetHolder(null, null);
        DebeziumOffsetTracker tracker = new DebeziumOffsetTracker(restriction);
        KafkaSourceConsumerFn.restrictionTrackers.put(Integer.toString(System.identityHashCode(this)), tracker);
        history = new DebeziumHistory(KafkaSourceConsumerFn.restrictionTrackers);
    }

    @After
    public void afterEach() throws Exception {
        try {
            if (history != null) {
                history.stop();
            }
        }
        finally {
            history = null;
        }
    }

    @Test
    public void shouldStartWithEmptyListAndStoreDataAndRecoverAllState() throws Exception {
        testHistoryContent();
    }

    @Test
    public void testExists() {
        assertFalse(history.exists());

        // happy path
        testHistoryContent();

        assertTrue(history.exists());
    }

    private void testHistoryContent() {
        history.start();

        // Should be able to call start more than once ...
        history.start();

        history.initializeStorage();

        // Calling it another time to ensure we can work with the DB history already existing
        history.initializeStorage();

        LegacyDdlParser recoveryParser = new DdlParserSql2003();
        LegacyDdlParser ddlParser = new DdlParserSql2003();
        ddlParser.setCurrentSchema("db1"); // recover does this, so we need to as well
        Tables tables1 = new Tables();
        Tables tables2 = new Tables();
        Tables tables3 = new Tables();

        // Recover from the very beginning ...
        setLogPosition(0);
        history.recover(source, position, tables1, recoveryParser);

        // There should have been nothing to recover ...
        assertEquals(0, tables1.size());

        // Now record schema changes, which writes out to memory but doesn't actually change the Tables ...
        setLogPosition(10);
        String ddl = "CREATE TABLE foo ( name VARCHAR(255) NOT NULL PRIMARY KEY); \n" +
                "CREATE TABLE customers ( id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(100) NOT NULL ); \n" +
                "CREATE TABLE products ( productId INTEGER NOT NULL PRIMARY KEY, desc VARCHAR(255) NOT NULL); \n";
        history.record(source, position, "db1", ddl);

        // Parse the DDL statement 3x and each time update a different Tables object ...
        ddlParser.parse(ddl, tables1);
        assertEquals(3, tables1.size());

        ddlParser.parse(ddl, tables2);
        assertEquals(3, tables2.size());
        ddlParser.parse(ddl, tables3);
        assertEquals(3, tables3.size());

        // Record a drop statement and parse it for 2 of our 3 Tables...
        setLogPosition(39);
        ddl = "DROP TABLE foo;";
        history.record(source, position, "db1", ddl);
        ddlParser.parse(ddl, tables2);
        assertEquals(2, tables2.size());
        ddlParser.parse(ddl, tables3);
        assertEquals(2, tables3.size());

        // Record another DDL statement and parse it for 1 of our 3 Tables...
        setLogPosition(10003);
        ddl = "CREATE TABLE suppliers ( supplierId INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL);";
        history.record(source, position, "db1", ddl);
        ddlParser.parse(ddl, tables3);
        assertEquals(3, tables3.size());

        // Stop the history...
        history.stop();
        history = new DebeziumHistory(KafkaSourceConsumerFn.restrictionTrackers);
        history.start();

        // Recover from the very beginning to just past the first change ...
        Tables recoveredTables = new Tables();
        setLogPosition(15);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertEquals(recoveredTables, tables1);

        // Recover from the very beginning to just past the second change ...
        recoveredTables = new Tables();
        setLogPosition(50);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertEquals(recoveredTables, tables2);

        // Recover from the very beginning to just past the third change ...
        recoveredTables = new Tables();
        setLogPosition(10010);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertEquals(recoveredTables, tables3);

        // Recover from the very beginning to way past the third change ...
        recoveredTables = new Tables();
        setLogPosition(100000010);
        history.recover(source, position, recoveredTables, recoveryParser);
        assertEquals(recoveredTables, tables3);
    }

    protected void setLogPosition(int index) {
        this.position = Collect.hashMapOf("filename", "my-txn-file.log",
                "position", index);
    }

}
