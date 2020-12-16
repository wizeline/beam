package org.apache.beam.io.cdc;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.sqlserver.SqlServerConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CounterSourceConnectorTest {
  @Test
  public void testDebeziumIO() {
	  PipelineOptions options = PipelineOptionsFactory.create();
	  Pipeline p = Pipeline.create(options);
	  p.apply(
			  DebeziumIO.<String>read().
			  		withConnectorConfiguration(
						DebeziumIO.ConnectorConfiguration.create()
							.withUsername("debezium")
							.withPassword("dbz")
							.withConnectorClass(MySqlConnector.class)
							.withHostName("127.0.0.1")
							.withPort("3306")
							.withConnectionProperty("database.server.id", "184054")
							.withConnectionProperty("database.server.name", "dbserver1")
							.withConnectionProperty("database.include.list", "inventory")
							.withConnectionProperty("database.history", DebeziumHistory.class.getName())
							.withConnectionProperty("include.schema.changes", "false")
              ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
      ).setCoder(StringUtf8Coder.of());
	  //.apply(TextIO.write().to("test"));

	  p.run().waitUntilFinish();
  }
  
    @Test
    public void testDebeziumIOPostgreSql() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.<String>read().
                withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                                .withUsername("postgres")
                                .withPassword("debezium")
                                .withConnectorClass(PostgresConnector.class)
                                .withHostName("127.0.0.1")
                                .withPort("5000")
                                .withConnectionProperty("database.dbname", "postgres")
                                .withConnectionProperty("database.server.name", "dbserver2")
                                .withConnectionProperty("schema.include.list", "inventory")
                                .withConnectionProperty("slot.name", "dbzslot2")
                                .withConnectionProperty("database.history", DebeziumHistory.class.getName())
                                .withConnectionProperty("include.schema.changes", "false")
                ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
        ).setCoder(StringUtf8Coder.of());

        p.run().waitUntilFinish();
    }

    @Test
    public void testDebeziumIOSqlSever() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.<String>read().
                        withConnectorConfiguration(
                                DebeziumIO.ConnectorConfiguration.create()
                                        .withUsername("sa")
                                        .withPassword("Password!")
                                        .withConnectorClass(SqlServerConnector.class)
                                        .withHostName("127.0.0.1")
                                        .withPort("1433")
                                        .withConnectionProperty("database.dbname", "testDB")
                                        .withConnectionProperty("database.server.name", "server1")
                                        .withConnectionProperty("table.include.list", "dbo.customers")
                                        .withConnectionProperty("database.history", DebeziumHistory.class.getName())
                                        .withConnectionProperty("include.schema.changes", "false")
                        ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
        ).setCoder(StringUtf8Coder.of());

        p.run().waitUntilFinish();
    }
  
}
