package org.apache.beam.io.cdc;

import io.debezium.connector.mysql.MySqlConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

@RunWith(JUnit4.class)
public class DebeziumIOMySqlConnectorIT {
  /**
   * Debezium - MySqlContainer
   *
   * <p>
   *   Creates a docker container using the image used by the debezium tutorial.
   * </p>
   */
  @ClassRule
  public MySQLContainer<?> mySQLContainer = new MySQLContainer<>(
      DockerImageName.parse("debezium/example-mysql:1.4")
          .asCompatibleSubstituteFor("mysql")
  )
      .withUsername("mysqluser")
      .withDatabaseName("inventory")
      .withExposedPorts(3306)
      .withPassword("debezium")
      .waitingFor(
          new HttpWaitStrategy()
              .forPort(3306)
              .forStatusCodeMatching(response -> response == 200)
              .withStartupTimeout(Duration.ofMinutes(2))
      );

  /**
   * Debezium - MySQL connector Test
   *
   * <p>
   *     Tests that connector can actually connect to the database
   * </p>
   */
  @Test
  public void testDebeziumIOMySql() {
    mySQLContainer.start();

    String host = mySQLContainer.getContainerIpAddress();
    String port = mySQLContainer.getMappedPort(3306).toString();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    p.apply(DebeziumIO.<String>read().
        withConnectorConfiguration(
            DebeziumIO.ConnectorConfiguration.create()
                .withUsername("debezium")
                .withPassword("dbz")
                .withConnectorClass(MySqlConnector.class)
                .withHostName(host)
                .withPort(port)
                .withConnectionProperty("database.server.id", "184054")
                .withConnectionProperty("database.server.name", "dbserver1")
                .withConnectionProperty("database.include.list", "inventory")
                .withConnectionProperty("include.schema.changes", "false"))
        .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
        .withCoder(StringUtf8Coder.of())
    );

    p.run().waitUntilFinish();
    mySQLContainer.stop();
  }
}
