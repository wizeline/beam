package org.apache.beam.io.cdc;

import io.debezium.connector.mysql.MySqlConnector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.apache.beam.sdk.testing.SerializableMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;


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
  public static final MySQLContainer<?> mySQLContainer = new MySQLContainer<>(
      DockerImageName.parse("debezium/example-mysql:1.4")
          .asCompatibleSubstituteFor("mysql")
  )
      .withPassword("debezium")
      .withUsername("mysqluser")
      .withExposedPorts(3306)
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
    PCollection<String> results = p.apply(DebeziumIO.<String>read().
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
    String expected = "{\"metadata\":{\"connector\":\"mysql\",\"version\":\"1.3.1.Final\",\"name\":\"dbserver1\",\"database\":\"inventory\",\"schema\":\"mysql-bin.000003\",\"table\":\"addresses\"},\"before\":null,\"after\":{\"fields\":{\"zip\":\"76036\",\"city\":\"Euless\",\"street\":\"3183 Moore Avenue\",\"id\":10,\"state\":\"Texas\",\"customer_id\":1001,\"type\":\"SHIPPING\"}}}";

    PAssert.that(results).satisfies((Iterable<String> res) -> {
      assertThat(res, hasItem(expected));
      return null;
    });

    p.run().waitUntilFinish();
    mySQLContainer.stop();
  }
}
