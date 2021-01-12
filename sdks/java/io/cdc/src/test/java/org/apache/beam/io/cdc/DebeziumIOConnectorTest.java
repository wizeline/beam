/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.io.cdc;

import io.debezium.connector.db2.Db2Connector;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.sqlserver.SqlServerConnector;

import io.debezium.relational.history.FileDatabaseHistory;
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

/**
 * DebeziumIO Tester
 *
 * <p>
 *     Tests the three different Connectors.
 *     Requires each environment (MySQL, PostgreSQL, SQLServer) already set up.
 * </p>
 */
@RunWith(JUnit4.class)
public class DebeziumIOConnectorTest {
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

    /**
     * Debezium - PostgreSQL connector Test
     *
     * <p>
     *     Tests that connector can actually connect to the database
     * </p>
     */
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
                                .withConnectionProperty("include.schema.changes", "false"))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withCoder(StringUtf8Coder.of())
        );

        p.run().waitUntilFinish();
    }

    /**
     * Debezium - SQLServer connector Test
     *
     * <p>
     *     Tests that connector can actually connect to the database
     * </p>
     */
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
                                .withConnectionProperty("include.schema.changes", "false"))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withCoder(StringUtf8Coder.of())
        );

        p.run().waitUntilFinish();
    }

    @Test
    public void testDebeziumIODefaultOutputAsJson() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.readAsJson().
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
                                .withConnectionProperty("include.schema.changes", "false")
                ));

        p.run().waitUntilFinish();
    }
    
    @Test
    public void testDebeziumIOdb2() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.readAsJson().
                        withConnectorConfiguration(
                                DebeziumIO.ConnectorConfiguration.create()
                                        .withUsername("db2inst1")
                                        .withPassword("=Password!")
                                        .withConnectorClass(Db2Connector.class)
                                        .withHostName("127.0.0.1")
                                        .withPort("50000")
                                        .withConnectionProperty("database.dbname", "TESTDB")
                                        .withConnectionProperty("tasks.max", "1")
                                        .withConnectionProperty("database.hostname","db2server")
                                        .withConnectionProperty("database.server.name", "db2server")
                                        .withConnectionProperty("database.cdcschema", "ASNCDC")
                                        .withConnectionProperty("database.include.list", "TESTDB")
                                        .withConnectionProperty("table.include.list", "DB2INST1.CUSTOMERS")
                                        .withConnectionProperty("database.history", FileDatabaseHistory.class.getName())
                                        .withConnectionProperty("database.history.file.filename", "file2-history.dat")
                        )
        ).setCoder(StringUtf8Coder.of());

        p.run().waitUntilFinish();
    }

}
