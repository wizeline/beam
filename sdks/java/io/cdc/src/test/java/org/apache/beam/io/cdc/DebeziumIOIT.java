package org.apache.beam.io.cdc;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.PostgresIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;

import com.google.cloud.Timestamp;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.relational.history.FileDatabaseHistory;

/**
 * A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on an independent Postgres instance.
 *
 * <p>This test requires a running instance of Postgres. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/jdbc -DintegrationTestPipelineOptions='[
 *  "--postgresServerName=1.2.3.4",
 *  "--postgresUsername=postgres",
 *  "--postgresDatabaseName=myfancydb",
 *  "--postgresPassword=mypass",
 *  "--postgresSsl=false",
 *  "--numberOfRecords=1000" ]'
 *  --tests org.apache.beam.sdk.io.jdbc.JdbcIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class DebeziumIOIT {

	private static final String NAMESPACE = DebeziumIOIT.class.getName();
	private static int numberOfRows;
	private static PGSimpleDataSource dataSource;
	private static String tableName;
	private static String bigQueryDataset;
	private static String bigQueryTable;
	private static Long tableSize;
	private static InfluxDBSettings settings;
	  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
	@Rule public TestPipeline pipelineRead = TestPipeline.create();
	
	@BeforeClass
	public static void setup() throws Exception {
		PostgresIOTestPipelineOptions options = readIOTestPipelineOptions(PostgresIOTestPipelineOptions.class);
		
	    bigQueryDataset = options.getBigQueryDataset();
	    bigQueryTable = options.getBigQueryTable();
		numberOfRows = options.getNumberOfRecords();
		dataSource = DatabaseTestHelper.getPostgresDataSource(options);
		tableName = DatabaseTestHelper.getTestTableName("ITCDC");
		executeWithRetry(DebeziumIOIT::createTable);
		tableSize = DatabaseTestHelper.getPostgresTableSize(dataSource, tableName).orElse(0L);
		settings =
		        InfluxDBSettings.builder()
		            .withHost(options.getInfluxHost())
		            .withDatabase(options.getInfluxDatabase())
		            .withMeasurement(options.getInfluxMeasurement())
		            .get();
		
	}

	private static void createTable() throws SQLException {
		DatabaseTestHelper.createTable(dataSource, tableName);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		executeWithRetry(DebeziumIOIT::deleteTable);
	}

	private static void deleteTable() throws SQLException {
		DatabaseTestHelper.deleteTable(dataSource, tableName);
	}
	
	/** Tests writing then reading data for a postgres database. */
	@Test
	public void testWriteThenRead() {
		PipelineResult writeResult = runWrite();
		writeResult.waitUntilFinish();
		PipelineResult readResult = runRead();
		readResult.waitUntilFinish();
		gatherAndPublishMetrics(writeResult, readResult);
	}
	
	private void gatherAndPublishMetrics(PipelineResult writeResult, PipelineResult readResult) {
		String uuid = UUID.randomUUID().toString();
		String timestamp = Timestamp.now().toString();
		
		Set<Function<MetricsReader, NamedTestResult>> metricSuppliers = getWriteMetricSuppliers(uuid, timestamp);
		IOITMetrics writeMetrics = new IOITMetrics(metricSuppliers, writeResult, NAMESPACE, uuid, timestamp);
		writeMetrics.publish(bigQueryDataset, bigQueryTable);
		writeMetrics.publishToInflux(settings);
		
		IOITMetrics readMetrics = new IOITMetrics(
		        getReadMetricSuppliers(uuid, timestamp), readResult, NAMESPACE, uuid, timestamp);
		readMetrics.publish(bigQueryDataset, bigQueryTable);
		readMetrics.publishToInflux(settings);
	}
	
	private Set<Function<MetricsReader, NamedTestResult>> getWriteMetricSuppliers(String uuid, String timestamp) {
		Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
		Optional<Long> postgresTableSize = DatabaseTestHelper.getPostgresTableSize(dataSource, tableName);

	    suppliers.add(
	        reader -> {
	          long writeStart = reader.getStartTimeMetric("write_time");
	          long writeEnd = reader.getEndTimeMetric("write_time");
	          return NamedTestResult.create(
	              uuid, timestamp, "write_time", (writeEnd - writeStart) / 1e3);
	        });

	    postgresTableSize.ifPresent(
	        tableFinalSize ->
	            suppliers.add(
	                ignore ->
	                    NamedTestResult.create(
	                        uuid, timestamp, "total_size", tableFinalSize - tableSize)));
	    return suppliers;
	}

	private Set<Function<MetricsReader, NamedTestResult>> getReadMetricSuppliers(String uuid, String timestamp) {
		Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
		suppliers.add(
		    reader -> {
		      long readStart = reader.getStartTimeMetric("read_time");
			  long readEnd = reader.getEndTimeMetric("read_time");
			  return NamedTestResult.create(uuid, timestamp, "read_time", (readEnd - readStart) / 1e3);
        });
		return suppliers;
	}
	
	/**
	 * Writes the test dataset to postgres.
	 *
	 * <p>This method does not attempt to validate the data - we do so in the read test. This does
	 * make it harder to tell whether a test failed in the write or read phase, but the tests are much
	 * easier to maintain (don't need any separate code to write test data for read tests to the
	 * database.)
	*/
	private PipelineResult runWrite() {
		pipelineWrite
			.apply(GenerateSequence.from(0).to(numberOfRows))
			.apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
			.apply(ParDo.of(new TimeMonitor<>(NAMESPACE, "write_time")))
			.apply(JdbcIO.<TestRow>write()
						.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
						.withStatement(String.format("insert into %s values(?, ?)", tableName))
						.withPreparedStatementSetter(new JdbcTestHelper.PrepareStatementFromTestRow()));
		return pipelineWrite.run();
	}
	
  /**
   * Read the test dataset from postgres and validate its contents.
   *
   * <p>When doing the validation, we wish to ensure that we: 1. Ensure *all* the rows are correct
   * 2. Provide enough information in assertions such that it is easy to spot obvious errors (e.g.
   * all elements have a similar mistake, or "only 5 elements were generated" and the user wants to
   * see what the problem was.
   *
   * <p>We do not wish to generate and compare all of the expected values, so this method uses
   * hashing to ensure that all expected data is present. However, hashing does not provide easy
   * debugging information (failures like "every element was empty string" are hard to see), so we
   * also: 1. Generate expected values for the first and last 500 rows 2. Use containsInAnyOrder to
   * verify that their values are correct. Where first/last 500 rows is determined by the fact that
   * we know all rows have a unique id - we can use the natural ordering of that key.
   */
	private PipelineResult runRead() {
		PCollection<String> pipelineCdc =
		        pipelineRead
		            .apply(DebeziumIO.readAsJson()
		                    .withConnectorConfiguration(
		                    		DebeziumIO.ConnectorConfiguration.create()
	                                .withUsername(dataSource.getUser())
	                                .withPassword(dataSource.getPassword())
	                                .withConnectorClass(PostgresConnector.class)
	                                .withHostName(dataSource.getServerName())
	                                .withPort(String.valueOf(dataSource.getPortNumber()))
	                                .withConnectionProperty("database.dbname", dataSource.getDatabaseName())
	                                .withConnectionProperty("database.server.name", "dbserver2")
	                                .withConnectionProperty("schema.include.list", "public")
	                                .withConnectionProperty("table.include.list", "public."+tableName)
	                                .withConnectionProperty("slot.name", "dbzslot2")
	                                .withConnectionProperty("include.schema.changes", "false")
	                                .withConnectionProperty("database.history", FileDatabaseHistory.class.getName())
                                    .withConnectionProperty("database.history.file.filename", "file2-history.dat")
		                    		))
		            .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, "read_time")));
		PCollection<String> windowed_items = pipelineCdc.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));
		PAssert.thatSingleton(windowed_items.apply("Count All", Combine.globally(Count.<String>combineFn()).withoutDefaults() )).isEqualTo((long) 10);
		return pipelineRead.run();
	}
 
}
