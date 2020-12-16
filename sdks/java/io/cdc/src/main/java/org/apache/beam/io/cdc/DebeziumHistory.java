package org.apache.beam.io.cdc;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DebeziumHistory extends AbstractDatabaseHistory {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumHistory.class);
    protected List<byte[]> history;
    private Map<String, RestrictionTracker<KafkaSourceConsumerFn.OffsetHolder, Map<String, Object>>>
            restrictionTrackers;

    public DebeziumHistory(Map<String, RestrictionTracker<KafkaSourceConsumerFn.OffsetHolder,  Map<String, Object>>>
                                   restrictionTrackers) {
        this.history = new ArrayList<byte[]>();
        this.restrictionTrackers = restrictionTrackers;
    }

    @Override
    public void start() {
        super.start();
        // TODO(pabloem): Figure out how to link these. For now, we'll just link directly.
        RestrictionTracker<KafkaSourceConsumerFn.OffsetHolder, Map<String, Object>> tracker = restrictionTrackers.get(
                // JUST GETTING THE FIRST KEY. This will not work in the future.
                KafkaSourceConsumerFn.restrictionTrackers.keySet().iterator().next());
        this.history = (List<byte[]>) tracker.currentRestriction().history;
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        LOG.trace("Storing record into database history: {}", record);
        history.add(DocumentWriter.defaultWriter().writeAsBytes(record.document()));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> consumer) {
        System.out.println("Trying to recover!");
        try {
            for (byte[] record : history) {
                Document doc = DocumentReader.defaultReader().read(record);
                HistoryRecord recordObject = new HistoryRecord(doc);
                if(recordObject == null || !recordObject.isValid() ) {
                    LOG.warn("Skipping invalid database history record '{}'.", recordObject);
                } else {
                    consumer.accept(recordObject);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists() {
        return history != null && !history.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return history != null && !history.isEmpty();
    }
}
