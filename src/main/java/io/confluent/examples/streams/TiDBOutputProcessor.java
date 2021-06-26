package io.confluent.examples.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TiDBOutputProcessor implements Processor<String, Long> {

    private static final Logger LOG = Logger.getLogger(TiDBOutputProcessor.class);

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://127.0.0.1:4000/test";

    //  Database credentials
    private static final String USER = "root";
    private static final String PASS = "";

    private static final String QUERY_TEMPLATE = "INSERT INTO wordcount (word, wordcount) VALUES ('%s', %d)";

    private Connection dbConn;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        // TODO: (liquanpei) We would like to schedule a periodic execution to do batch execution
        try {
            Class.forName(JDBC_DRIVER);
            dbConn = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (final Exception e) {
            LOG.error(e);
        }
    }

    @Override
    public void process(final String key, final Long value) {
        try {
            // TODD: (liquanpei) We need to batch in insert. There are a couple ways:
            // 1. Use cache in Kafka Streams.
            // 2. Use punctuate. Need to verify the semantics.
            // 3. https://issues.apache.org/jira/browse/KAFKA-6989
            final Statement statement = dbConn.createStatement();
            final String query = String.format(QUERY_TEMPLATE, key, value);
            statement.executeUpdate(query);
        } catch (final SQLException e) {
            // To avoid data loss, we need to do infinite retry or crash the application.
            LOG.error(e);
        }
    }

    @Override
    public void close() {
        // close any resources managed by this processor
        // Note: Do not close any StateStores as these are managed by the library
        try {
            dbConn.close();
        } catch (final SQLException e) {
            LOG.error(e);
        }
    }
}
