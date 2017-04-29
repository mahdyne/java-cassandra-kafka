package com.mahdyne.biglog;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.cast;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        List<String> contact_points = Arrays.asList("localhost");
        CassConnectionCfg conf1 = new CassConnectionCfg(9042, contact_points, "cassandra", "cassandra", "big_log");
        CassConnection.DB.connect(conf1);
        ResultSet results = CassConnection.DB.getSession().execute("select * from table1");

        CassConnection.DB.shutdown();
    }
}
