package com.mahdyne.biglog;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mahdi on 4/29/17.
 */
public enum CassConnection {
    DB;
    private Cluster cluster;
    private Session session;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassConnection.class);
    public void connect(CassConnectionCfg conf) {
        if (cluster == null && session == null) {
            cluster = Cluster.builder()
                    .withPort(conf.getPort())
                    .withCredentials(conf.getUsername(), conf.getPassword())
                    .addContactPoints(conf.getContactPoints().toArray(new String[conf.getContactPoints().size()]))
                    .build();
            session = cluster.connect(conf.getKeyspace());
        }
        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: " + metadata.getClusterName() + " with partitioner: " + metadata.getPartitioner());
        metadata.getAllHosts().stream().forEach((host) -> {
            LOGGER.info("Cassandra datacenter: " + host.getDatacenter() + " | address: " + host.getAddress() + " | rack: " + host.getRack());
        });

    }
    public void shutdown(){
        LOGGER.info("Shutting down the session and cassandra cluster");
        if (null != session) {
            session.close();
        }
        if (null != cluster) {
            cluster.close();
        }
    }
    public Session getSession() {
        if (session == null) {
            throw new IllegalStateException("No connection initialized");
        }
        return session;

    }
}
