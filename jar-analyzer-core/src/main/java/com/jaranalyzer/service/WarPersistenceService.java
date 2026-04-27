package com.jaranalyzer.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Persistence service for WAR analyses.
 * Stores data under {data-dir}/war/{warName}/ and serves stored.war files.
 * Extends PersistenceService so all streaming, caching, and correction methods
 * are inherited — only the storage root differs.
 */
@Service("warPersistenceService")
public class WarPersistenceService extends PersistenceService {

    public WarPersistenceService(@Value("${app.data-dir:data}") String dataBase) {
        super(new WarDataPaths(dataBase));
    }
}
