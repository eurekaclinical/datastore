package org.eurekaclinical.datastore.bdb;

/*-
 * #%L
 * Datastore
 * %%
 * Copyright (C) 2016 - 2018 Emory University
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of a Berkeley DB database factory. It automatically sets the
 * cache size of created databases, makes created databases read-write, and
 * sets databases to be persistent. Its databases will not be deleted when the
 * Java virtual machine exits.
 * 
 * @author Andrew Post
 */
public class BdbPersistentStoreFactory<E, V> extends BdbStoreFactory<E, V> {
    private static final Logger LOGGER = 
            Logger.getLogger(BdbPersistentStoreFactory.class.getPackage().getName());
    
    private static final String CLASS_CATALOG = "java_class_catalog";
    
    /**
     * Creates a persistent Berkeley DB factory that creates databases at the
     * provided path.
     * @param pathname the path at which to create databases. Cannot be
     * <code>null</code>.
     */
    public BdbPersistentStoreFactory(String pathname) {
        super(pathname, false);
    }
    
    /**
     * Creates a persistent class catalog.
     * 
     * @param env the environment to use.
     * 
     * @return the class catalog.
     * 
     * @throws OperationFailureException if one of the Read Operation Failures 
     * occurs, or one of the Write Operation Failures occurs.
     * @throws EnvironmentFailureException - if an unexpected, internal or 
     * environment-wide failure occurs.
     * @throws java.lang.IllegalStateException if the provided environment 
     * has been closed.
     * @throws java.lang.IllegalStateException if there are other open handles 
     * for this database.
     */
    @Override
    protected StoredClassCatalog createClassCatalog(Environment env)
            throws IllegalArgumentException, DatabaseException {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTemporary(false);
        dbConfig.setAllowCreate(true);
        Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
        return new StoredClassCatalog(catalogDb);
    }

    /**
     * Creates an environment config that allows object creation and supports
     * transactions. In addition, it automatically calculates a cache size
     * based on available memory.
     * 
     * @return a newly created environment config instance.
     */
    @Override
    protected EnvironmentConfig createEnvConfig() {
        EnvironmentConfig envConf = new EnvironmentConfig();
        envConf.setAllowCreate(true);
        envConf.setTransactional(true);

        LOGGER.log(Level.FINE, "Calculating cache size");
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long max = memoryUsage.getMax();
        long used = memoryUsage.getUsed();
        long available = max - used;
        long cacheSize = Math.round(available / 6.0);
        envConf.setCacheSize(cacheSize);
        LOGGER.log(Level.FINE, "Cache size set to {0}", cacheSize);
        return envConf;
    }

    /**
     * Creates a database config for creating persistent databases.
     * 
     * @return a newly created database config instance.
     */
    @Override
    protected DatabaseConfig createDatabaseConfig() {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTemporary(false);
        return dbConfig;
    }
}
