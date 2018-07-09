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

import org.eurekaclinical.datastore.DataStoreFactory;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract factory for creating new Berkeley DB key-value data stores. Berkeley
 * DB databases have three configuration points: the environment, which allows
 * configuring the cache size of created databases; the database, which allows
 * optimizing the database for the types of data being stored; and the class
 * catalog, which allows configuring how classes are stored. These may be set by
 * implementing {@link #createEnvConfig() }, {@link #createDatabaseConfig() },
 * and {@link #createClassCatalog(com.sleepycat.je.Environment)}, respectively.
 * The resulting concrete class manages the lifecycle of Berkeley DB databases.
 *
 * @author Andrew Post
 *
 * @param <E> the key type to store.
 * @param <V> the value type to store.
 */
public abstract class BdbStoreFactory<E, V> implements DataStoreFactory<E, V> {
    private static final Logger LOGGER = 
            Logger.getLogger(BdbStoreFactory.class.getPackage().getName());

    private BdbEnvironmentInfo envInfo;
    private final File envFile;
    private final List<Database> databaseHandles;
    private final BdbStoreShutdownHook shutdownHook;

    /**
     * Creates a new Berkeley DB data store factory instance, setting the
     * directory in which databases will be created, and whether or not to
     * delete created databases when the Java virtual machine exits.
     *
     * @param pathname the directory in which databases will be created. Cannot
     * be <code>null</code>.
     * @param deleteOnExit whether or not to delete created databases upon JVM
     * exit.
     */
    protected BdbStoreFactory(String pathname, boolean deleteOnExit) {
        assert pathname != null : "pathname cannot be null";
        this.envFile = new File(pathname);
        this.databaseHandles
                = Collections.synchronizedList(new ArrayList<>());
        this.shutdownHook = new BdbStoreShutdownHook(deleteOnExit);
        Runtime.getRuntime().addShutdownHook(this.shutdownHook);
    }

    /**
     * Cleanly shuts down the store and cleans up associated resources.
     *
     * @throws IOException if an error occurred during shutdown.
     */
    @Override
    public final void shutdown() throws IOException {
        this.shutdownHook.shutdown();
    }

    /**
     * Opens a database, creating it if needed.
     *
     * @param dbName the database's name.
     *
     * @return the created and opened database.
     *
     * @throws IOException if an error occurs creating the database.
     */
    @Override
    public BdbMap<E, V> newInstance(String dbName) throws IOException {
        if (dbName == null) {
            throw new IllegalArgumentException("dbName cannot be null");
        }
        try {
        synchronized (this) {
            if (this.envInfo == null) {
                createEnvironmentInfo();
            }
        }
        DatabaseConfig dbConfig = createDatabaseConfig();
        Database databaseHandle
                = this.envInfo.getEnvironment().openDatabase(null, dbName,
                        dbConfig);
        this.databaseHandles.add(databaseHandle);
        return new BdbMap<>(this.envInfo, databaseHandle);
        } catch (OperationFailureException | EnvironmentFailureException
                | IllegalStateException | IllegalArgumentException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Returns a new environment config instance. This should be called only by
     * the {@link #newInstance(java.lang.String) } method.
     *
     * @return a new environment config instance.
     */
    protected abstract EnvironmentConfig createEnvConfig();

    /**
     * Returns a new database config instance. This should be called only by the {@link #newInstance(java.lang.String)
     * } method.
     *
     * @return a new database config instance.
     */
    protected abstract DatabaseConfig createDatabaseConfig();

    /**
     * Returns a new class catalog instance. This should be called only by the 
     * {@link #newInstance(java.lang.String) } method.
     *
     * @param env a Berkeley DB environment instance.
     *
     * @return a new class catalog instance.
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
    protected abstract StoredClassCatalog createClassCatalog(Environment env);

    /**
     * Closes a database that was previously created by this factory instance.
     *
     * @param databaseHandle the database to close.
     * 
     * @throws EnvironmentFailureException if an unexpected, internal or 
     * environment-wide failure occurs.
     * @throws IllegalStateException if the database has been closed.
     */
    final void closeAndRemoveDatabaseHandle(Database databaseHandle) {
        try {
            databaseHandle.close();
        } catch (EnvironmentFailureException | IllegalStateException ex) {
            try {
                LOGGER.log(Level.SEVERE,
                        "Error closing database {0}", databaseHandle.getDatabaseName());
            } catch (EnvironmentFailureException | IllegalStateException ex2) {
                ex.addSuppressed(ex2);
            }
            throw ex;
        } finally {
            this.databaseHandles.remove(databaseHandle);
        }
    }

    /**
     * Closes all databases that this factory instance previously created.
     * 
     * @throws EnvironmentFailureException if an unexpected, internal or 
     * environment-wide failure occurs.
     * @throws IllegalStateException if the database has been closed.
     */
    final void closeAndRemoveAllDatabaseHandles() {
        for (Database databaseHandle : this.databaseHandles) {
            try {
                databaseHandle.close();
            } catch (EnvironmentFailureException | IllegalStateException ex) {
                try {
                    LOGGER.log(Level.SEVERE,
                            "Error closing database {0}", databaseHandle.getDatabaseName());
                } catch (EnvironmentFailureException | IllegalStateException ex2) {
                    ex.addSuppressed(ex2);
                }
            }
        }
        this.databaseHandles.clear();
    }

    private void createEnvironmentInfo() {
        Environment env = createEnvironment();
        StoredClassCatalog classCatalog = createClassCatalog(env);
        this.envInfo = new BdbEnvironmentInfo(env, classCatalog, this);
        this.shutdownHook.addEnvironmentInfo(this.envInfo);
    }

    /**
     * Creates a Berkeley DB database environment from the provided environment 
     * configuration.
     * 
     * @return an environment instance.
     * 
     * @throws SecurityException if the directory for storing the databases
     * could not be created.
     * @throws EnvironmentNotFoundException if the environment does not exist 
     * (does not contain at least one log file) and the EnvironmentConfig 
     * AllowCreate parameter is false.
     * @throws EnvironmentLockedException when an environment cannot be opened 
     * for write access because another process has the same environment open 
     * for write access. Warning: This exception should be handled when an 
     * environment is opened by more than one process.
     * @throws VersionMismatchException when the existing log is not 
     * compatible with the version of JE that is running. This occurs when a 
     * later version of JE was used to create the log. Warning: This exception 
     * should be handled when more than one version of JE may be used to access 
     * an environment.
     * @throws EnvironmentFailureException if an unexpected, internal or 
     * environment-wide failure occurs.
     * @throws java.lang.UnsupportedOperationException if this environment was 
     * previously opened for replication and is not being opened read-only.
     * @throws java.lang.IllegalArgumentException if an invalid parameter is 
     * specified, for example, an invalid EnvironmentConfig parameter.
     * 
     */
    private Environment createEnvironment() {
        EnvironmentConfig envConf = createEnvConfig();
        if (!envFile.exists()) {
            envFile.mkdirs();
        }
        LOGGER.log(Level.INFO,
                "Initialized BerkeleyDB cache environment at {0}",
                envFile.getAbsolutePath());

        return new Environment(this.envFile, envConf);
    }
}
