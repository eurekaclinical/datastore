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
import com.sleepycat.je.Database;
import com.sleepycat.je.Environment;

/**
 * A container for a Berkeley DB database factory's configuration parameters.
 * 
 * @author Andrew Post
 */
class BdbEnvironmentInfo {
    private final Environment environment;
    private final StoredClassCatalog classCatalog;
    private final BdbStoreFactory<?,?> storeFactory;

    /**
     * Creates a container instance containing a database factory's 
     * environment, class catalog, and database factory.
     * 
     * @param environment the environment.
     * @param classCatalog the class catalog.
     * @param storeFactory the database factory.
     */
    BdbEnvironmentInfo(Environment environment, StoredClassCatalog classCatalog,
            BdbStoreFactory<?,?> storeFactory) {
        this.environment = environment;
        this.classCatalog = classCatalog;
        this.storeFactory = storeFactory;
    }

    /**
     * Returns the class catalog.
     * 
     * @return the class catalog.
     */
    StoredClassCatalog getClassCatalog() {
        return classCatalog;
    }

    /**
     * Returns the environment.
     * 
     * @return the environment.
     */
    Environment getEnvironment() {
        return environment;
    }

    /**
     * Closes the specified database.
     * 
     * @param databaseHandle the database to close.
     * 
     * @throws EnvironmentFailureException if an unexpected, internal or 
     * environment-wide failure occurs.
     * @throws IllegalStateException if the database has been closed.
     */
    void closeAndRemoveDatabaseHandle(Database databaseHandle) {
        this.storeFactory.closeAndRemoveDatabaseHandle(databaseHandle);
    }
    
    /**
     * Closes all databases.
     * 
     */
    void closeAndRemoveAllDatabaseHandles() {
        this.storeFactory.closeAndRemoveAllDatabaseHandles();
    }
}
