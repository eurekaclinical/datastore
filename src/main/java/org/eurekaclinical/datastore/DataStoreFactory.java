package org.eurekaclinical.datastore;

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

import java.io.IOException;

/**
 * Interface for factory classes for creating data stores.
 * 
 * @author Andrew Post
 * @param <E> the key to store.
 * @param <V> the value to store.
 */
public interface DataStoreFactory<E, V> extends AutoCloseable {
    
    /**
     * Checks whether a database with the given name exists.
     * 
     * @param dbName the name of the database to check.
     * @return <code>true</code> or <code>false</code>.
     * @throws java.io.IOException if an error occurs while accessing the data store.
     */
    boolean exists(String dbName) throws IOException;
    
    /**
     * Opens a data store, creating it if needed.
     *
     * @param dbName the data store's name.
     *
     * @return the opened data store.
     *
     * @throws IOException if an error occurs while getting or creating the data store.
     */
    DataStore<E, V> getInstance(String dbName) throws IOException;

    /**
     * Cleanly shuts down the store and cleans up associated resources.
     *
     * @throws IOException if an error occurred during shutdown.
     */
    @Override
    void close() throws IOException;
    
}
