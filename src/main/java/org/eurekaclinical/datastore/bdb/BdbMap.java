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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.util.RuntimeExceptionWrapper;
import java.io.IOError;
import org.eurekaclinical.datastore.DataStore;

/**
 * A Berkeley DB implementation of the DataStore interface.
 *
 * This implementation is thread-safe.
 *
 * @author Andrew Post
 * @param <K> the key type to store.
 * @param <V> the value type to store.
 */
public class BdbMap<K, V> implements DataStore<K, V> {

    private final Database db;
    private final StoredMap<K, V> storedMap;
    private boolean isClosed;
    private BdbEnvironmentInfo envInfo;

    /**
     * A creates a new Berkeley DB instance.
     * 
     * @param envInfo the database environment configuration.
     * @param database the database.
     * 
     * @throws DatabaseException if any database-related exception occurred.
     */
    BdbMap(BdbEnvironmentInfo envInfo, Database database) {
        this.db = database;
        this.envInfo = envInfo;
        StoredClassCatalog catalog = envInfo.getClassCatalog();
        EntryBinding<K> kBinding = new SerialBinding<>(catalog, null);
        EntryBinding<V> vBinding = new SerialBinding<>(catalog, null);
        this.storedMap = new StoredMap<>(this.db, kBinding, vBinding, true);
    }

    @Override
    public void shutdown() {
        synchronized (this.db) {
            if (!this.isClosed) {
                try {
                    this.envInfo.closeAndRemoveDatabaseHandle(this.db);
                } catch (EnvironmentFailureException | IllegalStateException ex) {
                    throw new IOError(ex);
                }
                this.isClosed = true;
            }
        }
    }

    @Override
    public boolean isClosed() {
        synchronized (this.db) {
            return isClosed;
        }
    }

    @Override
    public void clear() {
        try {
            this.storedMap.clear();
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper | UnsupportedOperationException ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public boolean containsKey(Object arg0) {
        try {
            return this.storedMap.containsKey(arg0);
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public boolean containsValue(Object arg0) {
        try {
            return this.storedMap.containsValue(arg0);
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        try {
            return this.storedMap.entrySet();
        } catch (RuntimeExceptionWrapper ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public V get(Object arg0) {
        try {
            return this.storedMap.get(arg0);
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            return this.storedMap.isEmpty();
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper | UnsupportedOperationException ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public Set<K> keySet() {
        try {
            return this.storedMap.keySet();
        } catch (RuntimeExceptionWrapper ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public V put(K arg0, V arg1) {
        try {
            return this.storedMap.put(arg0, arg1);
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper | UnsupportedOperationException
                | IllegalArgumentException ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> arg0) {
        try {
            this.storedMap.putAll(arg0);
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper | UnsupportedOperationException ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public V remove(Object arg0) {
        try {
            return this.storedMap.remove(arg0);
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper | UnsupportedOperationException ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public int size() {
        try {
            return this.storedMap.size();
        } catch (OperationFailureException | EnvironmentFailureException
                | RuntimeExceptionWrapper ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public Collection<V> values() {
        try {
            return this.storedMap.values();
        } catch (RuntimeExceptionWrapper ex) {
            throw new IOError(ex);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        final BdbMap<K, V> other = (BdbMap<K, V>) obj;
        if (this.storedMap != other.storedMap
                && (this.storedMap == null
                || !this.storedMap.equals(other.storedMap))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return this.storedMap.hashCode();
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            shutdown();
        } finally {
            super.finalize();
        }
    }
}
