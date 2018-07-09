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

import java.util.Map;
import java.io.IOError;
import java.util.Collection;
import java.util.Set;

/**
 * Represents a data store, which can be either temporary or persistent. It can
 * be treated just like a {@link java.util.Map}, but defines two extra methods,
 * <code>shutdown</code>, which closes the store, and <code>isClosed</code>. In
 * addition, many of the standard map methods may throw an {@link IOError}
 * runtime exception.
 * 
 * @param <K>
 *            the key type to store
 * @param <V>
 *            the value type to store
 * 
 * @author Michel Mansour
 */
public interface DataStore<K, V> extends Map<K, V> {

    /**
     * Performs any clean up of the store and shuts it down.
     * 
     * @throws IOError if an error occurred shutting down the 
     * database.
     */
    void shutdown();

    /**
     * Checks whether the store has already been shut down
     * 
     * @return <code>true</code> if the store has been shutdown;
     *         <code>false</code> otherwise
     */
    boolean isClosed();

    /**
     * Returns whether the data store is empty.
     * @return <code>true</code> if the data store is empty, or <code>false</code>
     * if not.
     * 
     * @throws IOError if an error occurs while checking the data store.
     */
    @Override
    boolean isEmpty();

    /**
     * Clears the contents of the data store.
     * 
     * @throws IOError if an error occurs modifying the data store.
     */
    @Override
    void clear();

    /**
     * Returns the number of key-value pairs in the data store.
     * 
     * @throws IOError if an error occurs while checking the data store.
     */
    @Override
    int size();

    /**
     * Returns whether the given key is in the data store.
     * 
     * @throws IOError if an error occurs while checking the data store.
     */
    @Override
    boolean containsKey(Object key);

    /**
     * Returns whether the given value is in the data store.
     * 
     * @throws IOError if an error occurs while checking the data store.
     */
    @Override
    boolean containsValue(Object value);

    /**
     * Copes all of the key-value pairs from the given map into the data store.
     * @param m a map.
     * 
     * @throws IOError if an error occurs while writing the map to the data
     * store.
     */
    @Override
    void putAll(Map<? extends K, ? extends V> m);

    /**
     * Returns a set view of the keys in the data store.
     * 
     * @return a set containing the keys in the data store.
     * 
     * @throws IOError if an error occurs while creating the set view.
     */
    @Override
    Set<K> keySet();

    /**
     * Returns a set view of the key-value pairs in this data store.
     * 
     * @return a set of map entries.
     * 
     * @throws IOError if an error occurs while creating the set view.
     */
    @Override
    Set<Entry<K, V>> entrySet();

    /**
     * Returns a collection view of the values contained in this map.
     * @return a collection of values.
     * 
     * @throws IOError if an error occurs while creating the set view.
     */
    @Override
    Collection<V> values();

    /**
     * Puts the given key-value pair into the data store.
     * 
     * @param key a key.
     * @param value a value.
     * @return the value that used to be in the data store, or the new value,
     * if there was not one previously.
     * 
     * @throws IOError if an error occurs while adding the key-value pair to
     * the data store.
     */
    @Override
    V put(K key, V value);

    /**
     * Returns the value corresponding to the given key.
     * 
     * @param key a key.
     * @return the value corresponding to the given key.
     * 
     * @throws IOError if an error occurs getting the value corresponding to
     * the given key.
     */
    @Override
    V get(Object key);

    /**
     * Removes the mapping for this key from this map if present.
     * 
     * @param key a key.
     * @return the removed mapping, if any.
     * 
     * @throws IOError if an error occurred while attempting to remove a 
     * mapping.
     */
    @Override
    V remove(Object key);
    
}
