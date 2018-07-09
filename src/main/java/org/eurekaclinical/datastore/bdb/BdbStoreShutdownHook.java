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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.arp.javautil.io.FileUtil;

/**
 * A Java virtual machine shutdown hook that shuts down known Berkeley DB
 * databases.
 * 
 * @author Andrew Post
 */
public final class BdbStoreShutdownHook extends Thread {
    private static final Logger LOGGER = 
            Logger.getLogger(BdbStoreShutdownHook.class.getPackage().getName());

    private final List<BdbEnvironmentInfo> envInfos;
    private final boolean deleteOnExit;

    /**
     * Creates the shutdown hook instance.
     * 
     * @param deleteOnExit whether known databases should be deleted after
     * shutdown.
     */
    BdbStoreShutdownHook(boolean deleteOnExit) {
        this.envInfos = new ArrayList<>();
        this.deleteOnExit = deleteOnExit;
    }

    /**
     * Adds an environment to check for databases to shutdown.
     * 
     * @param environmentInfo an environment.
     */
    void addEnvironmentInfo(BdbEnvironmentInfo environmentInfo) {
        this.envInfos.add(environmentInfo);
    }

    /**
     * Cleanly shuts down all databases in the provided environment.
     * 
     * @throws IOException if a database delete attempt failed.
     * @throws DatabaseException if an error occurs while closing the class 
     * catalog database.
     */
    void shutdown() throws IOException {
        synchronized (this) {
            for (BdbEnvironmentInfo envInfo : this.envInfos) {
                try {
                    envInfo.getClassCatalog().close();
                } catch (DatabaseException ignore) {
                    LOGGER.log(Level.SEVERE, "Failure closing class catalog", ignore);
                }
                envInfo.closeAndRemoveAllDatabaseHandles();
                
                try (Environment env = envInfo.getEnvironment()) {
                    if (this.deleteOnExit) {
                        FileUtil.deleteDirectory(env.getHome());
                    }
                }
            }
            this.envInfos.clear();
        }
    }

    /**
     * Runs the shutdown hook.
     */
    @Override
    public void run() {
        try {
            shutdown();
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Error during shutdown", ex);
        }
    }
}
