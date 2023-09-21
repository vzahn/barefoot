/*
 * Copyright (C) 2016, BMW Car IT GmbH
 *
 * Author: Sebastian Mattheis <sebastian.mattheis@bmw-carit.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.bmwcarit.barefoot.roadmap;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bmwcarit.barefoot.road.BfmapReader;
import com.bmwcarit.barefoot.util.SourceException;

/**
 * Standard map loader that loads road map from database connection or file
 * buffer.
 */
public class Loader {
    private static Logger logger = LoggerFactory.getLogger(Loader.class);

    /**
     * Loads {@link RoadMap} object from database (or file buffer, if set to true)
     * using database connection parameters provided with the properties. For
     * details on properties, see {@link Loader#roadmap(Properties, boolean)}.
     *
     * @param propertiesPath
     *            Path to properties file.
     * @param buffer
     *            Indicates if map shall be read from file buffer and written to
     *            file buffer.
     * @return {@link RoadMap} read from source. (Note: It is not yet constructed!)
     * @throws SourceException
     *             thrown if reading properties, road types or road map data fails.
     * @throws IOException
     *             thrown if opening properties file fails.
     */
    public static RoadMap roadmap(String propertiesPath, boolean buffer) throws SourceException, IOException {
        InputStream is = new FileInputStream(propertiesPath);
        Properties props = new Properties();
        props.load(is);
        is.close();
        return roadmap(props, buffer);
    }

    /**
     * Loads {@link RoadMap} object from database (or file buffer, if set to true)
     * using database connection parameters provided with the following properties:
     * <ul>
     * <li>database.host (e.g. localhost)</li>
     * <li>database.port (e.g. 5432)</li>
     * <li>database.name (e.g. barefoot-oberbayern)</li>
     * <li>database.table (e.g. bfmap_ways)</li>
     * <li>database.user (e.g. osmuser)</li>
     * <li>database.password</li>
     * <li>database.road-types (e.g. /path/to/road-types.json)</li>
     * </ul>
     *
     * @param properties
     *            {@link Properties} object with database connection parameters.
     * @param buffer
     *            Indicates if map shall be read from file buffer and written to
     *            file buffer.
     * @return {@link RoadMap} read from source. (Note: It is not yet constructed!)
     * @throws SourceException
     *             thrown if reading properties, road types or road map data fails.
     */
    public static RoadMap roadmap(Properties properties, boolean buffer) throws SourceException {
        String database = properties.getProperty("database.name");
        if (database == null) {
            throw new SourceException("could not read database properties");
        }
        String pathDatabase = properties.getProperty("database.dir", "");

        File file = new File(Paths.get(pathDatabase, database) + ".bfmap");
        RoadMap map = null;

        if (!file.exists() || !buffer) {
            logger.info("File does not exist: " + file);
        } else {
            logger.info("load map from file {}", file.getAbsolutePath());
            map = RoadMap.load(new BfmapReader(file.getAbsolutePath()));
        }

        return map;
    }

}
