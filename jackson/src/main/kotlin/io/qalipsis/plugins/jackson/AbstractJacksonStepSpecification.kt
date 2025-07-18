/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.jackson

import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.BroadcastSpecification
import io.qalipsis.api.steps.LoopableSpecification
import io.qalipsis.api.steps.NoSingletonSpecification
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.UnicastSpecification
import io.qalipsis.api.steps.datasource.DatasourceRecord
import io.qalipsis.plugins.jackson.config.SourceConfiguration
import java.net.URL
import java.nio.charset.Charset
import java.nio.file.Path

/**
 *
 * @author Eric Jessé
 */
abstract class AbstractJacksonStepSpecification<O : Any?, SELF : AbstractJacksonStepSpecification<O, SELF>> :
    AbstractStepSpecification<Unit, DatasourceRecord<O>, SELF>(),
    JacksonStepSpecification<Unit, DatasourceRecord<O>, SELF>,
    LoopableSpecification, UnicastSpecification, BroadcastSpecification, NoSingletonSpecification {

    internal val sourceConfiguration = SourceConfiguration()

    override val singletonConfiguration: SingletonConfiguration = SingletonConfiguration(SingletonType.SEQUENTIAL)

    /**
     * Reads the CSV data from a plain file on the file system.
     *
     * @param path the path to the file, either absolute or relative to the working directory.
     */
    fun file(path: String): SELF {
        sourceConfiguration.url = Path.of(path).toUri().toURL()
        @Suppress("UNCHECKED_CAST")
        return this as SELF
    }

    /**
     * Reads the CSV data from a class path resource.
     *
     * @param path the path to the resource, the leading slash is ignored.
     */
    fun classpath(path: String): SELF {
        sourceConfiguration.url =
            this::class.java.classLoader.getResource(if (path.startsWith("/")) path.substring(1) else path)
        @Suppress("UNCHECKED_CAST")
        return this as SELF
    }

    /**
     * Reads the CSV data from the a URL.
     *
     * @param url the url to access to the CSV resource.
     */
    fun url(url: String): SELF {
        sourceConfiguration.url = URL(url)
        @Suppress("UNCHECKED_CAST")
        return this as SELF
    }

    /**
     * Sets the charset of the source file. Default is UTF-8.
     *
     * @param encoding the name of the charset
     */
    fun encoding(encoding: String): SELF {
        sourceConfiguration.encoding = Charset.forName(encoding)
        @Suppress("UNCHECKED_CAST")
        return this as SELF
    }

    /**
     * Sets the charset of the source file. Default is UTF-8.
     *
     * @param encoding the charset to use
     */
    fun encoding(encoding: Charset): SELF {
        sourceConfiguration.encoding = encoding
        @Suppress("UNCHECKED_CAST")
        return this as SELF
    }
}
