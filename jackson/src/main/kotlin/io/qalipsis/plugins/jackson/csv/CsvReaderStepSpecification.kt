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

package io.qalipsis.plugins.jackson.csv

import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.exceptions.InvalidSpecificationException
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.plugins.jackson.AbstractJacksonStepSpecification
import io.qalipsis.plugins.jackson.JacksonScenarioSpecification
import io.qalipsis.plugins.jackson.JacksonStepSpecification
import kotlin.reflect.KClass

/**
 * Specification for a [io.qalipsis.api.steps.datasource.IterativeDatasourceStep] for CSV files.
 *
 * @property targetClass class to which the output lines should be mapped.
 *
 * @author Eric Jessé
 */
@Spec
data class CsvReaderStepSpecification<O : Any>(internal var targetClass: KClass<O>) :
    AbstractJacksonStepSpecification<O, CsvReaderStepSpecification<O>>() {

    internal val parsingConfiguration = CsvParsingConfiguration()

    internal val headerConfiguration = CsvHeaderConfiguration()

    /**
     * Sets the line separator. Default is [System.lineSeparator].
     *
     * @param sep the feed character / line separator to use
     */
    fun lineSeparator(sep: String): CsvReaderStepSpecification<O> {
        if (sep.isEmpty()) {
            throw InvalidSpecificationException("The line separator should not be empty")
        }
        parsingConfiguration.lineSeparator = sep
        return this
    }

    /**
     * Sets the line separator. Default is [System.lineSeparator].
     *
     * @param sep the feed character / line separator to use
     */
    fun lineSeparator(sep: Char): CsvReaderStepSpecification<O> {
        parsingConfiguration.lineSeparator = sep.toString()
        return this
    }

    /**
     * Sets the column separator. Default is `,`.
     *
     * @param sep the character sequence to split the columns
     */
    fun columnSeparator(sep: Char): CsvReaderStepSpecification<O> {
        parsingConfiguration.columnSeparator = sep
        return this
    }

    /**
     * Sets the character to quote sequences of string without splitting them into columns,
     * even if they contain a column or line separator. Default is `"`.
     *
     * @param quoteChar the character to quote unsplittable sequences
     */
    fun quoteChar(quoteChar: Char): CsvReaderStepSpecification<O> {
        parsingConfiguration.quoteChar = quoteChar
        return this
    }

    /**
     * Sets the character to escape a character that should otherwise be interpreted. Default is `\`.
     *
     * @param escapeChar the character to escape signs
     */
    fun escapeChar(escapeChar: Char): CsvReaderStepSpecification<O> {
        parsingConfiguration.escapeChar = escapeChar
        return this
    }

    /**
     * Enables the comments in the source files: lines where the first non-whitespace character is '#' are ignored.
     */
    fun allowComments(): CsvReaderStepSpecification<O> {
        parsingConfiguration.allowComments = true
        return this
    }

    fun header(config: CsvHeaderConfiguration.() -> Unit): CsvReaderStepSpecification<O> {
        this.headerConfiguration.config()
        return this
    }
}

/**
 * Reads a CSV resource (file, classpath resource, URL) and returns each row as a [Map].
 *
 * @author Eric Jessé
 */
fun JacksonStepSpecification<*, *, *>.csvToMap(
        configurationBlock: CsvReaderStepSpecification<Map<String, *>>.() -> Unit
): CsvReaderStepSpecification<Map<String, *>> {
    @Suppress("UNCHECKED_CAST")
    val step = CsvReaderStepSpecification(Map::class as KClass<Map<String, *>>)
    step.configurationBlock()
    this.add(step)
    return step
}

/**
 * Reads a CSV resource (file, classpath resource, URL) and returns each row as a [List].
 *
 * @author Eric Jessé
 */
fun JacksonStepSpecification<*, *, *>.csvToList(
        configurationBlock: CsvReaderStepSpecification<List<*>>.() -> Unit
): CsvReaderStepSpecification<List<*>> {
    val step = CsvReaderStepSpecification(List::class)
    step.configurationBlock()
    this.add(step)
    return step
}

/**
 * Reads a CSV resource (file, classpath resource, URL) and returns each row as an instance of OUTPUT.
 *
 * @author Eric Jessé
 */
fun <OUTPUT : Any> JacksonStepSpecification<*, *, *>.csvToObject(
        mappingClass: KClass<OUTPUT>,
        configurationBlock: CsvReaderStepSpecification<OUTPUT>.() -> Unit
): CsvReaderStepSpecification<OUTPUT> {
    val step = CsvReaderStepSpecification(mappingClass)
    step.configurationBlock()
    this.add(step)
    return step
}

/**
 * Reads a CSV resource (file, classpath resource, URL) and returns each row as a [List].
 *
 * @author Eric Jessé
 */
fun JacksonScenarioSpecification.csvToList(
        configurationBlock: CsvReaderStepSpecification<List<*>>.() -> Unit
): CsvReaderStepSpecification<List<Any?>> {
    val step = CsvReaderStepSpecification(List::class)
    step.configurationBlock()
    (this as StepSpecificationRegistry).add(step)
    return step
}

/**
 * Reads a CSV resource (file, classpath resource, URL) and returns each row as a [Map].
 *
 * @author Eric Jessé
 */
fun JacksonScenarioSpecification.csvToMap(
        configurationBlock: CsvReaderStepSpecification<Map<String, *>>.() -> Unit
): CsvReaderStepSpecification<Map<String, *>> {
    @Suppress("UNCHECKED_CAST")
    val step = CsvReaderStepSpecification(Map::class as KClass<Map<String, *>>)
    step.configurationBlock()
    (this as StepSpecificationRegistry).add(step)
    return step
}

/**
 * Reads a CSV resource (file, classpath resource, URL) and returns each row as an instance of OUTPUT.
 *
 * @author Eric Jessé
 */
fun <OUTPUT : Any> JacksonScenarioSpecification.csvToObject(
        mappingClass: KClass<OUTPUT>,
        configurationBlock: CsvReaderStepSpecification<OUTPUT>.() -> Unit
): CsvReaderStepSpecification<OUTPUT> {
    val step = CsvReaderStepSpecification(mappingClass)
    step.configurationBlock()
    (this as StepSpecificationRegistry).add(step)
    return step
}
