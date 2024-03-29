/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.jackson.csv

import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.exceptions.InvalidSpecificationException

/**
 * Configuration of the header of the CSV file.
 *
 * @property skipFirstDataRow skips the first row of data, in case it is not real data (when the CSV was generated from an external tool for instance)
 * @property withHeader when set to true, the first lines of the payload is considered as the header
 * @property withHeader when set to true - requires [withHeader] true as well - the values in each column of the header are used for the keys of the output map
 *
 * @author Eric Jessé
 */
@Spec
data class CsvHeaderConfiguration internal constructor(
        internal var skipFirstDataRow: Boolean = false,
        internal var withHeader: Boolean = false
) {

    internal val columns = mutableListOf<CsvColumnConfiguration<*>?>()

    /**
     * Skips the first row of data, in case it is not real data
     * (when the CSV was generated from an external tool for instance).
     */
    fun skipFirstDataRow(): CsvHeaderConfiguration {
        this.skipFirstDataRow = true
        return this
    }

    /**
     * The CSV file as a header line just after the skipped lines.
     */
    fun withHeader(): CsvHeaderConfiguration {
        this.withHeader = true
        return this
    }

    /**
     * Adds the configuration of a column added at the end of the current list.
     *
     * @param name name of the column or field.
     */
    fun column(name: String): CsvColumnConfiguration<String?> {
        return column(columns.size, name)
    }

    /**
     * Adds the configuration of a column.
     *
     * @param index 0-based index of the column.
     * @param name name of the column or field.
     */
    fun column(index: Int, name: String): CsvColumnConfiguration<String?> {
        if (name.isBlank()) {
            throw InvalidSpecificationException("A column name cannot be blank")
        }
        if (columns.filterNotNull().any { it.name == name }) {
            throw InvalidSpecificationException("A column with name $name already exists")
        }
        if (columns.filterNotNull().any { it.index == index }) {
            throw InvalidSpecificationException("A column with index $index already exists")
        }

        return CsvColumnConfiguration<String?>(
                index,
                name,
                CsvColumnType.NULLABLE_STRING
        ).also { column ->
            if (columns.size > index) {
                columns[index] = column
            } else {
                while (columns.size < index) {
                    columns.add(null)
                }
                columns.add(column)
            }
        }
    }

    /**
     * Adds the configuration of a column.
     *
     * @param index 0-based index of the column.
     */
    fun column(index: Int): CsvColumnConfiguration<String?> {
        return column(index, "field-$index")
    }
}
