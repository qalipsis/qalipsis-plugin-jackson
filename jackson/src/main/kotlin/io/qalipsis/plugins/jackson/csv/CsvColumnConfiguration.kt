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
import java.math.BigDecimal
import java.math.BigInteger
import javax.validation.constraints.NotBlank
import javax.validation.constraints.Positive

/**
 * Configuration of a single CSV column.
 *
 * @property index the 0-based ordinal of the column
 * @property name the name of the field to designate the column in the result. If null or empty, and no header value is specified the ordinal as string is used.
 * @property type type of the column for casting and validation. When the column is for an array, this applies to each value.
 * @property trim when true, leading and trailing white spaces of the values are removed. When the column is for an array, this applies to each value.
 * @property isArray defines that the column should be mapped onto an array of values.
 * @property listSeparator separator of the collection values in a single column.
 * @property ignoreError when true, casting and validation errors are ignored and [defaultValue] is returned, which has to match the type of the column.
 * @property defaultValue default value - matching the type of the column - to use when an error occurs but is ignored.
 *
 * @author Eric Jessé
 */
@Spec
data class CsvColumnConfiguration<T> internal constructor(
        internal var index: @Positive Int,
        internal var name: @NotBlank String,
        internal var type: CsvColumnType,
        internal var trim: Boolean = false,
        internal var isArray: Boolean = false,
        internal var listSeparator: String = ";",
        internal var ignoreError: Boolean = false,
        internal var defaultValue: T? = null
) {

    /**
     * Values of the column are nullable strings. Empty strings will be considered as null.
     *
     * @param trim trim leading and tailing whitespace signs around the value
     */
    fun nullableString(trim: Boolean = false): CsvColumnConfiguration<String?> {
        this.type = CsvColumnType.NULLABLE_STRING
        this.trim = trim
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<String?>
    }

    /**
     * Values of the column are non-nullable strings. Empty strings will be considered as null and generate an error.
     *
     * @param trim trim leading and tailing whitespace signs around the value
     */
    fun string(trim: Boolean = false): CsvColumnConfiguration<String> {
        this.type = CsvColumnType.STRING
        this.trim = trim
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<String>
    }

    /**
     * Values of the column are nullable integers. Empty strings will be considered as null.
     */
    fun nullableInteger(): CsvColumnConfiguration<Int?> {
        this.type = CsvColumnType.NULLABLE_INTEGER
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Int?>
    }

    /**
     * Values of the column are non-nullable integers. Empty strings will be considered as null and generate an error.
     */
    fun integer(): CsvColumnConfiguration<Int> {
        this.type = CsvColumnType.INTEGER
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Int>
    }

    /**
     * Values of the column are nullable big integers. Empty strings will be considered as null.
     */
    fun nullableBigInteger(): CsvColumnConfiguration<BigInteger?> {
        this.type = CsvColumnType.NULLABLE_BIG_INTEGER
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<BigInteger?>
    }

    /**
     * Values of the column are non-nullable big integers. Empty strings will be considered as null and generate an error.
     */
    fun bigInteger(): CsvColumnConfiguration<BigInteger> {
        this.type = CsvColumnType.BIG_INTEGER
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<BigInteger>
    }

    /**
     * Values of the column are nullable doubles. Empty strings will be considered as null.
     */
    fun nullableDouble(): CsvColumnConfiguration<Double?> {
        this.type = CsvColumnType.NULLABLE_DOUBLE
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Double?>
    }

    /**
     * Values of the column are non-nullable doubles. Empty strings will be considered as null and generate an error.
     */
    fun double(): CsvColumnConfiguration<Double> {
        this.type = CsvColumnType.DOUBLE
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Double>
    }

    /**
     * Values of the column are nullable big decimals. Empty strings will be considered as null.
     */
    fun nullableBigDecimal(): CsvColumnConfiguration<BigDecimal?> {
        this.type = CsvColumnType.NULLABLE_BIG_DECIMAL
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<BigDecimal?>
    }

    /**
     * Values of the column are non-nullable big decimals. Empty strings will be considered as null and generate an error.
     */
    fun bigDecimal(): CsvColumnConfiguration<BigDecimal> {
        this.type = CsvColumnType.BIG_DECIMAL
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<BigDecimal>
    }

    /**
     * Values of the column are nullable longs. Empty strings will be considered as null.
     */
    fun nullableLong(): CsvColumnConfiguration<Long?> {
        this.type = CsvColumnType.NULLABLE_LONG
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Long?>
    }

    /**
     * Values of the column are non-nullable longs. Empty strings will be considered as null and generate an error.
     */
    fun long(): CsvColumnConfiguration<Long> {
        this.type = CsvColumnType.LONG
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Long>
    }

    /**
     * Values of the column are nullable floats. Empty strings will be considered as null.
     */
    fun nullableFloat(): CsvColumnConfiguration<Float?> {
        this.type = CsvColumnType.NULLABLE_FLOAT
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Float?>
    }

    /**
     * Values of the column are non-nullable floats. Empty strings will be considered as null and generate an error.
     */
    fun float(): CsvColumnConfiguration<Float> {
        this.type = CsvColumnType.FLOAT
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Float>
    }

    /**
     * Values of the column are nullable booleans. Empty strings will be considered as false.
     */
    fun nullableBoolean(): CsvColumnConfiguration<Boolean?> {
        this.type = CsvColumnType.NULLABLE_BOOLEAN
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Boolean?>
    }

    /**
     * Values of the column are non-nullable booleans. Empty strings will be considered as null and generate an error.
     */
    fun boolean(): CsvColumnConfiguration<Boolean> {
        this.type = CsvColumnType.BOOLEAN
        this.trim = true
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Boolean>
    }

    /**
     * Values of the column are non-nullable floats.
     */
    fun array(separator: String = ";"): CsvColumnConfiguration<Array<T>> {
        this.isArray = true
        this.listSeparator = separator
        @Suppress("UNCHECKED_CAST")
        return this as CsvColumnConfiguration<Array<T>>
    }

    /**
     * Ignores all the conversion and casting errors and returns the default value instead.
     */
    fun ignoreError(defaultValue: T? = null): CsvColumnConfiguration<T> {
        this.ignoreError = true
        this.defaultValue = defaultValue
        return this
    }
}
