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

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.prop
import io.qalipsis.plugins.jackson.csv.CsvColumnType.*
import io.qalipsis.test.assertk.prop
import org.junit.jupiter.api.Test

/**
 * @author Eric Jessé
 */
internal class CsvColumnConfigurationTest {

    @Test
    internal fun `should make the column a nullable string`() {
        val column = buildDefaultColumn(FLOAT).nullableString(true)

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_STRING)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }

        column.nullableString(false)

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_STRING)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(false)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a non nullable string`() {
        val column = buildDefaultColumn().string(true)

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(STRING)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }

        column.string(false)

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(STRING)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(false)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a nullable integer`() {
        val column = buildDefaultColumn().nullableInteger()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_INTEGER)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a non nullable integer`() {
        val column = buildDefaultColumn().integer()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(INTEGER)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a nullable big integer`() {
        val column = buildDefaultColumn().nullableBigInteger()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_BIG_INTEGER)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a non nullable big integer`() {
        val column = buildDefaultColumn().bigInteger()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(BIG_INTEGER)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a nullable double`() {
        val column = buildDefaultColumn().nullableDouble()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_DOUBLE)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a non nullable double`() {
        val column = buildDefaultColumn().double()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(DOUBLE)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a nullable big decimal`() {
        val column = buildDefaultColumn().nullableBigDecimal()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_BIG_DECIMAL)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a non nullable big decimal`() {
        val column = buildDefaultColumn().bigDecimal()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(BIG_DECIMAL)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a nullable long`() {
        val column = buildDefaultColumn().nullableLong()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_LONG)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a non nullable long`() {
        val column = buildDefaultColumn().long()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(LONG)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a nullable float`() {
        val column = buildDefaultColumn().nullableFloat()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_FLOAT)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a non nullable float`() {
        val column = buildDefaultColumn().float()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(FLOAT)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a nullable boolean`() {
        val column = buildDefaultColumn().nullableBoolean()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(NULLABLE_BOOLEAN)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column a non nullable boolean`() {
        val column = buildDefaultColumn().boolean()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(BOOLEAN)
            prop(CsvColumnConfiguration<*>::trim).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(false)
        }
    }

    @Test
    internal fun `should make the column an array with default separator`() {
        val column = buildDefaultColumn(INTEGER).array()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(INTEGER)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::listSeparator).isEqualTo(";")
        }
    }

    @Test
    internal fun `should make the column an array with specified separator`() {
        val column = buildDefaultColumn(INTEGER).array(":::")

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(INTEGER)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(true)
            prop(CsvColumnConfiguration<*>::listSeparator).isEqualTo(":::")
        }
    }

    @Test
    internal fun `should enable error management on simple column`() {
        val column = buildDefaultColumn().integer().ignoreError(123)

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(INTEGER)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(false)
            prop("ignoreError").isEqualTo(true)
            prop(CsvColumnConfiguration<*>::defaultValue).isEqualTo(123)
        }
    }

    @Test
    internal fun `should enable error management with default value on array`() {
        val column = buildDefaultColumn().integer().array().ignoreError()

        assertThat(column).all {
            prop(CsvColumnConfiguration<*>::type).isEqualTo(INTEGER)
            prop(CsvColumnConfiguration<*>::isArray).isEqualTo(true)
            prop("ignoreError").isEqualTo(true)
            prop(CsvColumnConfiguration<*>::defaultValue).isEqualTo(null)
        }
    }

    private fun buildDefaultColumn(type: CsvColumnType = NULLABLE_STRING) = CsvColumnConfiguration<String>(
            0, "column", type
    )
}
