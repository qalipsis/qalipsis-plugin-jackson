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

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.index
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.prop
import io.qalipsis.api.exceptions.InvalidSpecificationException
import io.qalipsis.test.assertk.prop
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * @author Eric Jessé
 */
internal class CsvHeaderConfigurationTest {

    @Test
    internal fun `should have default values`() {
        val config = CsvHeaderConfiguration()

        assertThat(config).all {
            prop(CsvHeaderConfiguration::columns).hasSize(0)
            prop("skipFirstDataRow").isEqualTo(false)
            prop("withHeader").isEqualTo(false)
        }
    }

    @Test
    internal fun `should skip first data row`() {
        val config = CsvHeaderConfiguration().skipFirstDataRow()

        assertThat(config).all {
            prop(CsvHeaderConfiguration::columns).hasSize(0)
            prop("skipFirstDataRow").isEqualTo(true)
            prop("withHeader").isEqualTo(false)
        }
    }

    @Test
    internal fun `should have a header`() {
        val config = CsvHeaderConfiguration().withHeader()

        assertThat(config).all {
            prop(CsvHeaderConfiguration::columns).hasSize(0)
            prop("skipFirstDataRow").isEqualTo(false)
            prop("withHeader").isEqualTo(true)
        }
    }

    @Test
    internal fun `should have new columns`() {
        val config = CsvHeaderConfiguration().also {
            it.column("my-column")
            it.column("my-column-1")
            it.column(4)
            it.column(2, "my-column-2")
        }

        assertThat(config).all {
            prop(CsvHeaderConfiguration::columns).all {
                hasSize(5)
                index(0).isNotNull().all {
                    prop(CsvColumnConfiguration<*>::name).isEqualTo("my-column")
                    prop(CsvColumnConfiguration<*>::index).isEqualTo(0)
                }
                index(1).isNotNull().all {
                    prop(CsvColumnConfiguration<*>::name).isEqualTo("my-column-1")
                    prop(CsvColumnConfiguration<*>::index).isEqualTo(1)
                }
                index(2).isNotNull().all {
                    prop(CsvColumnConfiguration<*>::name).isEqualTo("my-column-2")
                    prop(CsvColumnConfiguration<*>::index).isEqualTo(2)
                }
                index(3).isNull()
                index(4).isNotNull().all {
                    prop(CsvColumnConfiguration<*>::name).isEqualTo("field-4")
                    prop(CsvColumnConfiguration<*>::index).isEqualTo(4)
                }
            }
            prop("skipFirstDataRow").isEqualTo(false)
            prop("withHeader").isEqualTo(false)
        }
    }

    @Test
    internal fun `should throw an error when creating a column with blank name`() {
        assertThrows<InvalidSpecificationException> {
            CsvHeaderConfiguration().column("  ")
        }
    }

    @Test
    internal fun `should throw an error when creating two columns with same index`() {
        val config = CsvHeaderConfiguration().also {
            it.column("any")
        }

        assertThrows<InvalidSpecificationException> {
            config.column(0, "my-column")
        }
        assertThrows<InvalidSpecificationException> {
            config.column(0)
        }
    }

    @Test
    internal fun `should throw an error when creating two columns with same name`() {
        val config = CsvHeaderConfiguration().also {
            it.column("any")
        }

        assertThrows<InvalidSpecificationException> {
            config.column(1, "any")
        }
        assertThrows<InvalidSpecificationException> {
            config.column("any")
        }
    }
}
