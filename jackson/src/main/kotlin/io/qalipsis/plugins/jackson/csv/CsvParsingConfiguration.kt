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
import javax.validation.constraints.NotEmpty

/**
 * Configuration to parse data from a CSV file.
 *
 * @author Eric Jessé
 */
@Spec
internal data class CsvParsingConfiguration internal constructor(
    var lineSeparator: @NotEmpty String = System.lineSeparator(),
    var columnSeparator: Char = ',',
    var escapeChar: Char = '\\',
    var quoteChar: Char = '"',
    var allowComments: Boolean = false
)
