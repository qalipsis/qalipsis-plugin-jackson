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

package io.qalipsis.plugins.jackson.xml

import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.micronaut.jackson.modules.BeanIntrospectionModule
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.exceptions.InvalidSpecificationException
import io.qalipsis.api.steps.Step
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.steps.datasource.DatasourceIterativeReader
import io.qalipsis.api.steps.datasource.DatasourceRecord
import io.qalipsis.api.steps.datasource.DatasourceRecordObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.SequentialDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.jackson.JacksonDatasourceIterativeReader
import java.io.InputStreamReader

/**
 * [StepSpecificationConverter] from [XmlReaderStepSpecification] to [IterativeDatasourceStep] for a XML data source.
 *
 * @author Maxim Golokhov
 */
@StepConverter
internal class XmlReaderStepSpecificationConverter : StepSpecificationConverter<XmlReaderStepSpecification<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is XmlReaderStepSpecification<*>
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<XmlReaderStepSpecification<*>>) {
        creationContext.createdStep(convert(creationContext.stepSpecification as XmlReaderStepSpecification<out Any>))
    }

    private fun <O : Any> convert(spec: XmlReaderStepSpecification<O>): Step<*, DatasourceRecord<O>> {
        return if (spec.isReallySingleton) {
            IterativeDatasourceStep(
                spec.name,
                createReader(spec), NoopDatasourceObjectProcessor(), DatasourceRecordObjectConverter()
            )
        } else {
            SequentialDatasourceStep(
                spec.name,
                createReader(spec), NoopDatasourceObjectProcessor(), DatasourceRecordObjectConverter()
            )
        }
    }

    private fun <O : Any> createReader(spec: XmlReaderStepSpecification<O>): DatasourceIterativeReader<O> {
        val sourceUrl = spec.sourceConfiguration.url ?: throw InvalidSpecificationException("No source specified")
        return JacksonDatasourceIterativeReader(
            InputStreamReader(sourceUrl.openStream(), spec.sourceConfiguration.encoding),
            createMapper(spec).readerFor(spec.targetClass.java)
        )
    }

    private fun <O : Any> createMapper(spec: XmlReaderStepSpecification<O>): XmlMapper {
        val mapper = XmlMapper()
        mapper.registerModule(BeanIntrospectionModule())
        mapper.registerModule(JavaTimeModule())
        mapper.registerModule(KotlinModule())
        mapper.registerModule(Jdk8Module())

        spec.mapperConfiguration(mapper)

        return mapper
    }

}
