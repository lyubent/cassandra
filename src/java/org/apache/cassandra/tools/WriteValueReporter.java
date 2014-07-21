/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.reporting.CsvReporter;

public class WriteValueReporter extends CsvReporter
{
    private final File outputDir;

    public WriteValueReporter(File outputDir)
    {
        super(Metrics.defaultRegistry(), new WriteValuePredicate(), outputDir);
        this.outputDir = outputDir;
    }

    @Override
    protected PrintStream createStreamForMetric(MetricName metricName) throws IOException
    {
        final File newFile = new File(outputDir, metricName.getScope() + "_" + metricName.getName() + ".csv");
        if (newFile.createNewFile()) {
            return new PrintStream(new FileOutputStream(newFile));
        }
        throw new IOException("Unable to create " + newFile);
    }

    private static class WriteValuePredicate implements MetricPredicate
    {
        @Override
        public boolean matches(MetricName name, Metric metric)
        {
            return name.getType().equalsIgnoreCase("directory");
        }
    }
}
