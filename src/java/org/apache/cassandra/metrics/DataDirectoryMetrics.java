/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.cassandra.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import org.apache.cassandra.db.Directories;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class DataDirectoryMetrics
{

    private final MetricNameFactory factory;

    public final Meter writeTasks;
    public final Meter writeSize;
    public final Gauge estimatedAvailableSize;
    public final Gauge totalSpace;
    public final Gauge writeValueOneMinute;
    public final Gauge writeValueFiveMinutes;
    public final Gauge writeValueFifteenMinutes;
    public final Gauge writeValueMean;
    public final Meter readTasks;

    public DataDirectoryMetrics(final Directories.DataDirectory dataDirectory)
    {
        factory = new DataDirectoryMetricNameFactory(dataDirectory);

        readTasks = Metrics.newMeter(factory.createMetricName("ReadTasks"), "readTasks", TimeUnit.SECONDS);
        writeTasks = Metrics.newMeter(factory.createMetricName("WriteTasks"), "writeTasks", TimeUnit.SECONDS);
        writeSize = Metrics.newMeter(factory.createMetricName("WriteSize"), "writeSize", TimeUnit.SECONDS);
        estimatedAvailableSize = Metrics.newGauge(factory.createMetricName("EstimatedAvailableSize"), new Gauge<Long>()
        {
            @Override
            public Long value()
            {
                return dataDirectory.getEstimatedAvailableSpace();
            }
        });
        totalSpace = Metrics.newGauge(factory.createMetricName("TotalSpace"), new Gauge<Long>()
        {
            @Override
            public Long value()
            {
                return dataDirectory.location.getTotalSpace();
            }
        });
        writeValueOneMinute = Metrics.newGauge(factory.createMetricName("WriteValueOneMinute"), new Gauge<Double>()
        {
            @Override
            public Double value()
            {
                return dataDirectory.writeValueOneMinute();
            }
        });
        writeValueFiveMinutes = Metrics.newGauge(factory.createMetricName("WriteValueFiveMinutes"), new Gauge<Double>()
        {
            @Override
            public Double value()
            {
                return dataDirectory.writeValueFiveMinutes();
            }
        });
        writeValueFifteenMinutes = Metrics.newGauge(factory.createMetricName("WriteValueFifteenMinutes"), new Gauge<Double>()
        {
            @Override
            public Double value()
            {
                return dataDirectory.writeValueFifteenMinutes();
            }
        });
        writeValueMean = Metrics.newGauge(factory.createMetricName("WriteValueMean"), new Gauge<Double>()
        {
            @Override
            public Double value()
            {
                return dataDirectory.writeValueMean();
            }
        });
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        // TODO Directories.DataDirectory requires a "release" cycle in order to relase resources for this metrics (if necessary).

        Metrics.defaultRegistry().removeMetric(factory.createMetricName("ReadTasks"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("WriteTasks"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName("WriteSize"));
    }

    class DataDirectoryMetricNameFactory implements MetricNameFactory
    {
        private final String directoryName;

        DataDirectoryMetricNameFactory(Directories.DataDirectory dataDirectory)
        {
            this.directoryName = mbeanName(dataDirectory.location);
        }

        private String mbeanName(File location)
        {
            String absPath = location.getAbsolutePath();
            char[] r = new char[absPath.length()];
            for (int i = 0; i < r.length; i++)
            {
                char c = absPath.charAt(i);
                if ((c >= 'A' && c <= 'Z') ||
                        (c >= 'a' && c <= 'z') ||
                        (c >= '0' && c <= '9'))
                    r[i] = c;
                else
                    r[i] = '_';
            }
            return new String(r);
        }

        public MetricName createMetricName(String metricName)
        {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(':');
            mbeanName.append("type=DataDirectory");
            mbeanName.append(",directory=").append(directoryName);
            mbeanName.append(",name=").append(metricName);

            return new MetricName(groupName, "directory", metricName, directoryName, mbeanName.toString());
        }
    }
}
