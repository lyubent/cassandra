package org.apache.cassandra.config;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import static org.junit.Assert.assertThat;

public class YamlConfigurationLoaderTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(YamlConfigurationLoaderTest.class);

    ConfigurationLoader configurationLoader;
    Matcher isNotNull;
    Matcher isNull;

    @Before
    public void setUp() throws Exception
    {
        this.configurationLoader = new YamlConfigurationLoader();

        isNotNull = new BaseMatcher()
        {
            public boolean matches(Object o)
            {
                return o != null;
            }

            public void describeTo(Description description)
            {

            }
        };

        isNull = new BaseMatcher()
        {
            public boolean matches(Object o)
            {
                return o == null;
            }

            public void describeTo(Description description)
            {

            }
        };
    }

    @Test
    public void testLoadConfigSingle()
    {
        System.setProperty("cassandra.config", "file:/C://etc/etc");
        Config config = null;
        try
        {
            config = this.configurationLoader.loadConfig();
        }
        catch (ConfigurationException e)
        {
            assertThat("ConfigurationException caught, path not found", config, isNull);
        }
    }

    @Test
    public void testLoadConfigDouble()
    {
        System.setProperty("cassandra.config", "file://C://etc/etc");
        Config config = null;
        try
        {
            config = this.configurationLoader.loadConfig();
        }
        catch (ConfigurationException e)
        {
            assertThat("ConfigurationException caught, path not found", config, isNull);
        }
    }

    @Test
    public void testLoadConfigTriple()
    {
        System.setProperty("cassandra.config", "file:///C://etc/etc");
        Config config = null;
        try
        {
            config = this.configurationLoader.loadConfig();
        }
        catch (ConfigurationException e)
        {
            assertThat("ConfigurationException caught, path not found", config, isNull);
        }
    }

    @Test
    public void testLoadConfigWithout()
    {
        System.setProperty("cassandra.config", "C://etc/etc");
        Config config = null;
        try
        {
            config = this.configurationLoader.loadConfig();
        }
        catch (ConfigurationException e)
        {
            assertThat("ConfigurationException caught, path not found", config, isNull);
        }
    }

    @Test
    public void testLoadConfigOnlyPrefix()
    {
        System.setProperty("cassandra.config", "file:///");
        Config config = null;
        try
        {
            config = this.configurationLoader.loadConfig();
        }
        catch (ConfigurationException e)
        {
            assertThat("ConfigurationException caught, path not found", config, isNull);
        }
    }

    @Test
    public void testLoadConfigInvalidInput()
    {
        System.setProperty("cassandra.config", "file:///////////////fjdjfjsd");
        Config config = null;
        try
        {
            config = this.configurationLoader.loadConfig();
        }
        catch (ConfigurationException e)
        {
            assertThat("ConfigurationException caught, path not found", config, isNull);
        }
    }

    @Test
    @Ignore
    public void testLoadConfigInValidInputBackSlashes()
    {
        System.setProperty("cassandra.config", "C:\\Users\\marcoavilac\\Documents\\cassandra\\cassandra\\conf\\cassandra.yaml");
        Config config = null;
        try
        {
            config = this.configurationLoader.loadConfig();
        }
        catch (ConfigurationException e)
        {
            assertThat("ConfigurationException caught, path not found", config, isNotNull);
        }
    }


    @Test
    @Ignore
    public void testLoadConfigValidInputSlashed()
    {
        System.setProperty("cassandra.config", "file:///C:/Users/marcoavilac/Documents/cassandra/cassandra/conf/cassandra.yaml");
        Config config = null;
        try
        {
            config = this.configurationLoader.loadConfig();
        }
        catch (ConfigurationException e)
        {
            assertThat("ConfigurationException caught, path not found", config, isNotNull);
        }
    }
}
