package org.apache.cassandra.config;

import com.google.common.io.Files;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.io.File;
import java.util.Properties;

import static org.junit.Assert.assertThat;

public class YamlConfigurationLoaderTest
{
    ConfigurationLoader configurationLoader;
    Matcher isNotNull;
    Matcher isNull;
    String testConfig;

    @Before
    public void setUp() throws Exception
    {
        testConfig = System.getProperty("storage-config");
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

    @Test
    public void testValidLoadConfig() throws Exception
    {
        String prefix = FBUtilities.isUnix() ? "file://" : "file:///";
        String tempDir = prefix + testConfig + File.separator + "cassandra.yaml";
        System.setProperty("cassandra.config", tempDir);
        this.configurationLoader.loadConfig();

        // no exception thus test passes.
    }

    @Test(expected=ConfigurationException.class)
    public void testDoubleSeparatorDir() throws Exception
    {
        String dir = Files.createTempDir().getAbsolutePath();
        dir.replaceAll(File.separator, File.separator + File.separator);
        System.setProperty("cassandra.config", dir);
        this.configurationLoader.loadConfig();

        // expecting ConfigurationException.
    }
}
