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

package org.apache.flink.configuration;

import com.zto.fire.common.conf.FireFrameworkConf;
import com.zto.fire.common.util.OSUtils;
import com.zto.fire.common.util.PropUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Global configuration object for Flink. Similar to Java properties configuration objects it
 * includes key-value pairs which represent the framework's configuration.
 */
@Internal
public final class GlobalConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

    public static final String FLINK_CONF_FILENAME = "flink-conf.yaml";

    // the keys whose values should be hidden
    private static final String[] SENSITIVE_KEYS =
            new String[] {"password", "secret", "fs.azure.account.key", "apikey"};

    // the hidden content to be displayed
    public static final String HIDDEN_CONTENT = "******";

    // --------------------------------------------------------------------------------------------

    private GlobalConfiguration() {}

    // --------------------------------------------------------------------------------------------

    /**
     * Loads the global configuration from the environment. Fails if an error occurs during loading.
     * Returns an empty configuration object if the environment variable is not set. In production
     * this variable is set but tests and local execution/debugging don't have this environment
     * variable set. That's why we should fail if it is not set.
     *
     * @return Returns the Configuration
     */
    public static Configuration loadConfiguration() {
        return loadConfiguration(new Configuration());
    }

    /**
     * Loads the global configuration and adds the given dynamic properties configuration.
     *
     * @param dynamicProperties The given dynamic properties
     * @return Returns the loaded global configuration with dynamic properties
     */
    public static Configuration loadConfiguration(Configuration dynamicProperties) {
        final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
        if (configDir == null) {
            return new Configuration(dynamicProperties);
        }

        return loadConfiguration(configDir, dynamicProperties);
    }

    /**
     * Loads the configuration files from the specified directory.
     *
     * <p>YAML files are supported as configuration files.
     *
     * @param configDir the directory which contains the configuration files
     */
    public static Configuration loadConfiguration(final String configDir) {
        return loadConfiguration(configDir, null);
    }

    /**
     * Loads the configuration files from the specified directory. If the dynamic properties
     * configuration is not null, then it is added to the loaded configuration.
     *
     * @param configDir directory to load the configuration from
     * @param dynamicProperties configuration file containing the dynamic properties. Null if none.
     * @return The configuration loaded from the given configuration directory
     */
    public static Configuration loadConfiguration(
            final String configDir, @Nullable final Configuration dynamicProperties) {

        if (configDir == null) {
            throw new IllegalArgumentException(
                    "Given configuration directory is null, cannot load configuration");
        }

        final File confDirFile = new File(configDir);
        if (!(confDirFile.exists())) {
            throw new IllegalConfigurationException(
                    "The given configuration directory name '"
                            + configDir
                            + "' ("
                            + confDirFile.getAbsolutePath()
                            + ") does not describe an existing directory.");
        }

        // get Flink yaml configuration file
        final File yamlConfigFile = new File(confDirFile, FLINK_CONF_FILENAME);

        if (!yamlConfigFile.exists()) {
            throw new IllegalConfigurationException(
                    "The Flink config file '"
                            + yamlConfigFile
                            + "' ("
                            + yamlConfigFile.getAbsolutePath()
                            + ") does not exist.");
        }

        Configuration configuration = loadYAMLResource(yamlConfigFile);

        if (dynamicProperties != null) {
            configuration.addAll(dynamicProperties);
        }

        return configuration;
    }

    /**
     * Loads a YAML-file of key-value pairs.
     *
     * <p>Colon and whitespace ": " separate key and value (one per line). The hash tag "#" starts a
     * single-line comment.
     *
     * <p>Example:
     *
     * <pre>
     * jobmanager.rpc.address: localhost # network address for communication with the job manager
     * jobmanager.rpc.port   : 6123      # network port to connect to for communication with the job manager
     * taskmanager.rpc.port  : 6122      # network port the task manager expects incoming IPC connections
     * </pre>
     *
     * <p>This does not span the whole YAML specification, but only the *syntax* of simple YAML
     * key-value pairs (see issue #113 on GitHub). If at any point in time, there is a need to go
     * beyond simple key-value pairs syntax compatibility will allow to introduce a YAML parser
     * library.
     *
     * @param file the YAML file to read from
     * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
     */
    private static Configuration loadYAMLResource(File file) {
        final Configuration config = new Configuration();

        // TODO: ------------ start：二次开发代码 --------------- //
        Method setSetting = null;
        try {
            Class env = Class.forName("org.apache.flink.runtime.util.EnvironmentInformation");
            setSetting = env.getMethod("setSetting", String.class, String.class);
        } catch (Exception e) {
            LOG.error("获取EnvironmentInformation.setSetting()失败", e);
        }
        // TODO: ------------ end：二次开发代码 --------------- //

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    LOG.info(
                            "Loading configuration property: {}, {}",
                            key,
                            isSensitive(key) ? HIDDEN_CONTENT : value);
                    config.setString(key, value);

                    // TODO: ------------ start：二次开发代码 --------------- //
                    try {
                        setSetting.invoke(null, key, value);
                    } catch (Exception e) {
                        LOG.error("二次开发代码异常，反射调用配置产生失败！", e);
                    }
                    // TODO: ------------ end：二次开发代码 --------------- //
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        // TODO: ------------ start：二次开发代码 --------------- //
        fireBootstrap(config);
        // TODO: ------------ end：二次开发代码 --------------- //

        return config;
    }

    // TODO: ------------ start：二次开发代码 --------------- //
    private static AtomicBoolean isStart = new AtomicBoolean(false);
    // 用于判断是JobManager还是TaskManager
    private static boolean isJobManager = false;
    // fire rest服务占用端口
    private static ServerSocket restServerSocket;
    // 任务的运行模式
    private static String runMode;
    private static final Map<String, String> settings = new HashMap<>();

    static {
        try {
            restServerSocket = new ServerSocket(0);
        } catch (Exception e) {
            LOG.error("创建Socket失败", e);
        }
    }

    /**
     * 获取配置信息
     */
    public static Map<String, String> getSettings() {
        return settings;
    }

    /**
     * 获取随机分配的Rest端口号
     */
    public static int getRestPort() {
        return restServerSocket.getLocalPort();
    }

    /**
     * 获取rest服务端口号，并关闭Socket
     */
    public static int getRestPortAndClose() {
        int port = restServerSocket.getLocalPort();
        if (restServerSocket != null && !restServerSocket.isClosed()) {
            try {
                restServerSocket.close();
            } catch (Exception e) {
                LOG.error("关闭Rest Socket失败", e);
            }
        }
        return port;
    }

    /**
     * fire框架相关初始化动作
     */
    private static void fireBootstrap(Configuration config) {
        if (isStart.compareAndSet(false, true)) {
            // 加载必要的配置文件
            loadTaskConfiguration(config);
        }
    }

    /**
     * 获取当前任务运行模式
     */
    public static String getRunMode() {
        return runMode;
    }

    /**
     * 加载必要的配置文件
     */
    private static void loadTaskConfiguration(Configuration config) {
        // 用于加载任务同名配置文件中的flink参数
        // 获取当前任务的类名称
        String className = config.getString("$internal.application.main", config.getString("flink.fire.className", ""));
        // 获取当前任务的运行模式：yarn-application或yarn-per-job
        runMode = config.getString("flink.execution.target", config.getString("execution.target", ""));

        try {
            Class env = Class.forName("org.apache.flink.runtime.util.EnvironmentInformation");
            Method method = env.getMethod("isJobManager");
            isJobManager = Boolean.valueOf(method.invoke(null) + "");
        } catch (Exception e) {
            LOG.error("调用EnvironmentInformation.isJobManager()失败", e);
        }

        // 配置信息仅在JobManager端进行加载，TaskManager端会被主动的merge
        if (isJobManager && className != null && className.contains(".")) {
            String simpleClassName = className.substring(className.lastIndexOf('.') + 1);
            if (simpleClassName.length() > 0) {
                PropUtils.setProperty("driver.class.name", className);
                // TODO: 判断批处理模式，并加载对应配置文件
                // PropUtils.load(FireFrameworkConf.FLINK_BATCH_CONF_FILE)
                PropUtils.loadFile(FireFrameworkConf.FLINK_STREAMING_CONF_FILE());
                // 将所有configuration信息同步到PropUtils中
                PropUtils.setProperties(config.confData);
                // 加载用户公共配置文件
                PropUtils.load(FireFrameworkConf.userCommonConf());
                // 加载任务同名的配置文件
                // PropUtils.loadJobConf(className);
                // 构建fire rest接口地址
                PropUtils.setProperty(FireFrameworkConf.FIRE_REST_URL(), "http://" + OSUtils.getIp() + ":" + getRestPort());
                // 加载外部系统配置信息，覆盖同名配置文件中的配置，实现动态替换
                PropUtils.loadJobConf(className);
                PropUtils.setProperty("flink.run.mode", runMode);

                Map<String, String> settingMap = (Map<String, String>) JavaConversions.mapAsJavaMap(PropUtils.settings());
                settingMap.forEach((k, v) -> {
                    config.setString(k, v);
                    settings.put(k, v);
                });
                LOG.info("main class：" + PropUtils.getProperty("driver.class.name"));
            }
        }
    }

    /**
     * Check whether the key is a hidden key.
     *
     * @param key the config key
     */
    public static boolean isSensitive(String key) {
        Preconditions.checkNotNull(key, "key is null");
        final String keyInLower = key.toLowerCase();
        // 用于隐藏webui中敏感信息
        String hideKeys = ((Map<String, String>) JavaConversions.mapAsJavaMap(PropUtils.settings())).getOrDefault("fire.conf.print.blacklist", "password,secret,fs.azure.account.key");
        if (hideKeys != null && hideKeys.length() > 0) {
            String[] hideKeyArr = hideKeys.split(",");
            for (String hideKey : hideKeyArr) {
                if (keyInLower.length() >= hideKey.length()
                        && keyInLower.contains(hideKey)) {
                    return true;
                }
            }
        }
        return false;
    }
    // TODO: ------------ end：二次开发代码 ----------------- //
}