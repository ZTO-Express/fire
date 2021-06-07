/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.core.task;

import com.google.common.collect.Maps;
import com.zto.fire.common.anno.Scheduled;
import com.zto.fire.common.conf.FireFrameworkConf;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.common.util.ReflectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 定时任务管理器，内部使用Quartz框架
 * 为了适用于Spark，没有采用按包扫描的方式去扫描标记有@Scheduled的方法
 * 而是要主动通过TaskManager.registerTasks注册，然后扫描该实例中所有标记
 * 有@Scheduled的方法，并根据cron表达式定时执行
 *
 * @author ChengLong 2019年11月4日 18:06:21
 * @since 0.3.5
 */
public abstract class SchedulerManager implements Serializable {
    // 用于指定当前spark任务的main方法所在的对象实例
    private static Map<String, Object> taskMap;
    // 已注册的task列表
    private static Map<String, Object> alreadyRegisteredTaskMap;
    // 定时调度实例
    private static Scheduler scheduler;
    // 初始化标识
    private static AtomicBoolean isInit = new AtomicBoolean(false);
    // 定时任务黑名单，存放带有@Scheduler标识的方法名
    private static Map<String, String> blacklistMap = Maps.newHashMap();
    protected static final String DRIVER = "driver";
    protected static final String EXECUTOR = "executor";
    private static final String DEFAULT_COLOR = "\u001B[0m ] ";
    private static final Logger logger = LoggerFactory.getLogger(SchedulerManager.class);

    static {
        String blacklistMethod = FireFrameworkConf.schedulerBlackList();
        if (StringUtils.isNotBlank(blacklistMethod)) {
            String[] methods = blacklistMethod.split(",");
            for (String method : methods) {
                if (StringUtils.isNotBlank(method)) {
                    blacklistMap.put(method.trim(), method);
                }
            }
        }
    }

    protected SchedulerManager() {}

    /**
     * 初始化quartz
     */
    protected static void init() {
        if (isInit.compareAndSet(false, true)) {
            taskMap = Maps.newConcurrentMap();
            alreadyRegisteredTaskMap = Maps.newConcurrentMap();
            try {
                StdSchedulerFactory factory = new StdSchedulerFactory();
                Properties quartzProp = new Properties();
                quartzProp.setProperty("org.quartz.threadPool.threadCount", FireFrameworkConf.quartzMaxThread());
                factory.initialize(quartzProp);
                scheduler = factory.getScheduler();
            } catch (Exception e) {
                logger.error("初始化quartz发生异常", e);
            }
        }
    }

    /**
     * 添加待执行的任务列表类实例
     *
     * @param tasks 带有@Scheduled的类的实例
     */
    protected void addScanTask(Object... tasks) {
        if (tasks != null && tasks.length > 0) {
            for (Object task : tasks) {
                if (task != null) {
                    taskMap.put(task.getClass().getName(), task);
                }
            }
        }
    }


    /**
     * 判断当前是否为driver
     * @return
     */
    protected abstract String label();

    /**
     * 将标记有@Scheduled的类实例注册给定时调度管理器
     * 注：参数是类的实例而不是Class类型，是由于像Spark所在的object类型传入后，会被反射调用构造器创建另一个实例
     * 为了保证当前Spark任务所在的Object实例只有一个，约定传入的参数必须是类的实例而不是Class类型
     *
     * @param taskInstances 具有@Scheduled注解类的实例
     */
    public synchronized void registerTasks(Object... taskInstances) {
        try {
            if (!FireFrameworkConf.scheduleEnable()) return;
            SchedulerManager.init();
            addScanTask(taskInstances);
            if (!taskMap.isEmpty()) {
                for (Map.Entry<String, Object> entry : taskMap.entrySet()) {
                    // 已经注册过的任务不再重复注册
                    if (alreadyRegisteredTaskMap.containsKey(entry.getKey())) continue;

                    Class<?> clazz = entry.getValue().getClass();
                    if (clazz != null) {
                        Method[] methods = clazz.getDeclaredMethods();
                        for (Method method : methods) {
                            if (method != null) {
                                ReflectionUtils.setAccessible(method);
                                if (blacklistMap.containsKey(method.getName())) continue;
                                Scheduled anno = method.getAnnotation(Scheduled.class);
                                String label = label();
                                if (anno != null && StringUtils.isNotBlank(anno.scope()) && ("all".equalsIgnoreCase(anno.scope()) || anno.scope().equalsIgnoreCase(label))) {
                                    // 通过anno.concurrent判断是否使用并发任务实例
                                    JobDetail job = (anno.concurrent() ? JobBuilder.newJob(TaskRunner.class) : JobBuilder.newJob(TaskRunnerQueue.class)).usingJobData(clazz.getName() + "#" + method.getName(), anno.cron()).build();
                                    TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.newTrigger();

                                    if (StringUtils.isNotBlank(anno.cron())) {
                                        // 优先执行cron表达式
                                        triggerBuilder.withSchedule(CronScheduleBuilder.cronSchedule(anno.cron()));
                                    } else if (anno.fixedInterval() != -1) {
                                        // 固定频率的调度器
                                        SimpleScheduleBuilder simpleScheduleBuilder = SimpleScheduleBuilder
                                                .simpleSchedule().withIntervalInMilliseconds(anno.fixedInterval());
                                        // 设定重复执行的次数
                                        long repeatCount = anno.repeatCount();
                                        if (repeatCount == -1) {
                                            simpleScheduleBuilder.repeatForever();
                                        } else {
                                            simpleScheduleBuilder.withRepeatCount((int) repeatCount - 1);
                                        }
                                        triggerBuilder.withSchedule(simpleScheduleBuilder);
                                    }
                                    // 用于指定任务首次执行的时间
                                    if (StringUtils.isNotBlank(anno.startAt())) {
                                        // startAt优先级较高
                                        triggerBuilder.startAt(DateFormatUtils.formatDateTime(anno.startAt()));
                                    } else {
                                        // 首次延迟多久（毫秒）开始执行
                                        if (anno.initialDelay() == 0) triggerBuilder.startNow();
                                        if (anno.initialDelay() != 0 && anno.initialDelay() != -1)
                                            triggerBuilder.startAt(DateUtils.addMilliseconds(new Date(), (int) anno.initialDelay()));
                                    }
                                    // 添加到调度任务中
                                    if (scheduler == null) scheduler = StdSchedulerFactory.getDefaultScheduler();
                                    scheduler.scheduleJob(job, triggerBuilder.build());
                                    // 将已注册的task放到已注册标记列表中，防止重复注册同一个类的同一个定时方法
                                    alreadyRegisteredTaskMap.put(entry.getKey(), entry.getValue());
                                    String schedulerInfo = buildSchedulerInfo(anno);
                                    logger.info("\u001B[33m---> 已注册定时任务[ {}.{} ]，{}. \u001B[33m<---\u001B[0m", entry.getKey(), method.getName(), schedulerInfo);
                                }
                            }
                        }
                    }
                }
                if (alreadyRegisteredTaskMap.size() > 0)
                    scheduler.start();
            }
        } catch (Exception e) {
            logger.error("定时任务注册失败：作为定时任务的类必须可序列化，并且标记有@Scheduled的方法必须是无参的！", e);
        }
    }

    /**
     * 用于描述定时任务的详细信息
     *
     * @param anno Scheduled注解
     * @return 描述信息
     */
    protected String buildSchedulerInfo(Scheduled anno) {
        if (anno == null) return "Scheduled为空";
        StringBuilder schedulerInfo = new StringBuilder("\u001B[31m调度信息\u001B[0m");
        if (StringUtils.isNotBlank(anno.scope())) {
            schedulerInfo.append("[ 范围=\u001B[32m").append(anno.scope()).append(DEFAULT_COLOR);
        }
        if (StringUtils.isNotBlank(anno.cron())) {
            schedulerInfo.append("[ 频率=\u001B[33m").append(anno.cron()).append(DEFAULT_COLOR);
        } else if (anno.fixedInterval() != -1) {
            schedulerInfo.append("[ 频率=\u001B[34m").append(anno.fixedInterval()).append(DEFAULT_COLOR);
        }
        if (anno.initialDelay() != -1) {
            schedulerInfo.append("[ 延迟=\u001B[35m").append(anno.initialDelay()).append(DEFAULT_COLOR);
        }
        if (StringUtils.isNotBlank(anno.startAt())) {
            schedulerInfo.append("[ 启动时间=\u001B[36m").append(anno.startAt()).append(DEFAULT_COLOR);
        }
        if (anno.repeatCount() != -1) {
            schedulerInfo.append("[ 重复=\u001B[32m").append(anno.repeatCount()).append("\u001B[0m次 ] ");
        }
        return schedulerInfo.toString();
    }

    /**
     * 通过execute方法调用传入的指定类的指定方法
     */
    public static void execute(JobExecutionContext context) {
        try {
            JobDataMap dataMap = context.getJobDetail().getJobDataMap();
            for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                String key = entry.getKey();
                // 定时调用指定类的指定方法
                if (StringUtils.isNotBlank(key) && key.contains("#")) {
                    String[] classMethod = key.split("#");
                    Class<?> clazz = Class.forName(classMethod[0]);
                    Method method = clazz.getMethod(classMethod[1]);
                    Object instance = taskMap.get(classMethod[0]);
                    if (instance != null) method.invoke(instance);
                }
            }
        } catch (Exception e) {
            logger.error("执行execute发生异常", e);
        }
    }

    /**
     * 用于判断当前的定时调度器是否已启动
     */
    public synchronized boolean schedulerIsStarted() {
        if (scheduler == null) {
            return false;
        }
        try {
            return scheduler.isStarted();
        } catch (Exception e) {
            logger.error("获取调度器是否启用失败", e);
        }
        return false;
    }

    /**
     * 关闭定时调度
     *
     * @param waitForJobsToComplete 是否等待所有job全部执行完成再关闭
     */
    public static synchronized void shutdown(boolean waitForJobsToComplete) {
        try {
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown(waitForJobsToComplete);
                scheduler = null;
                taskMap.clear();
                alreadyRegisteredTaskMap.clear();
                logger.info("\u001B[33m---> 完成定时任务的资源回收. <---\u001B[0m");
            }
        } catch (Exception e) {
            logger.error("定时任务注册失败：作为定时任务的类必须可序列化，并且标记有@Scheduled的方法必须是无参的！", e);
        }
    }
}

