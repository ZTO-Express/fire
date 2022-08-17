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

package com.zto.fire.common.util;

import com.zto.fire.common.conf.FirePS1Conf;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.zto.fire.common.util.UnitFormatUtils.readable;

/**
 * 反射工具类，获取各元素信息后缓存到map中
 * Created by ChengLong on 2017-03-30.
 */
public class ReflectionUtils {
    private static final Map<Class<?>, Map<String, Field>> cacheFieldMap = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Map<String, Method>> cacheMethodMap = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(ReflectionUtils.class);

    private ReflectionUtils() {
    }

    public static void setAccessible(Field field) {
        if (field != null) field.setAccessible(true);
    }

    public static void setAccessible(Method method) {
        if (method != null) method.setAccessible(true);
    }

    /**
     * 根据类名反射获取Class对象
     */
    public static Class<?> forName(String className) {
        try {
            return Class.forName(className);
        } catch (Exception e) {
            logger.error("未找到类信息：" + className, e);
            return null;
        }
    }

    /**
     * 用于判断某类是否存在指定的字段
     */
    public static boolean containsField(Class<?> clazz, String fieldName) {
        Field field = getFieldByName(clazz, fieldName);
        return field != null ? true : false;
    }

    /**
     * 获取所有公有字段，并返回Map
     */
    private static Map<String, Field> getFields(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyMap();
        }
        Field[] fields = clazz.getFields();
        Map<String, Field> fieldMap = new HashMap<>(fields.length);
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * 获取所有声明字段，并返回Map
     */
    private static Map<String, Field> getDeclaredFields(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyMap();
        }
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Field> fieldMap = new HashMap<>(fields.length);
        for (Field field : fields) {
            setAccessible(field);
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * 获取所有字段，含私有和继承而来的，并返回Map
     */
    public static Map<String, Field> getAllFields(Class<?> clazz) {
        if (!cacheFieldMap.containsKey(clazz)) {
            Map<String, Field> fieldMap = new HashMap<>();
            fieldMap.putAll(getFields(clazz));
            fieldMap.putAll(getDeclaredFields(clazz));
            cacheFieldMap.put(clazz, fieldMap);
        }

        return cacheFieldMap.get(clazz);
    }

    /**
     * 根据成员变量名称获取Filed类型（从缓存中获取）
     */
    public static Field getFieldByName(Class<?> clazz, String fieldName) {
        return getAllFields(clazz).get(fieldName);
    }

    /**
     * 获取所有方法，含私有和继承而来的，并返回Map
     */
    public static Map<String, Method> getAllMethods(Class<?> clazz) {
        if (!cacheMethodMap.containsKey(clazz)) {
            Map<String, Method> methodMap = new HashMap<>();
            methodMap.putAll(getMethods(clazz));
            methodMap.putAll(getDeclaredMethods(clazz));
            cacheMethodMap.put(clazz, methodMap);
        }

        return cacheMethodMap.get(clazz);
    }

    /**
     * 根据方法名称获取Method类型（从缓存中获取）
     *
     * @param clazz      类类型
     * @param methodName 方法名称
     * @return Method
     */
    public static Method getMethodByName(Class<?> clazz, String methodName) {
        return getAllMethods(clazz).get(methodName);
    }

    /**
     * 根据方法名称获取Method类型（从缓存中获取）
     *
     * @param className      类名
     * @param methodName 方法名称
     * @return Method
     */
    public static Method getMethodByName(String className, String methodName) {
        return getAllMethods(forName(className)).get(methodName);
    }


    /**
     * 用于判断某类是否存在指定的方法名
     */
    public static boolean containsMethod(Class<?> clazz, String methodName) {
        Method method = getMethodByName(clazz, methodName);
        return method != null ? true : false;
    }

    /**
     * 获取所有公有方法，并返回Map
     */
    private static Map<String, Method> getMethods(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyMap();
        }
        Method[] methods = clazz.getMethods();
        Map<String, Method> methodMap = new HashMap<>(methods.length);
        for (Method method : methods) {
            methodMap.put(method.getName(), method);
        }
        return methodMap;
    }

    /**
     * 获取所有声明方法，并返回Map
     */
    private static Map<String, Method> getDeclaredMethods(Class<?> clazz) {
        if (clazz == null) {
            return Collections.emptyMap();
        }
        Method[] methods = clazz.getDeclaredMethods();
        Map<String, Method> methodMap = new HashMap<>(methods.length);
        for (Method method : methods) {
            setAccessible(method);
            methodMap.put(method.getName(), method);
        }
        return methodMap;
    }

    /**
     * 获取指定field的类型
     */
    public static Class<?> getFieldType(Class<?> clazz, String fieldName) {
        if (clazz == null || StringUtils.isBlank(fieldName)) return null;

        try {
            Map<String, Field> fieldMap = getAllFields(clazz);
            if (fieldMap == null) {
                return null;
            }
            Field field = fieldMap.get(fieldName);
            if (field != null) {
                return field.getType();
            }
        } catch (Exception e) {
            logger.error("指定的Field:" + fieldName + "不存在，请检查", e);
        }
        return null;
    }

    /**
     * 获取指定的annotation
     *
     * @param scope      annotation所在的位置
     * @param memberName 成员名称，指定获取指定成员的Annotation实例
     */
    private static <T extends Annotation> Annotation getAnnotation(Class<?> clazz, ElementType scope, String memberName, Class<T> annoClass) {
        try {
            if (ElementType.FIELD == scope) {
                Field field = clazz.getDeclaredField(memberName);
                setAccessible(field);
                return field.getAnnotation(annoClass);
            } else if (ElementType.METHOD == scope) {
                Method method = clazz.getDeclaredMethod(memberName);
                setAccessible(method);
                return method.getAnnotation(annoClass);
            } else if (ElementType.TYPE == scope) {
                return clazz.getAnnotation(annoClass);
            }
        } catch (Exception e) {
            logger.error("获取annotation出现异常", e);
        }
        return null;
    }

    /**
     * 获取指定的annotation
     *
     * @param scope      annotation所在的位置
     * @param memberName 成员名称，指定获取指定成员的Annotation实例
     */
    private static List<Annotation> getAnnotations(Class<?> clazz, ElementType scope, String memberName) {
        try {
            if (ElementType.FIELD == scope) {
                Field field = clazz.getDeclaredField(memberName);
                setAccessible(field);
                return Arrays.asList(field.getDeclaredAnnotations());
            } else if (ElementType.METHOD == scope) {
                Method method = clazz.getDeclaredMethod(memberName);
                setAccessible(method);
                return Arrays.asList(method.getDeclaredAnnotations());
            } else if (ElementType.TYPE == scope) {
                return Arrays.asList(clazz.getDeclaredAnnotations());
            }
        } catch (Exception e) {
            logger.error("获取annotation出现异常", e);
        }
        return Collections.emptyList();
    }

    /**
     * 获取Field指定的annotation
     */
    public static <T extends Annotation> Annotation getFieldAnnotation(Class<?> clazz, String fieldName, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.FIELD, fieldName, annoClass);
    }

    /**
     * 获取Field所有annotation
     */
    public static List<Annotation> getFieldAnnotations(Class<?> clazz, String fieldName) {
        return getAnnotations(clazz, ElementType.FIELD, fieldName);
    }

    /**
     * 获取Method指定的annotation
     */
    public static <T extends Annotation> Annotation getMethodAnnotation(Class<?> clazz, String methodName, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.METHOD, methodName, annoClass);
    }

    /**
     * 获取Method所有annotation
     */
    public static List<Annotation> getMethodAnnotations(Class<?> clazz, String methodName) {
        return getAnnotations(clazz, ElementType.METHOD, methodName);
    }

    /**
     * 获取类指定annotation
     */
    public static <T extends Annotation> Annotation getClassAnnotation(Class<?> clazz, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.TYPE, clazz.getName(), annoClass);
    }

    /**
     * 获取类所有annotation
     */
    public static List<Annotation> getClassAnnotations(Class<?> clazz) {
        return getAnnotations(clazz, ElementType.TYPE, clazz.getName());
    }

    /**
     * 根据注解调用对应的方法
     * @param target
     * 目标对象
     * @param annotationClass
     * 注解类型
     * @param args
     * 方法反射调用传参
     */
    public static void invokeAnnoMethod(Object target, Class<? extends Annotation> annotationClass, Object ...args) throws Exception {
        if (target == null || annotationClass == null) return;

        try {
            for (Method method : getAllMethods(target.getClass()).values()) {
                if (method.isAnnotationPresent(annotationClass)) {
                    method.invoke(target, args);
                }
            }
        } catch (Exception e) {
            logger.error("反射调用方法失败，请检查：" + target.getClass().getName());
            throw e;
        }
    }

    /**
     * 根据注解调用步骤方法
     * @param target
     * 目标对象
     * @param annotations
     * 注解类型列表
     */
    public static void invokeStepAnnoMethod(Object target, Class<? extends Annotation> ... annotations) throws Exception {
        if (target == null || annotations == null || annotations.length == 0) return;
        long successCount = 0, failedCount = 0, begin = System.currentTimeMillis();

        try {
            Collection<Method> methods = getAllMethods(target.getClass()).values();
            for (Class<? extends Annotation> annotationClass : annotations) {
                for (Method method : methods) {
                    // 避免因为将注解标注到process方法上导致process执行多次
                    if (!"process".equals(method.getName()) && method.isAnnotationPresent(annotationClass)) {
                        Annotation anno = method.getAnnotation(annotationClass);
                        Object retVal = getAnnoFieldValue(anno, "value");
                        String desc = retVal == null ? "" : retVal.toString();
                        if (StringUtils.isBlank(desc)) desc = "开始执行";
                        String step = annotationClass.getSimpleName();
                        logger.warn(FirePS1Conf.GREEN() + " " + step + ". " + desc + " " + FirePS1Conf.DEFAULT());

                        long start = System.currentTimeMillis();
                        Object skipError = getAnnoFieldValue(anno, "skipError");
                        try {
                            method.invoke(target, null);
                            successCount += 1;
                        } catch (Exception e) {
                            long end = System.currentTimeMillis();
                            logger.error(FirePS1Conf.RED() + " " + step + ". 执行报错！耗时："+ (readable(end - start, UnitFormatUtils.TimeUnitEnum.MS))  + " " + FirePS1Conf.DEFAULT() + "\n", e);
                            boolean isSkip = Boolean.parseBoolean(skipError.toString());
                            failedCount += 1;
                            if (!isSkip) throw e;
                        }
                        long end = System.currentTimeMillis();
                        logger.warn(FirePS1Conf.GREEN() + " " + step + ". 执行耗时：" + (readable(end - start, UnitFormatUtils.TimeUnitEnum.MS)) + " " + FirePS1Conf.DEFAULT() + "\n");
                    }
                }
            }
            long finalEnd = System.currentTimeMillis();
            long allCount = successCount + failedCount;
            if (allCount > 0) {
                logger.warn(FirePS1Conf.GREEN() + " Finished. 总计：" + allCount + "个 成功：" + successCount + "个 失败：" + failedCount + "个, 执行耗时：" + (readable(finalEnd - begin, UnitFormatUtils.TimeUnitEnum.MS)) + " " + FirePS1Conf.DEFAULT() + "\n");
            }
        } catch (Exception e) {
            logger.error("反射调用方法失败，请检查：" + target.getClass().getName());
            throw e;
        }
    }

    /**
     * 获取指定Annotation的字段配置值
     * @param anno
     * 具体的注解类
     * @param methodName
     * 注解的field
     */
    public static Object getAnnoFieldValue(Annotation anno, String methodName) throws Exception {
        Method[] methods = anno.getClass().getMethods();
        Object retVal = null;
        for (Method method : methods) {
            if (method.getName().equalsIgnoreCase(methodName)) {
                retVal = method.invoke(anno, null);
            }
        }
        return retVal;
    }
}
