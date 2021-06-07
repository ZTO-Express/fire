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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
}
