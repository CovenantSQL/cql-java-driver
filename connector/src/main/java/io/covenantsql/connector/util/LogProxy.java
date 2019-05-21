/*
 * Copyright 2018 The CovenantSQL Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.covenantsql.connector.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

public class LogProxy<T> implements InvocationHandler {
    private static final Logger log = LoggerFactory.getLogger(LogProxy.class);

    private final T object;
    private final Class<T> clazz;

    private LogProxy(Class<T> interfaceClass, T object) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalStateException("Class " + interfaceClass.getName() + " is not an interface");
        }
        clazz = interfaceClass;
        this.object = object;
    }

    public static <T> T wrap(Class<T> interfaceClass, T object) {
        if (log.isTraceEnabled()) {
            LogProxy<T> proxy = new LogProxy<T>(interfaceClass, object);
            return proxy.getProxy();
        }
        return object;
    }

    @SuppressWarnings("unchecked")
    public T getProxy() {
        //xnoinspection x
        // unchecked
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz}, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String msg =
            "Call class: " + object.getClass().getName() +
                "\nMethod: " + method.getName() +
                "\nObject: " + object +
                "\nArgs: " + Arrays.toString(args) +
                "\nInvoke result: ";
        try {
            final Object invokeResult = method.invoke(object, args);
            msg += invokeResult;
            return invokeResult;
        } catch (InvocationTargetException e) {
            msg += e.getMessage();
            throw e.getTargetException();
        } finally {
            msg = "==== CovenantSQL JDBC trace begin ====\n" + msg + "\n==== CovenantSQL JDBC trace end ====";
            log.trace(msg);
        }
    }
}
