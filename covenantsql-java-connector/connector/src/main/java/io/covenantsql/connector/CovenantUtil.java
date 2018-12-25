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

package io.covenantsql.connector;

import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

public class CovenantUtil {
    private final static Escaper COVENANT_ESCAPER = Escapers.builder()
        .addEscape('\\', "\\\\")
        .addEscape('\n', "\\n")
        .addEscape('\t', "\\t")
        .addEscape('\b', "\\b")
        .addEscape('\f', "\\f")
        .addEscape('\r', "\\r")
        .addEscape('\0', "\\0")
        .addEscape('\'', "\\'")
        .addEscape('`', "\\`")
        .build();

    public static String escape(String s) {
        if (s == null) {
            return "\\N";
        }
        return COVENANT_ESCAPER.escape(s);
    }

    public static String quoteIdentifier(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Can't quote null as identifier");
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('`');
        sb.append(COVENANT_ESCAPER.escape(s));
        sb.append('`');
        return sb.toString();
    }
}