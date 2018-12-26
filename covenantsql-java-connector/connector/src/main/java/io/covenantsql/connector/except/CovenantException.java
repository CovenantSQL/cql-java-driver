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

package io.covenantsql.connector.except;

import java.sql.SQLException;

public class CovenantException extends SQLException {
    public CovenantException(String message) {
        super(String.format("CovenantException, message: %s", message), null, 0);
    }

    public CovenantException(String message, String host, int port) {
        super(String.format("CovenantException, message: %s, host: %s, port: %s",
            message, host, port), null, 0);
    }

    public CovenantException(Throwable cause, String host, int port) {
        super(String.format("CovenantException, host: %s, port: %d; %s",
            host, port, (cause == null ? "" : cause.getMessage())), null, 0, cause);
    }

    public CovenantException(String message, Throwable cause, String host, int port) {
        super(String.format("CovenantException, message: %s, host: %s, port: %d; %s",
            message, host, port, (cause == null ? "" : cause.getMessage())), null, 0, cause);
    }
}
