/*
 * Copyright 2018 The ThunderDB Authors.
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

package io.covenantsql.connector.response.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CovenantQueryResponseBean {
    @JsonProperty
    String status;
    @JsonProperty
    boolean success;
    @JsonProperty
    CovenantQueryResponseDataBean data;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public CovenantQueryResponseDataBean getData() {
        return data;
    }

    public void setData(CovenantQueryResponseDataBean data) {
        this.data = data;
    }

    public static final class CovenantQueryResponseDataBean {
        @JsonProperty
        List<String> types;
        @JsonProperty
        List<String> columns;
        @JsonProperty
        List<List<Object>> rows;

        public List<String> getTypes() {
            return types;
        }

        public void setTypes(List<String> types) {
            this.types = types;
        }

        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }

        public List<List<Object>> getRows() {
            return rows;
        }

        public void setRows(List<List<Object>> rows) {
            this.rows = rows;
        }
    }
}
