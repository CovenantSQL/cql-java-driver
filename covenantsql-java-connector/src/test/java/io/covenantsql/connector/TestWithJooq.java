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

package io.covenantsql.connector;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

public class TestWithJooq {
    @Test
    public void testConnectToDatabase() {
        try {
            Class.forName("io.covenantsql.connector.CovenantDriver");
            Properties properties = new Properties();

            properties.setProperty("key_path", "write.test.covenantsql.io-key.pem");
            properties.setProperty("cert_path", "write.test.covenantsql.io.pem");
            properties.setProperty("sslmode", "none");
            properties.setProperty("ssl", "true");

            Connection conn = DriverManager.getConnection(
                "jdbc:covenantsql://192.168.2.100:11105/0fcd0c97995ace3b4f1f299cde854b3777568e61f9216155514ff5a215547cd4", properties);
            DSLContext ctx = DSL.using(conn);


            System.out.println(ctx.select().from("tracking").limit(10).fetch().formatCSV());
            System.out.println(ctx.select().from("tracking").limit(200).fetch().formatCSV());
            System.out.println(ctx.insertInto(DSL.table("tracking")).columns(
                DSL.field("scanDateTime"),
                DSL.field("zoneCode"),
                DSL.field("opCode"),
                DSL.field("remark"))
                .values(new Date(), "XXXX", 123, "test remark").execute());
            System.out.println(ctx.select().from("tracking").orderBy(DSL.field("scanDateTime").desc()).limit(1).fetchOne().intoList());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
