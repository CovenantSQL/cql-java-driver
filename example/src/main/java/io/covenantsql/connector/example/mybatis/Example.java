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

package io.covenantsql.connector.example.mybatis;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.Properties;

public class Example {
    public static void main(String[] args) {
        Properties properties = new Properties();

        String host = System.getProperty("COVENANTSQL_HOST", "adp00.cn.gridb.io");
        String port = System.getProperty("COVENANTSQL_PORT", "7784");
        String database = System.getProperty("COVENANTSQL_DATABASE", "e1c4e80701773c1656a99d317148f2eada0fc6f2dad33afd5425e65bc9a35270");

        properties.setProperty("host", host);
        properties.setProperty("port", port);
        properties.setProperty("database", database);

        InputStream mybatisConfigStream = Example.class.getResourceAsStream("mybatis-config.xml");
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(mybatisConfigStream, properties);
        SqlSession session = sqlSessionFactory.openSession();
        UserMapper mapper = session.getMapper(UserMapper.class);
        mapper.createUserTableIfNotExists();
        System.out.println(mapper.selectUsers());
        mapper.addNewUser("Apple", "appleisdelicious");
        System.out.println(mapper.selectUsers());
        System.out.println(mapper.getUser("Apple"));
        mapper.changeUserPassword("Apple", "happy");
        System.out.println(mapper.getUser("Apple"));
        System.out.println(mapper.selectUsers());
        mapper.delUser("Apple");
        System.out.println(mapper.selectUsers());
    }
}
