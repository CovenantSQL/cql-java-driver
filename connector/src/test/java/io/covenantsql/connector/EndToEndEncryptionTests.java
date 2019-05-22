/*
 * Copyright 2019 The CovenantSQL Authors.
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

import io.covenantsql.connector.util.EndToEndEncryption;
import org.apache.commons.codec.binary.Hex;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

class testCase {
    String raw;
    String password;
    String possibleEncrypted;

    public testCase(String raw, String password, String possibleEncrypted) {
        this.raw = raw;
        this.password = password;
        this.possibleEncrypted = possibleEncrypted;
    }
}


public class EndToEndEncryptionTests {
    testCase[] cases;

    @BeforeMethod
    public void setUp() {
        cases = new testCase[]{
                new testCase(
                        "11",
                        ";#K]As9C*6L",
                        "a372ea2c158a2f99d386e309db4355a659a7a8dd3986fd1d94f7604256061609"
                ),
                new testCase(
                        "111282C128421286712857128C2128EF" +
                                "128B7671283C128571287512830128EC" +
                                "128391281A1312849128381281E1286A" +
                                "12871128621287A9D12857128C412886" +
                                "128FD12834128DA128F5",
                        "",
                        "1bfb6a7fda3e3eb1e14c9afd0baefe86" +
                                "c90979101f179db7e48a0fa7617881e8" +
                                "f752c59fb512bb86b8ed69c5644bf2dc" +
                                "30fbcd3bf79fb20342595c84fad00e46" +
                                "2fab3e51266492a3d5d085e650c1e619" +
                                "6278d7f5185c263440ec6fd940ffbb85"
                ),
                new testCase(
                        "11",
                        "'K]\"#'pi/1/JD2",
                        "a83d152777ce3a1c0710b03676ae867c86ab0a47b3ca080f825683ac1079eb41"
                ),
                new testCase(
                        "11111111111111111111111111111111",
                        "",
                        "7dda438c4256a63c62d6816617fcbf9c" +
                                "7773b9b4f87902b7253848ba2b0ed0ba" +
                                "f70a3ac976a835b7bc3008e9ba43da74"
                ),
                new testCase(
                        "11111111111111111111111111111111",
                        "youofdas1312",
                        "cab07967cf377dbc010fbf5f84d12bcb" +
                                "6f8b188e6965738cf9007a671b4bfeb9" +
                                "f52257aac3808048c341dcaa1c125ca7"
                ),
                new testCase(
                        "11111111111111111111111111",
                        "空のBottle😄",
                        "4384874473945c5b70519ad5ace6305ef6b78c60c3c694add08a8b81899c4171"
                ),
        };
    }

    @Test
    public void EncryptDecrypt() throws Exception {
        for (int i = 0; i < cases.length; i++) {
            byte[] raw = Hex.decodeHex(cases[i].raw.toCharArray());
            byte[] password = cases[i].password.getBytes();
            byte[] enc = Hex.decodeHex(cases[i].possibleEncrypted.toCharArray());
            byte[] encrypt = EndToEndEncryption.Encrypt(raw, password);
            byte[] dec = EndToEndEncryption.Decrypt(encrypt, password);
            byte[] dec2 = EndToEndEncryption.Decrypt(enc, password);
            assertEquals(dec, raw);
            assertEquals(dec2, raw);
            System.out.printf("Test case: #%d Passed\n", i);
        }
    }
}