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

package io.covenantsql.connector.util;

import java.nio.charset.Charset;
import java.security.*;

import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.math.BigInteger;


public class EndToEndEncryption {
    private static final SecureRandom random = new SecureRandom();
    private static final byte[] salt = new byte[]{
            (byte) 0x3f, (byte) 0xb8, (byte) 0x87, (byte) 0x7d, (byte) 0x37, (byte) 0xfd, (byte) 0xc0, (byte) 0x4e,
            (byte) 0x4a, (byte) 0x47, (byte) 0x65, (byte) 0xEF, (byte) 0xb8, (byte) 0xab, (byte) 0x7d, (byte) 0x36
    };

//    public EndToEndEncryption() {
//        Security.addProvider(new BouncyCastleProvider());
//    }

    private static byte[] generateIV() {
        byte[] ivBytes = new byte[16];
        random.nextBytes(ivBytes);
        return ivBytes;
    }

    private static byte[] kdf(byte[] rawPass) {
        MessageDigest sha = null;
        MessageDigest final_sha = null;
        try {
            sha = MessageDigest.getInstance("SHA-256");
            final_sha = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        sha.update(rawPass);
        sha.update(salt);
        final_sha.update(sha.digest());
        byte[] key = new byte[16];
        System.arraycopy(final_sha.digest(), 0, key, 0, 16);
        return key;
    }

    public static byte[] Encrypt(final byte[] raw,
                                 final byte[] key) {
        try {
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            final int blockSize = cipher.getBlockSize();

            // create the key
            final SecretKeySpec symKey = new SecretKeySpec(kdf(key), "AES");

            // generate random IV using block size (possibly create a method for
            // this)
            final byte[] ivData = new byte[blockSize];
            final SecureRandom rnd = SecureRandom.getInstance("SHA1PRNG");
            rnd.nextBytes(ivData);
            final IvParameterSpec iv = new IvParameterSpec(ivData);

            cipher.init(Cipher.ENCRYPT_MODE, symKey, iv);

            final byte[] encryptedMessage = cipher.doFinal(raw);

            // concatenate IV and encrypted message
            final byte[] ivAndEncryptedMessage = new byte[ivData.length
                    + encryptedMessage.length];
            System.arraycopy(ivData, 0, ivAndEncryptedMessage, 0, blockSize);
            System.arraycopy(encryptedMessage, 0, ivAndEncryptedMessage,
                    blockSize, encryptedMessage.length);

            return ivAndEncryptedMessage;
        } catch (InvalidKeyException e) {
            throw new IllegalArgumentException(
                    "key argument does not contain a valid AES key");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(
                    "Unexpected exception during encryption", e);
        }
    }

    public static byte[] Decrypt(final byte[] enc,
                                 final byte[] key) {

        try {
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            final int blockSize = cipher.getBlockSize();

            // create the key
            final SecretKeySpec symKey = new SecretKeySpec(kdf(key), "AES");

            // retrieve random IV from start of the received message
            final byte[] ivData = new byte[blockSize];
            System.arraycopy(enc, 0, ivData, 0, blockSize);
            final IvParameterSpec iv = new IvParameterSpec(ivData);

            // retrieve the encrypted message itself
            final byte[] encryptedMessage = new byte[enc.length
                    - blockSize];
            System.arraycopy(enc, blockSize,
                    encryptedMessage, 0, encryptedMessage.length);

            cipher.init(Cipher.DECRYPT_MODE, symKey, iv);

            final byte[] decodedMessage = cipher.doFinal(encryptedMessage);

            return decodedMessage;
        } catch (InvalidKeyException e) {
            throw new IllegalArgumentException(
                    "key argument does not contain a valid AES key");
        } catch (BadPaddingException e) {
            // you'd better know about padding oracle attacks
            return null;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(
                    "Unexpected exception during decryption", e);
        }
    }
}