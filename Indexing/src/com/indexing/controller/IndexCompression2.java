/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.indexing.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 *
 * @author user
 */
public class IndexCompression2 {

    /**
     * author: Pandapotan 
     * untuk mengubah linkedList of integer menjadi
     * linkedlist yang berisi delta dari masing-masing element.
     *
     * @param input
     * @return LinkedList delta
     */
    public static LinkedList<Integer> gapEncode(LinkedList<Integer> input) {
        Collections.sort(input);
        ArrayList<Integer> inputs = new ArrayList<>(input);
        LinkedList<Integer> hasil = new LinkedList<Integer>();
        for (int i = 0; i < inputs.size(); i++) {
            if (i != 0) {
                hasil.add(inputs.get(i) - inputs.get(i - 1));
            } else {
                hasil.add(inputs.get(i));
            }
        }
        return hasil;
    }

    /**
     * author: Pandapotan 
     * untuk mengubah linkedList yang berisi deltaEncoding
     * menjadi linkedlist angka biasa.
     *
     * @param input
     * @return
     */
    public static ArrayList<Integer> gapDecode(ArrayList<Integer> input) {
        ArrayList<Integer> hasil = new ArrayList<Integer>();
        for (int i = 0; i < input.size(); i++) {
            if (i != 0) {

                hasil.add(input.get(i) + hasil.get(i - 1));
            } else {
                hasil.add(input.get(i));
            }
        }
        return hasil;
    }

    /**
     * author: Pandapotan kelas tipe bentukan Byte yang berisi 8 integer untuk
     * membantu proses kompresi dengan v-byte
     *
     */
    public static class Byte {

        int[] abyte;

        Byte() {
            abyte = new int[8];
        }

        /**
         *
         * author: Pandapotan 
         * untuk mengubah sebuah integer(desimal) ke dalam
         * bentuk Byte
         *
         * @param n
         */
        public void readInt(int n) {

            String bin = Integer.toBinaryString(n);

            // inisialisai array
            for (int i = 0; i < (8 - bin.length()); i++) {
                abyte[i] = 0;
            }
            // mengisi array
            for (int i = 0; i < bin.length(); i++) {
                abyte[i + (8 - bin.length())] = bin.charAt(i) - 48; // kode ASCII untuk 0
            }
        }

        /**
         * author: Pandapotan
         * mengganti array yg ke 0 dengan nilai 1;
         */
        public void switchFirst() {
            abyte[0] = 1;
        }

        /**
         * author: Pandapotan
         * mengembalikan Byte ke dalam bentuk 
         * angka desimal.
         * @return
         */
        public int toInt() {
            int res = 0;
            for (int i = 0; i < 8; i++) {
                res += abyte[i] * Math.pow(2, (7 - i));
            }
            return res;
        }

        public String toString() {
            String res = "";
            for (int i = 0; i < 8; i++) {
                res += abyte[i];
            }
            return res;
        }
    }

    /**
     * author: Pandapotan
     * menggubah list angka menjadi list Byte yang sudah
     * dicompress menggunakan v-byte dan delta encoding
     * @param numbers
     * @return
     */
    public static ArrayList<Byte> vbEncode(LinkedList<Integer> numbers) {

        LinkedList<Integer> numbers2 = gapEncode(numbers);
        ArrayList<Byte> code = new ArrayList<Byte>();
        try {


            while (numbers2.size() > 0) {
                int n = numbers2.poll();
                code.addAll(vbEncodeNumber(n));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return code;
    }

    /**
     * author: Pandapotan
     * menggubah suatu angka menjadi list Byte yang sudah
     * dicompress menggunakan v-byte
     * @param n
     * @return
     */
    public static LinkedList<Byte> vbEncodeNumber(int n) {
        LinkedList<Byte> bytestream = new LinkedList<Byte>();
        int num = n;
        while (true) {
            Byte b = new Byte();
            b.readInt(num % 128);
            bytestream.addFirst(b);
            if (num < 128) {
                break;
            }
            num /= 128;
        }
        Byte last = bytestream.get(bytestream.size() - 1);
        last.switchFirst();
        return bytestream;
    }

    /**
     * author: Pandapotan
     * decompress list byte menjadi list angka/decimal
     * kembali
     * @param code
     * @return
     */
    public static ArrayList<Integer> vbDecode(LinkedList<Byte> code) {
        ArrayList<Integer> numbers = new ArrayList<Integer>();
        int n = 0;
        for (int i = 0; !(code.isEmpty()); i++) {
            Byte b = code.poll(); // read leading byte
            int bi = b.toInt();  // decimal value of this byte
            if (bi < 128) {       //continuation bit is set to 0
                n = 128 * n + bi;
            } else {              // continuation bit is set to 1
                n = 128 * n + (bi - 128);
                numbers.add(n);   // number is stored
                n = 0;            // reset
            }
        }
        numbers = gapDecode(numbers);
        return numbers;
    }

    /**
     * author: Pandapotan
     * menggubah list angka/decimal menjadi 
     * String yang berisi list tersebut yang sudah dicompress
     * dengan vbyte dan delta encoding sehingga siap ditulis ke file
     * @param test
     * @return
     */
    public static String VByteToString(LinkedList<Integer> test) {
        StringBuilder hasil = new StringBuilder();
        ArrayList<Byte> code = vbEncode(test);
        for (int i = 0; i < code.size(); i++) {
            String as = Integer.toHexString(code.get(i).toInt());
            if (as.length() < 2) {
                hasil.append("0").append(as);
            } else {
                hasil.append(as);
            }
        }
        return hasil.toString();
    }

    /**
     * author: Pandapotan
     * mengembalikan String yang sudah dikompress
     * ke dalam list angka desimal pada awalnya.
     * @param sb
     * @return
     */
    public static ArrayList<Integer> StringToVByte(String sb) {
        LinkedList<Byte> abs = new LinkedList<Byte>();
        for (int i = 0; i < sb.length() / 2; i++) {
            String temp = sb.substring((i * 2), i * 2 + 2);
            Byte tempb = new Byte();
            tempb.readInt(Integer.parseInt(temp, 16));
            abs.add(tempb);
        }
        return vbDecode(abs);
    }
}
