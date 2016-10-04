/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.indexing.controller;

import java.util.HashMap;

/**
 *
 * @author user
 */
public class subject_bodyTokenizer {

    private static final String EMAIL_PATTERN =
            "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";

    /**
     * author: Elisafina 
     * method untuk melakukan tokenisasi untuk field subject
     * dan body kemudian hasilnya ditampung pada hashmap yang berisi term dan
     * posisi-posisi kemunculan term pada dokumen tertentu.
     *
     * @param data
     * @return hashmap  term dan posisi dari field subject atau body
     */
    public static HashMap<String, String> getListTerm(String data) {
        HashMap<String, String> termList = new HashMap<String, String>();
        //System.out.println("bbb");
        data = Parser.removeHTMLTag(data);
        //System.out.println("aaaa");
        String[] ax = data.split("\\s+|, ");
        long pos = 0;
        for (String s : ax) {

            if (s.length() <= 50) {

                s = Parser.removeApostrope(s);
                s = Parser.removeHypenate(s);
                s = Parser.removePuncuation(s);

                if (!s.equals("")) {
                    if (!s.matches("[a-zA-Z0-9]+")) {

                        String[] slices = s.split("\\s");
                        for (String slice : slices) {
                            if (slice.matches("[a-zA-Z0-9]+") && slice.length() <= 30) {
                                pos++;
                                putToHashMap(slice, termList, pos);
                            }
                        }
                    } else {

                        if (s.length() <= 30) {
                            pos++;
                            putToHashMap(s, termList, pos);
                        }

                    }
                }
            }
        }
        return termList;
    }

    /**
     * author: Elisafina 
     * method untuk memasukan sebuah term dan posisinya ke hashmap dengan
     * pengecekan, jika term tersebut sudah pernah ada maka value-nya akan
     * ditambah posisi yang baru dengan menggunakan pemisah ","
     *
     * @param key
     * @param map
     */
    public static void putToHashMap(String key, HashMap<String, String> map, long pos) {

        String freq = map.get(key);
        if (freq == null) {
            freq = pos + ",";
        } else {
            String value = freq;
            freq += pos + ",";
        }
        map.put(key, freq);


    }
}
