/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tokenizer.controller;

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
     * author: Elisafina method untuk melakukan tokenisasi untuk field subject
     * dan body kemudian hasilnya ditampung pada hashmap yang berisi term dan
     * jumlah kemunculan term pada dokumen tertentu.
     *
     * @param data
     * @return
     */
    public static HashMap<String, Integer> getListTerm(String data) {
        HashMap<String, Integer> termList = new HashMap<String, Integer>();

        data = Parser.removeHTMLTag(data);
        String[] ax = data.split("\\s+|, ");

        for (String s : ax) {
            //if (s.matches(".*\\w+.*")) {

            s = Parser.removeApostrope(s);
            s = Parser.removeHypenate(s);
            //email dan link tidak akan ditokenisasi lagi (dipotong berdasarkan tanda baca
            if (!s.matches(EMAIL_PATTERN) && s.indexOf("http:") == -1 && s.indexOf("www.") == -1 && !s.matches("[a-zA-Z0-9]+")) {
                s = Parser.removePuncuation(s);
                String[] slices = s.split("\\s");
                for (String slice : slices) {
                    if (slice.matches("\\w+")) {
                        putToHashMap(slice, termList);
                    }
                }
            } else {

                putToHashMap(s, termList);
            }

            //} else {
            //   System.out.println(s);
            //}

        }
        //System.out.println(termList);
        return termList;
    }

    /**
     * author: Elisafina method untuk memasukan sebuah token ke hashmap dengan
     * pengecekan, jika token tersebut sudah pernah ada maka value-nya akan
     * ditambah 1
     *
     * @param key
     * @param map
     */
    public static void putToHashMap(String key, HashMap<String, Integer> map) {

        Integer freq = (Integer) map.get(key);
        if (freq == null) {
            freq = new Integer(1);
        } else {
            int value = freq.intValue();
            freq = new Integer(value + 1);
        }
        map.put(key, freq);
    }
}
