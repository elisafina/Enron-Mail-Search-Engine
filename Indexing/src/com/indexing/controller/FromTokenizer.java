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
public class FromTokenizer {

    private static final String EMAIL_PATTERN =
            "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";

    /**
     * author: Elisafina
     * untuk menampung term-term beserta posisi masing2 term
     * pada field from dari suatu file ke dalam bentuk hashmap
     * @param from
     * @return hashmap  term dan posisi dari field from
     */
    public static HashMap<String, String> getListFrom(String from) {
        HashMap<String, String> termList = new HashMap<String, String>();
        
        String[] terms = from.split("_|\\W");
        long pos=0;
        
        for (int i = 0; i < terms.length; i++) {
            String key = terms[i];
           if (!key.equals("")) {
                String freq = (String) termList.get(key);
                pos++;
                if (freq == null) {
                    freq = pos+",";
                } else {
                    String value = freq;
                    freq += pos+",";
                }
                termList.put(key, freq);
            }
        }
        return termList;
    }
}
