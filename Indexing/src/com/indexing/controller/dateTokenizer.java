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
public class dateTokenizer {

    /**
     * author: Elisafina
     * untuk menampung term-term beserta posisi masing2 term
     * pada field date dari suatu file ke dalam bentuk hashmap
     * @param date
     * @return hashmap  term dan posisi dari field date
     */
    public static HashMap<String, String> getListDate(String date) {
        HashMap<String, String> termList = new HashMap<String, String>();
         String[] terms = date.split("\\W");
        
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
