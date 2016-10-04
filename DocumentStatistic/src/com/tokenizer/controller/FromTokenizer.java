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
public class FromTokenizer {

    private static final String EMAIL_PATTERN =
            "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";

    /**
     * author: Elisafina
     * untuk menampung string from dari suatu file ke dalam bentuk hashmap
     * @param from
     * @return hashmap dari field from
     */
    public static HashMap<String, Integer> getListFrom(String from) {
        HashMap<String, Integer> termList = new HashMap<String, Integer>();
        
        String[] terms = from.split(",|(\\s)+|<|> ");
        
        for (int i = 0; i < terms.length; i++) {
            String key = terms[i];
            if (key.matches(EMAIL_PATTERN)) {
                Integer freq = (Integer) termList.get(key);
                if (freq == null) {
                    freq = new Integer(1);
                } else {
                    int value = freq.intValue();
                    freq = new Integer(value + 1);
                }
                termList.put(key, freq);
            }
        }
        //System.out.println(termList);
        return termList;
    }
}
