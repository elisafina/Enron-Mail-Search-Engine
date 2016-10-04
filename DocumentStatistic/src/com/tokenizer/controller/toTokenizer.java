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
public class toTokenizer {

    private static final String EMAIL_PATTERN =
            "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";

    /**
     * author:Elisafina adalah untuk melakukan tokenisasi pada field to dengan
     * cara memotong berdasarkan ", " atau "<>" sehingga didapatkan hashmap yang
     * berisi alamat email tujuan dan nama kontak email tersebut(jika ada)
     *
     * @param to
     * @return
     */
    public static HashMap<String, Integer> getListTo(String to) {
        HashMap<String, Integer> termList = new HashMap<String, Integer>();
        String[] terms = to.split(",|(\\s)+|>|< ");
        for (int i = 0; i < terms.length; i++) {
            String key = terms[i];
            if ( !key.matches("")) {
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
