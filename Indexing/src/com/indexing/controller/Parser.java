/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.indexing.controller;

/**
 *
 * @author irfannurhakim
 */
public class Parser {

   

    /**
     * author: Irfan
     * @param collection
     * @param keyword
     * @return
     */
    public static int findIdx(String collection, String keyword) {
        return collection.indexOf(keyword);
    }

    /**
     * author: Elisafina
     * method untuk menghapus apostrope:
     * jika 's maka akan dihapus (misalkan mary's ==> mary)
     * jika ' saja maka hanya dihapus apostropenya saja
     * 
     * @param s
     * @return s
     */
    public static String removeApostrope(String s) {
        //s =Pattern.compile("'s").matcher(s).replaceAll("");
        //s =Pattern.compile("'").matcher(s).replaceAll("");
        return s.replaceAll("'s", "").replaceAll("'", "");
    }

    /**
     * author Elisafina
     * untuk menggabungkan kata-kata yang memiliki tanda hubung -
     * misalkan state-of-the-art menjadi stateoftheart
     * @param s
     * @return
     */
    public static String removeHypenate(String s) {
        if (s.matches(".*([a-zA-Z]+-)*[a-zA-Z]+.*")) {
            s = s.replaceAll("-", "");
        }
        return s;
    }


    /**
     * author: Irfan
     * @param s
     * @return
     */
    public static String removePuncuation(String s) {
        //return (Pattern.compile("\\p{Punct}").matcher(s).replaceAll(" "));
        return s.replaceAll("\\p{Punct}", " ");
    }
    
    /**
     * author: Elisafina
     * menghapus semua html tag beserta semua atributnya
     * @param allString
     * @return
     */
    public static String removeHTMLTag(String allString){
        return allString.replaceAll("\\<.*?\\>", "");
    }
    
}
