/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.message_retrieval;

/**
 *
 * @author user
 */
public class DocMappingModel {

    String messID;
    int docLength[];

    public DocMappingModel() {
    }

    public DocMappingModel(String messID, int[] docLength) {
        this.messID = messID;
        this.docLength = docLength;
    }

    public String getMessID() {
        return messID;
    }

    public void setMessID(String messID) {
        this.messID = messID;
    }

    public int[] getDocLength() {
        return docLength;
    }

    public void setDocLength(int[] docLength) {
        this.docLength = docLength;
    }

    @Override
    public String toString() {
        String x = "";
        for (int i : docLength) {
            x += String.valueOf(i);
        }
        return "MID : " + messID + " length :" + x;
    }
}
