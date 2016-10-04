/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.message_retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author user
 */
public class MainQuery {

    public static HashMap<String, DocMappingModel> docMapping = new HashMap<>();
    public static ArrayList<HashMap<String, String>> termMapping = new ArrayList<>();
    public static HashMap<String, Integer> terms = new HashMap<>();
    public static double[] avgDocLength = new double[6];
    public static String path = ".", pathQueryFile = ".";
    //public static HashMap<String, HashMap<String, Integer>> allPostList = new HashMap<>();
    //public static HashSet<String> allDocID = new HashSet<>();
    public static final String codeName = "irfan_elisafina_pandapotan";
    public final static String NEWLINE = "\r\n";
    public static long start = 0;
    public static long end = 0;
    public static String com = "";
    public static boolean isCompress = false;

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("Usage: < path_to_index_folder > < path_to_query_file > < -u | -c > \n");
            System.out.println("Choose index file : ");
            System.out.println("    -u      uncompressed index\n");
            System.out.println("    -c      compressed index\n");
            System.exit(0);
        }

        path = args[0];
        pathQueryFile = args[1];

        if (args[2].equalsIgnoreCase("-c")) {
            com = "com_";
            isCompress = true;
        }
        
        docMapping = QueryController.getDocMapping();
        termMapping = QueryController.getTermMapping();

        HashMap<String, String> queryList = new HashMap<>();

        try {
            queryList = QueryController.dumpTermMapping(pathQueryFile);
        } catch (IOException ex) {
            Logger.getLogger(MainQuery.class.getName()).log(Level.SEVERE, null, ex);
        }

        //query = "\"meeting tomorrow\" urgent";

        for (Map.Entry<String, String> entry : queryList.entrySet()) {
            terms.clear();

            String qn = entry.getKey();
            String query = entry.getValue();
            String fileName = codeName + "-" + qn + ".txt";

            start = System.currentTimeMillis();
            query = Parser.parseQuery(query);
            terms = QueryController.queryNormalization(query);
            System.out.println("Processing query : " + terms);
            LinkedHashMap<String, Double> weight = QueryController.getWeight(terms, docMapping, avgDocLength);
            try {
                QueryController.printWeight(query, weight, com + fileName);
            } catch (IOException ex) {
                Logger.getLogger(MainQuery.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
