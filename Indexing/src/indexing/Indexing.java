/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package indexing;

import com.indexing.util.FileWalker;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

/**
 *
 * @author irfannurhakim
 */
public class Indexing {

    public static long N_messagge = 0; // date, to, from, subject, body, all
    public static int top_k_token;
    public static final String codeName = "irfan_elisafina_pandapotan";
    public static long docID = -1;
    public static BufferedWriter docMapping = null;
    public static BufferedWriter invertedIndexDate = null;
    public static BufferedWriter termMappingDate = null;
    public static BufferedWriter invertedIndexFrom = null;
    public static BufferedWriter termMappingFrom = null;
    public static BufferedWriter invertedIndexTo = null;
    public static BufferedWriter termMappingTo = null;
    public static BufferedWriter invertedIndexSubject = null;
    public static BufferedWriter termMappingSubject = null;
    public static BufferedWriter invertedIndexBody = null;
    public static BufferedWriter termMappingBody = null;
    public final static String NEWLINE="\r\n";
    public static LinkedHashMap<String,Long> indexDate = new LinkedHashMap<>();
    public static TreeMap<String,String> treeIndexDate = new TreeMap<>();
    public static LinkedHashMap<String,Long> indexFrom = new LinkedHashMap<>();
    public static TreeMap<String,String> treeIndexFrom = new TreeMap<>();
    public static LinkedHashMap<String,Long> indexTo = new LinkedHashMap<>();
    public static TreeMap<String,String> treeIndexTo = new TreeMap<>();
    public static LinkedHashMap<String,Long> indexSubject = new LinkedHashMap<>();
    public static TreeMap<String,String> treeIndexSubject = new TreeMap<>();
    public static LinkedHashMap<String,Long> indexBody = new LinkedHashMap<>();
    public static TreeMap<String,String> treeIndexBody = new TreeMap<>();
    
    public static List<String> test = new ArrayList<>();
    public static Integer counterCall=0;
    public static int jumFile=0;
    
    public static String [] fileNames ={"document_mapping.txt", 
        "inverted_index_date.txt","term_mapping_date.txt",
        "inverted_index_from.txt", "term_mapping_from.txt", 
        "inverted_index_to.txt", "term_mapping_to.txt",
        "inverted_index_subject.txt", "term_mapping_subject.txt", 
        "inverted_index_body.txt", "term_mapping_body.txt"};
    /**
     * author irfannurhakim
     *
     * @param args the command line arguments
     *
     */
    @SuppressWarnings("empty-statement")
    public static void main(String[] args) throws IOException {

        
          if(args.length != 1)
          { 
              System.out.println("Usage: Indexing <document_location> "); System.exit(0);
          }
         
        

        //String root = "/Users/hadipratama/Documents/Kuliah/Search_Engine_Tech/enron_mail_20110402/maildir/beck-s";
        //String root = "/Users/hadipratama/Documents/Kuliah/Search_Engine_Tech/enron_mail_20110402/maildir/";
        //String root = "D:\\Kuliah_S2\\IF6054_Teknologi_Mesin_Pencari\\Tugas\\enron_mail_20110402\\enron_mail_20110402\\maildir\\";
          String root = args[0];
        top_k_token = 100;
        File f1;
        
        for (int i = 0; i < fileNames.length; i++) {
            String string = fileNames[i];
            f1 = new File(string);
            if(f1.exists())
            {
                f1.delete();
            }
            
        }
        docMapping =  new BufferedWriter(new FileWriter(fileNames[0]));
        invertedIndexDate = new BufferedWriter(new FileWriter(fileNames[1]));
        termMappingDate = new BufferedWriter(new FileWriter(fileNames[2]));
        invertedIndexFrom = new BufferedWriter(new FileWriter(fileNames[3]));
        termMappingFrom = new BufferedWriter(new FileWriter(fileNames[4]));
        invertedIndexTo = new BufferedWriter(new FileWriter(fileNames[5]));
        termMappingTo = new BufferedWriter(new FileWriter(fileNames[6]));
        invertedIndexSubject = new BufferedWriter(new FileWriter(fileNames[7]));
        termMappingSubject = new BufferedWriter(new FileWriter(fileNames[8]));
        invertedIndexBody = new BufferedWriter(new FileWriter(fileNames[9]));
        termMappingBody = new BufferedWriter(new FileWriter(fileNames[10]));
        FileVisitor<Path> fileVisitor = new FileWalker();
        Files.walkFileTree(Paths.get(root), fileVisitor);
        
        
        
    }
}
