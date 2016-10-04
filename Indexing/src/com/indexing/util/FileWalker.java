
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.indexing.util;

import com.indexing.model.BigConcurentMap;
import indexing.Indexing;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author irfannurhakim
 */
public class FileWalker extends SimpleFileVisitor<Path> {

    private static long startTime;
    private int nrOfProcessors = Runtime.getRuntime().availableProcessors();
    private ExecutorService es = Executors.newFixedThreadPool(8);
    private Runtime rt = Runtime.getRuntime();
    private int i = 0;

    public FileWalker() {
        startTime = System.nanoTime();
    }

    /**
     * Visit each files in directory and after that submit a job to worker to
     * read the file.
     *
     * @param aFile
     * @param aAttrs
     * @return enumerator
     * @throws IOException
     */
    @Override
    public FileVisitResult visitFile(
            Path aFile, BasicFileAttributes aAttrs) throws IOException {
        /**
         * skip .DS_Store file Print every 1000 jobs send to worker
         */
        if (!aFile.getFileName().toString().equalsIgnoreCase(".DS_Store")) {
            if (i % 1000 == 0) {
                System.out.println("job send " + i);
            }
            i++;
            /**
             * Instantiate the worker send job to worker with parameter
             * directory file and job counter
             */
            FileReader task = new FileReader(aFile, i);
            task.setCaller(this);
            es.submit(task);
        }
        return FileVisitResult.CONTINUE;
    }

    /**
     * This method visit all directory, after end with a file then return
     * enumeration with CONTINUE value to read the file skip for all_documents
     * folder
     *
     * @param aDir
     * @param aAttrs
     * @return
     * @throws IOException
     */
    @Override
    public FileVisitResult preVisitDirectory(
            Path aDir, BasicFileAttributes aAttrs) throws IOException {
        if (aDir.endsWith("all_documents/")) {
            return FileVisitResult.SKIP_SUBTREE;
        }
        return FileVisitResult.CONTINUE;
    }

    /**
     * After a job has finished by the worker, worker will call this method to
     * acknowledge that the job has done When the total number of finished job
     * equal to total number job that has been submitted, the system will
     * terminate and give a statistical result in a text file
     *
     * @param date
     * @param from
     * @param to
     * @param subject
     * @param body
     * @param allFieldMap
     * @param jobDone
     * @throws InterruptedException
     */
    public void callback(HashMap<String, Integer> date, HashMap<String, Integer> from, HashMap<String, Integer> to, HashMap<String, Integer> subject, HashMap<String, Integer> body, HashMap<String, Integer> allFieldMap, int jobDone) throws InterruptedException {
        /**
         * Print each 1000 executed job, and run the garbage collector for
         * memory efficiency
         */
        synchronized (Indexing.counterCall) {
            Indexing.counterCall++;
            synchronized (Indexing.test) {
                if (Indexing.test.size() >= 4) {
                    System.out.println(Indexing.test);
                }
            }


            if (Indexing.counterCall % 1000 == 0) {
                System.out.println("<==> job done " + Indexing.counterCall + " from: " + i + " in : " + ((System.nanoTime() - startTime) / 1000000000.0) + " secs");

                if (Indexing.counterCall % 10000 == 0) {


                    System.out.println("masuk1");
                    Indexing.jumFile++;
                    BigConcurentMap.printPartIndex(BigConcurentMap.dateConcurentMap, "tempDate" + Indexing.jumFile + ".txt");
                    BigConcurentMap.printPartIndex(BigConcurentMap.fromConcurentMap, "tempFrom" + Indexing.jumFile + ".txt");
                    BigConcurentMap.printPartIndex(BigConcurentMap.toConcurentMap, "tempTo" + Indexing.jumFile + ".txt");
                    BigConcurentMap.printPartIndex(BigConcurentMap.subjectConcurentMap, "tempSubject" + Indexing.jumFile + ".txt");
                    BigConcurentMap.printPartIndex(BigConcurentMap.bodyConcurentMap, "tempBody" + Indexing.jumFile + ".txt");
                    rt.gc();
                    rt.gc();
                }
            }



            /**
             * Terminate job while all job has finished
             */
            if (Indexing.counterCall >= i) {
                es.shutdown();
                es.awaitTermination((long) 100, TimeUnit.MILLISECONDS);
                try {
                    
                    Indexing.docMapping.close();
                } catch (IOException ex) {
                    Logger.getLogger(FileWalker.class.getName()).log(Level.SEVERE, null, ex);
                }
                Indexing.N_messagge = i;
                try {
                    if (Indexing.counterCall % 10000 != 0) {
                        System.out.println("masuk2");
                        Indexing.jumFile++;
                        BigConcurentMap.printPartIndex(BigConcurentMap.dateConcurentMap, "tempDate" + Indexing.jumFile + ".txt");
                        BigConcurentMap.printPartIndex(BigConcurentMap.fromConcurentMap, "tempFrom" + Indexing.jumFile + ".txt");
                        BigConcurentMap.printPartIndex(BigConcurentMap.toConcurentMap, "tempTo" + Indexing.jumFile + ".txt");
                        BigConcurentMap.printPartIndex(BigConcurentMap.subjectConcurentMap, "tempSubject" + Indexing.jumFile + ".txt");
                        BigConcurentMap.printPartIndex(BigConcurentMap.bodyConcurentMap, "tempBody" + Indexing.jumFile + ".txt");
                        rt.gc();
                    }
                    
                    BigConcurentMap.mergeInvertedIndex(Indexing.treeIndexDate, "tempDate", Indexing.invertedIndexDate, Indexing.termMappingDate);
                    BigConcurentMap.mergeInvertedIndex(Indexing.treeIndexFrom, "tempFrom", Indexing.invertedIndexFrom, Indexing.termMappingFrom);
                    BigConcurentMap.mergeInvertedIndex(Indexing.treeIndexTo, "tempTo", Indexing.invertedIndexTo, Indexing.termMappingTo);
                    BigConcurentMap.mergeInvertedIndex(Indexing.treeIndexSubject, "tempSubject", Indexing.invertedIndexSubject, Indexing.termMappingSubject);
                    BigConcurentMap.mergeInvertedIndex(Indexing.treeIndexBody, "tempBody", Indexing.invertedIndexBody, Indexing.termMappingBody);
                   


                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Total execution time : " + ((System.nanoTime() - startTime) / 1000000000.0) + " secs");
                System.exit(0);
            }
        }

    }
    
}
