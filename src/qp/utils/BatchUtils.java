/**
 * Helper Class to manage Batches and their respective I/Os 
 */

package qp.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BatchUtils {
    
    /**
     * Given an input stream, reads the next page of this input stream, 
     * which represents the sorted run given.
     * @param inputStream the stream representing the run of tuples. 
     */
    public static Batch readBatch(ObjectInputStream inputStream) {
        try {
            Object batch = inputStream.readObject();
            if (batch instanceof Batch) {
                return (Batch) batch;
            }
            return null;
        } catch (Exception exception) {
            return null;
        }
    }

    /**
     * Generates the input streams which will help to generate the sorted runs for viewing.
     * @param runsToMerge the runs that will be merged into a larger medged sorted run. 
     */
    public static List<ObjectInputStream> createInputStreams(List<File> runsToMerge) {
        List<ObjectInputStream> inputStreams = new ArrayList<>();

        for (File sortedRun : runsToMerge) {
            try {
                FileInputStream fileInputStream = new FileInputStream(sortedRun);
                ObjectInputStream inputStream = new ObjectInputStream(fileInputStream);
                inputStreams.add(inputStream);
            } catch (IOException exception) {
                System.out.println(exception);
                return null;
            }
        }

        return inputStreams;
    }

    /**
     * Appends a list of runs runs to a given file. 
     * @param runs the list of runs given. 
     * @param file the file the runs are appended to. 
     */
    public static File appendRuns(List<Batch> runs, File file) {
        if (file == null) {
            return null;
        }

        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            ObjectOutputStream outputStream = new ObjectOutputStream(fileOutputStream);
            for (Batch run : runs) {
                outputStream.writeObject(run);
            }
            outputStream.close();

            return file;
        } catch (IOException exception) {
            return null;
        }
    }

    /**
     * Writes a list of runs into a new file. 
     * @param runs the list of runs given. 
     */
    public static File writeRuns(List<Batch> runs, String filename) {
        try {
            File runsFile = new File(filename);
            FileOutputStream fileOutputStream = new FileOutputStream(runsFile);
            ObjectOutputStream outputStream = new ObjectOutputStream(fileOutputStream);
            for (Batch run : runs) {
                outputStream.writeObject(run);
            }
            outputStream.close();

            return runsFile;
        } catch (IOException exception) {
            return null;
        }
    }
}
