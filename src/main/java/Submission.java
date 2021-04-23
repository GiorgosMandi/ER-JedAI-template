import org.apache.commons.cli.*;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datareader.entityreader.EntityCSVReader;


import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class Submission {

    public static void main(String[] args) {

        try {

            // parse command line arguments
            Options options = new Options();
            options.addOption("x", true, "path to X dataset");
            options.addOption("gt", true, "path to ground-truth file - the ground-truth file must contain only the duplicate pairs");
            options.addOption("o", true, "path to output");
            options.addOption("y", true, "path to Y dataset");
            options.addOption("cgt", false, "path to output");

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            List<String> xPaths = new ArrayList<>();
            xPaths.add("X1.csv");
            xPaths.add("X2.csv");
            xPaths.add("X3.csv");
            xPaths.add("X4.csv");
            if (cmd.hasOption("x")){
                xPaths.clear();
                xPaths.add(cmd.getOptionValue("x"));
            }

            String outputPath = "output.csv";
            if (cmd.hasOption("o")) outputPath = cmd.getOptionValue("o");

            // create ground-truth file if requested
            if (cmd.hasOption("y") && cmd.hasOption("cgt")) createGT(cmd.getOptionValue("y"));

            List<String[]> allDuplicates = new LinkedList<>();
            for (String xPath: xPaths) {

                // reading first line to find the index of the id
                BufferedReader reader = new BufferedReader(new FileReader(xPath));
                List<String> headers = Arrays.asList(reader.readLine().split(","));
                int idIndex = headers.indexOf("instance_id");

                // load CSV dataset into EntityProfiles
                EntityCSVReader csvReader = new EntityCSVReader(xPath);
                csvReader.setAttributeNamesInFirstRow(true);
                csvReader.setIdIndex(idIndex);

                List<EntityProfile> profiles = csvReader.getEntityProfiles();

                Workflow wf = new Workflow(profiles);
                EquivalenceCluster[] clusters = wf.apply(profiles);

                // get detected duplicates
                List<Integer[]> duplicates = wf.getDetectedDuplicates(clusters);
                System.out.println("Detected Duplicates: " + duplicates.size() + "\n");

                List<String[]> duplicatePairs = duplicates.stream()
                        .map(arr -> new String[] {profiles.get(arr[0]).getEntityUrl(), profiles.get(arr[1]).getEntityUrl()})
                        .collect(Collectors.toList());

                // adds detected duplicates in the overall list
                allDuplicates.addAll(duplicatePairs);


                // in case the ground-truth file is provided, we evaluate the performance of the algorithm
                if (cmd.hasOption("gt")) {
                    String gtPath = cmd.getOptionValue("gt");
                    wf.evaluate(gtPath, profiles);
                }
            }

            export(allDuplicates, outputPath);
        }
        catch (ParseException pe){
            System.err.println("Wrong input options");
            pe.printStackTrace();
        }
        catch (FileNotFoundException e){
            System.err.println("Output file not found");
            e.printStackTrace();
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Export the detected duplicates into a CSV
     * @param outputPath path of the CSV
     * @throws FileNotFoundException in case path does not exist
     */
    public static void export(List<String[]> duplicates, String outputPath) throws FileNotFoundException {
        File csvOutputFile = new File(outputPath);
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            pw.println("left_instance_id,right_instance_id");
            duplicates.stream().map(arr -> arr[0] + "," + arr[1]).forEach(pw::println);
        }
        assertTrue(csvOutputFile.exists());
    }



    public static void createGT(String yPath) throws IOException {
            String gtPath = yPath.replace("Y", "GT");
            File gtFile = new File(gtPath);
            BufferedReader reader = new BufferedReader(new FileReader(yPath));

            String line;
            try (PrintWriter pw = new PrintWriter(gtFile)) {
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split(",");
                    if (tokens[2].charAt(0) == '1') {
                        String pairs = tokens[0] + "," + tokens[1];
                        pw.println(pairs);
                    }
                }
            }
            assertTrue(gtFile.exists());
        }
}
