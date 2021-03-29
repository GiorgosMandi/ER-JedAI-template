import gnu.trove.list.TIntList;
import org.apache.commons.cli.*;
import org.scify.jedai.blockbuilding.*;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityNodePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.datareader.entityreader.EntityCSVReader;
import org.scify.jedai.datareader.groundtruthreader.GtCSVReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.entityclustering.ConnectedComponentsClustering;
import org.scify.jedai.entityclustering.IEntityClustering;
import org.scify.jedai.entitymatching.IEntityMatching;
import org.scify.jedai.entitymatching.ProfileMatcher;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.ClustersPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;
import org.scify.jedai.utilities.enumerations.WeightingScheme;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class Exp1 {

    public static void main(String[] args) {

        try {

            // parse command line arguments
            Options options = new Options();
            options.addOption("x", true, "path to X dataset");
            options.addOption("gt", true, "path to ground-truth file - the ground-truth file must contain only the duplicate pairs");
            options.addOption("o", true, "path to output");

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            List<String> xPaths = new ArrayList<>();
            xPaths.add("X1.csv");
            xPaths.add("X2.csv");
            if (cmd.hasOption("x")){
                xPaths.clear();
                xPaths.add( cmd.getOptionValue("x"));
            }

            String outputPath = "output.csv";
            if (cmd.hasOption("o")) outputPath = cmd.getOptionValue("o");

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

                // Design Workflow

                // Block Building and Cleaning techniques
                IBlockBuilding blockBuilding = new StandardBlocking();
                IBlockProcessing comparisonPurging = new ComparisonsBasedBlockPurging(false);
                IBlockProcessing blockFiltering = new BlockFiltering();
                IBlockProcessing cnp = new CardinalityNodePruning(WeightingScheme.JS);

                // Entity matching - build representation model
                RepresentationModel rm = RepresentationModel.CHARACTER_FOURGRAMS_TF_IDF;
                SimilarityMetric sm = SimilarityMetric.SIGMA_SIMILARITY;
                IEntityMatching entityMatching = new ProfileMatcher(profiles, rm, sm);

                // Entity Clustering
                IEntityClustering entityClustering = new ConnectedComponentsClustering(0.8f);

                // Execute Workflow
                long time1 = System.currentTimeMillis();
                List<AbstractBlock> blocks = blockBuilding.getBlocks(profiles);
                blocks = comparisonPurging.refineBlocks(blocks);
                blocks = blockFiltering.refineBlocks(blocks);
                blocks = cnp.refineBlocks(blocks);

                long time2 = System.currentTimeMillis();

                SimilarityPairs pairs = entityMatching.executeComparisons(blocks);
                EquivalenceCluster[] clusters = entityClustering.getDuplicates(pairs);
                long time3 = System.currentTimeMillis();

                // get and store detected duplicates
                List<Integer[]> duplicates = getDetectedDuplicates(clusters);
                System.out.println("Detected Duplicates: " + duplicates.size() + "\n");

                // adds detected duplicates in the overall list
                allDuplicates.addAll(duplicates.stream().map(arr -> new String[] {profiles.get(arr[0]).getEntityUrl(), profiles.get(arr[1]).getEntityUrl()} ).collect(Collectors.toList()));


                // in case the ground-truth file is provided, we evaluate the performance of the algorithm
                if (cmd.hasOption("gt")) {
                    String gtPath = cmd.getOptionValue("gt");
                    IGroundTruthReader gtReader = new GtCSVReader(gtPath);
                    final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(profiles));
                    System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

                    // blocking performance
                    final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
                    blStats.setStatistics();
                    blStats.printStatistics(time2 - time1, "", "");

                    // overall performance
                    final ClustersPerformance clp = new ClustersPerformance(clusters, duplicatePropagation);
                    clp.setStatistics();
                    clp.printStatistics(time3 - time1, "", "");

                    System.out.println("Running time\t:\t" + (time3 - time1));
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
     * Extract the detected duplicates from clusters
     * @param clusters detected clusters
     * @return a list containing the indices of the duplicates, e.g., List([1, 2], [1, 3], [3, 2])
     */
    public static List<Integer[]> getDetectedDuplicates(EquivalenceCluster[] clusters) {
        List<Integer[]> duplicates = new ArrayList<>();

        for (EquivalenceCluster ec : clusters) {
            if (!ec.getEntityIdsD1().isEmpty()) {
                TIntList clusterEntities = ec.getEntityIdsD1();
                if (clusterEntities.size() > 1)
                    for (int i = 0; i < clusterEntities.size(); i++)
                        for (int j=i+1; j <clusterEntities.size(); j++)
                            duplicates.add(new Integer[]{clusterEntities.get(i), clusterEntities.get(j)});
            }
        }
        return duplicates;
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
}
