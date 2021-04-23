import gnu.trove.list.TIntList;
import org.scify.jedai.blockbuilding.IBlockBuilding;
import org.scify.jedai.blockbuilding.QGramsBlocking;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityNodePruning;
import org.scify.jedai.blockprocessing.comparisoncleaning.WeightedNodePruning;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.datareader.groundtruthreader.GtCSVReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.entityclustering.CenterClustering;
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

import java.util.ArrayList;
import java.util.List;


/**
 * Fixed JedAI workflow for ER
 */
public class Workflow {

    protected IBlockBuilding blockBuilding;
    protected IBlockProcessing comparisonCleaning;
    protected IBlockProcessing blockFiltering;

    protected RepresentationModel rm;
    protected SimilarityMetric sm;
    protected IEntityMatching entityMatching;

    protected IEntityClustering entityClustering;

    protected long  blockingTime;
    protected long matchingTime;

    protected List<AbstractBlock> blocks;
    protected EquivalenceCluster[] clusters;


    public Workflow(List<EntityProfile> profiles) {
        blockBuilding = new QGramsBlocking(3);
        blockFiltering = new BlockFiltering(0.5f);
        comparisonCleaning = new WeightedNodePruning(WeightingScheme.JS);

        // Entity matching - build representation model
        rm = RepresentationModel.CHARACTER_BIGRAMS;
        sm = SimilarityMetric.COSINE_SIMILARITY;
        entityMatching = new ProfileMatcher(profiles, rm, sm);

        // Entity Clustering
        entityClustering = new CenterClustering(0.5f);
    }


    public EquivalenceCluster[] apply(List<EntityProfile> profiles){

        long time1 = System.currentTimeMillis();
        blocks = blockBuilding.getBlocks(profiles);
        blocks = blockFiltering.refineBlocks(blocks);
        blocks = comparisonCleaning.refineBlocks(blocks);

        long time2 = System.currentTimeMillis();
        blockingTime = time2 - time1;

        SimilarityPairs pairs = entityMatching.executeComparisons(blocks);
        clusters = entityClustering.getDuplicates(pairs);
        long time3 = System.currentTimeMillis();
        matchingTime = time3 - time2;

        return clusters;
    }


    /**
     * Extract the detected duplicates from clusters
     * @param clusters detected clusters
     * @return a list containing the indices of the duplicates, e.g., List([1, 2], [1, 3], [3, 2])
     */
    public List<Integer[]> getDetectedDuplicates(EquivalenceCluster[] clusters) {
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


    public void evaluate(String gtPath, List<EntityProfile> profiles){
        IGroundTruthReader gtReader = new GtCSVReader(gtPath);
        final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(profiles));
        System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

        // blocking performance
        final BlocksPerformance blStats = new BlocksPerformance(blocks, duplicatePropagation);
        blStats.setStatistics();
        blStats.printStatistics(blockingTime, "", "");

        // overall performance
        final ClustersPerformance clp = new ClustersPerformance(clusters, duplicatePropagation);
        clp.setStatistics();
        clp.printStatistics(matchingTime, "", "");

        System.out.println("Running time\t:\t" + (matchingTime + blockingTime));
    }
}
