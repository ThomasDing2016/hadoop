package mahout;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.BooleanPreference;
import org.apache.mahout.cf.taste.impl.model.BooleanUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Demonstrates TanimotoCoefficientSimilarity + recommender.
 *
 * @author Frank Scholten
 */
public class TanimotoDemo {

    private DataModel dataModel;
    private ItemSimilarity tanimoto;

    private long CUSTOMER_A = 0;
    private long CUSTOMER_B = 1;
    private long CUSTOMER_C = 2;

    private long product[]={0,1,2,3,4};
    @Before
    public void setup() {

        BooleanUserPreferenceArray customerAPrefs = new BooleanUserPreferenceArray(4);
        customerAPrefs.set(0, new BooleanPreference(CUSTOMER_A, product[0]));
        customerAPrefs.set(1, new BooleanPreference(CUSTOMER_A, product[1]));
        customerAPrefs.set(2, new BooleanPreference(CUSTOMER_A, product[3]));
        customerAPrefs.set(3, new BooleanPreference(CUSTOMER_A, product[4]));
        customerAPrefs.set(0, new BooleanPreference(CUSTOMER_A, product[4]));

        BooleanUserPreferenceArray customerBPrefs = new BooleanUserPreferenceArray(3);
        customerBPrefs.set(0, new BooleanPreference(CUSTOMER_B, product[1]));
        customerBPrefs.set(1, new BooleanPreference(CUSTOMER_B, product[2]));
        customerBPrefs.set(2, new BooleanPreference(CUSTOMER_B, product[4]));

        BooleanUserPreferenceArray customerCPrefs = new BooleanUserPreferenceArray(2);
        customerCPrefs.set(0, new BooleanPreference(CUSTOMER_C, product[0]));
        customerCPrefs.set(1, new BooleanPreference(CUSTOMER_C, product[4]));

        FastByIDMap<PreferenceArray> userIdMap = new FastByIDMap<PreferenceArray>();
        userIdMap.put(CUSTOMER_A, customerAPrefs);
        userIdMap.put(CUSTOMER_B, customerBPrefs);
        userIdMap.put(CUSTOMER_C, customerCPrefs);


        dataModel = new GenericDataModel(userIdMap);

        tanimoto = new TanimotoCoefficientSimilarity(dataModel);
    }

    @Test
    public void testSimilarities() throws TasteException {
        assertEquals((double) 1,        tanimoto.itemSimilarity(product[0], product[0]), 0.01);
        assertEquals(Double.NaN,        tanimoto.itemSimilarity(product[0], product[1]), 0.01);
        assertEquals(Double.NaN,        tanimoto.itemSimilarity(product[0], product[2]), 0.01);
        assertEquals(Double.NaN,        tanimoto.itemSimilarity(product[0], product[3]), 0.01);
        assertEquals((double) 1 / 4,    tanimoto.itemSimilarity(product[0], product[4]), 0.01);

        assertEquals((double) 1 / 1,    tanimoto.itemSimilarity(product[1], product[1]), 0.01);
        assertEquals((double) 1 / 2,    tanimoto.itemSimilarity(product[1], product[2]), 0.01);
        assertEquals((double) 1 / 2,    tanimoto.itemSimilarity(product[1], product[3]), 0.01);
        assertEquals((double) 1 / 2,    tanimoto.itemSimilarity(product[1], product[4]), 0.01);

        assertEquals((double) 1,        tanimoto.itemSimilarity(product[2], product[2]), 0.01);
        assertEquals(Double.NaN,        tanimoto.itemSimilarity(product[2], product[3]), 0.01);
        assertEquals((double) 1 / 4,    tanimoto.itemSimilarity(product[2], product[4]), 0.01);

        assertEquals((double) 1,        tanimoto.itemSimilarity(product[3], product[3]), 0.01);
        assertEquals((double) 1 / 4,    tanimoto.itemSimilarity(product[3], product[4]), 0.01);

        assertEquals((double) 1,        tanimoto.itemSimilarity(product[4], product[4]), 0.01);
    }

    @Test
    public void testRecommendProducts() throws TasteException {
        ItemBasedRecommender recommender = new GenericItemBasedRecommender(dataModel, tanimoto);

        List<RecommendedItem> similarToproductThree = recommender.mostSimilarItems(product[2], 2);

        assertEquals(product[1], similarToproductThree.get(0).getItemID());
        assertEquals(product[4], similarToproductThree.get(1).getItemID());
    }

    @Test
    public void showSimilarities() throws TasteException {
         for(long i=0;i<5;i++)
             for(long j=i;j<5;j++)
             showSimilarty(i,j);
    }
    private void showSimilarty(long prod_id1,long prod_id2) throws TasteException {
        System.out.printf("(%d , %d) = %f\n",prod_id1,prod_id2,tanimoto.itemSimilarity(prod_id1,prod_id2));
    }
}
