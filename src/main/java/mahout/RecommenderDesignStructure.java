package mahout;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.List;

public class RecommenderDesignStructure {
    static Recommender recommender;
    static DataModel model;
    static UserSimilarity similarity;
	  public static void main(String[] args) throws Exception {

        File f=new File("input/MoviesList.csv");
	    model = new FileDataModel(f);


	    similarity = new TanimotoCoefficientSimilarity(model);
	    //TanimotoCoefficientSimilarity(model);//PearsonCorrelationSimilarity(model);
	    
	    //First parameter - neighborhood size; capped at the number of users in the data model
	    UserNeighborhood neighborhood =
	      new NearestNUserNeighborhood(5, similarity, model);

	    recommender = new GenericUserBasedRecommender(
	        model, neighborhood, similarity);

        showSimilarities();
          /*
          LongPrimitiveIterator it = model.getUserIDs();
          while(it.hasNext()){
              showRecommendationForUser(it.nextLong());
          }          //while
          */
	  }
    public static void showRecommendationForUser(long user) throws TasteException {
        System.out.println( "For user "+user+"----------------------");
        List<RecommendedItem> recommendations =
                recommender.recommend(user,model.getNumItems());

        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
    }
    public static void showSimilarities() throws TasteException {
        LongPrimitiveIterator it=model.getUserIDs();
        int numUsers=model.getNumUsers();
        long[] user_ids=new long[numUsers];
        int ind=0;
        while(it.hasNext())user_ids[ind++]=it.nextLong();


            for(int i=0;i<numUsers;i++)
                for(int j=i;j<numUsers;j++)
                    System.out.printf( "(%d,%d) = %f\n" , user_ids[i] , user_ids[j] , similarity.userSimilarity(user_ids[i],user_ids[j]) );


    } //showSimilarities

}
