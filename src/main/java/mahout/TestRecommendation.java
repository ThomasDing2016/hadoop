package mahout;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.PlusAnonymousConcurrentUserDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.*;

public class TestRecommendation {
    static DataModel model;
    static PlusAnonymousConcurrentUserDataModel plusModel;
    static Recommender recommender;
    static UserSimilarity similarity;
    public static void main(String[] args) throws Exception {

        File f=new File("input/MoviesList.csv");
        model = new FileDataModel(f);


        plusModel = new PlusAnonymousConcurrentUserDataModel(model, 1); //1 = num of concurrent users
        similarity = new TanimotoCoefficientSimilarity(plusModel);
        //TanimotoCoefficientSimilarity(model);//PearsonCorrelationSimilarity(model);

        //First parameter - neighborhood size; capped at the number of users in the data model
        UserNeighborhood neighborhood =
                new NearestNUserNeighborhood(5, similarity, plusModel);


        recommender=new GenericUserBasedRecommender(plusModel,neighborhood,similarity);


        // Take the next available anonymous user from the pool
        Long anonymousUserID = plusModel.takeAvailableUser();

        PreferenceArray tempPrefs = new GenericUserPreferenceArray(2);
        tempPrefs.setUserID(0, anonymousUserID);
        tempPrefs.setItemID(0, 104);
        tempPrefs.setUserID(1, anonymousUserID);
        tempPrefs.setItemID(1, 103);


        plusModel.setTempPrefs(tempPrefs, anonymousUserID);


        //showDataModel(plusModel);
        RecommendationHelper.showDataModel(plusModel);
        RecommendationHelper.showDataModel(plusModel, anonymousUserID);
        // Produce recommendations
        RecommendationHelper.showRecommendationForUser(recommender, plusModel, anonymousUserID);

        RecommendationHelper.showUserSimilarity(similarity,anonymousUserID,plusModel.getUserIDs());
        //RecommendationHelper.showUserSimilarity(similarity, anonymousUserID, 2);

        // It is very IMPORTANT to release user back to the pool
        plusModel.releaseUser(anonymousUserID);


    }

}

////////////////////////////////////////////////////////////////////////////////////
class RecommendationHelper {
    public static void showRecommendationForUser(Recommender rec,DataModel model, long user) throws TasteException {

        System.out.println( "For user "+user+"----------------------");
        List<RecommendedItem> recommendations =
                rec.recommend(user,model.getNumItems());

        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
    }
    public static void showRecommendationForUsers(Recommender rec, DataModel model) throws TasteException {
        LongPrimitiveIterator it=model.getUserIDs();
        while(it.hasNext()){
            showRecommendationForUser(rec, model, it.nextLong());
        }


    }
    public static void showDataModel(DataModel model) throws TasteException {
        LongPrimitiveIterator it = model.getUserIDs();
        while(it.hasNext()){
            long user = it.nextLong();
            showDataModel(model,user);
        }
    }

    public static void showDataModel(DataModel model, long user) throws TasteException {
        PreferenceArray p=model.getPreferencesFromUser(user);
        for(int i=0;i<p.length();i++)
            System.out.printf("%d , %d , %f \n",user,p.getItemID(i),p.getValue(i));

    }

    public static void showUserSimilarity(UserSimilarity similarity,long u1,LongPrimitiveIterator users) throws TasteException {

        List<cpUserSimilarity> listPairedUsers = new ArrayList<cpUserSimilarity>();
        while(users.hasNext()){
            long u2=users.nextLong();
            if(u1!=u2) listPairedUsers.add(new cpUserSimilarity(u1, u2, similarity.userSimilarity(u1, u2)));
        }

        cpUserSimilarity.getComparator com = new cpUserSimilarity.getComparator();
        Collections.sort(listPairedUsers, com);

        ListIterator<cpUserSimilarity> it = listPairedUsers.listIterator();
        while(it.hasNext()){
            System.out.println(it.next());
        }
    } //showUserSimilarities


    public static void showUserSimilarity(UserSimilarity similarity, long u1,long u2) throws TasteException {
        cpUserSimilarity PairedUsers = new cpUserSimilarity(u1,u2,similarity.userSimilarity(u1,u2));
        System.out.println(PairedUsers);
    }

}

class cpUserSimilarity{
    long thisUser;
    long thatUser;
    double similarity;
    public cpUserSimilarity(long thisUser,long thatUser, double similarity){
        this.thisUser=thisUser;
        this.thatUser=thatUser;
        this.similarity=similarity;
    }
    public static class getComparator implements Comparator<cpUserSimilarity> {
        @Override
        public int compare(cpUserSimilarity u12, cpUserSimilarity u13) {
            double sim12=u12.similarity;
            double sim13=u13.similarity;
            if(sim12>sim13)
                return -1;
            else if(sim12<sim13)
                return +1;
            else
                return 0;
        }
    }

    @Override
    public String toString() {return "( " +thisUser+" , " + thatUser +" ) = " + similarity;}


}