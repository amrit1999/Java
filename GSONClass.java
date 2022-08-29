import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.internal.LinkedTreeMap;
//import org.codehaus.jettison.json.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class GSONClass {

    private static final Config config = ConfigFactory.load();
    static int noOfUsers = (int) Long.parseLong(config.getString(Constants.NO_OF_USERS));
    static int noOfSegments = (int) Long.parseLong(config.getString(Constants.NO_OF_SEGMENTS));
    static int noOfThreads = (int) Long.parseLong(config.getString(Constants.NO_OF_THREADS));

    public static class MyRunnable implements Runnable {

        public MyRunnable(String user) {
            //String user = "{\"zuid\":\"0001b481-6a93-407d-b897-593dee6176bd\",\"Demographic_Gender\":{\"value\":\"Male\",\"dpid\":\"1332\"},\"Demographic_MaxAge\":{\"value\":null,\"dpid\":null},\"Demographic_MinAge\":{\"value\":null,\"dpid\":null},\"Demographics_CardType\":{\"value\":null,\"dpid\":null},\"Demographic_CustomerCategory\":{\"value\":null,\"dpid\":null},\"Demographic_Language\":{\"value\":null,\"dpid\":null},\"Demographic_Nationality\":{\"value\":null,\"dpid\":null},\"Demographics_Customer\":{\"value\":null,\"dpid\":null},\"Demographic_IncomeBucket\":{\"value\":null,\"dpid\":null},\"Demographics_Bank_Type\":{\"value\":null,\"dpid\":null},\"Demographic_Education\":{\"value\":null,\"dpid\":null},\"Demographic_JobFunction\":{\"value\":null,\"dpid\":null},\"Demographic_HomeOwnerStatus\":{\"value\":null,\"dpid\":null},\"Demographic_CreditCardUser\":{\"value\":null,\"dpid\":null},\"Demographic_DebitCardUser\":{\"value\":null,\"dpid\":null},\"Demographic_EmploymentStatus\":{\"value\":null,\"dpid\":null},\"Demographic_EnergySupplier\":{\"value\":null,\"dpid\":null},\"Demographic_Loan\":{\"value\":null,\"dpid\":null},\"Demographic_MaritalStatus\":{\"value\":null,\"dpid\":null},\"Demographic_Mortgage\":{\"value\":null,\"dpid\":null},\"Demographic_Pension\":{\"value\":null,\"dpid\":null},\"Demographic_ResidencyType\":{\"value\":null,\"dpid\":null},\"Demographic_ResidencyStatus\":{\"value\":null,\"dpid\":null},\"Demographic_Csp\":{\"value\":null,\"dpid\":null},\"Demographic_Customer\":{\"value\":null,\"dpid\":null},\"Demographic_Gender_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_MinAge_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_MaxAge_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_Gender_nielsen_otr\":{\"value\":null},\"Demographic_MaxAge_nielsen_otr\":{\"value\":null},\"Demographic_MinAge_nielsen_otr\":{\"value\":null},\"Demographic_IndustryType\":{\"value\":null,\"dpid\":null},\"Demographic_Generation\":{\"value\":null,\"dpid\":null},\"Demographic_LifeEvent\":{\"value\":null,\"dpid\":null},\"Demographics_Relationship\":{\"value\":null,\"dpid\":null},\"Demographic_Country\":{\"value\":\"ESP\",\"dpid\":\"1332\"},\"demographic_birthmonth\":{\"value\":null,\"dpid\":null},\"demographic_parentalstatus\":{\"value\":null,\"dpid\":null},\"id\":\"fc208db3-e9cb-4f16-830a-ab44e32b5c85-tuct7a56ea3\",\"type\":\"id_mid_31\",\"dpid\":\"0\",\"id_timestamp\":\"1655440674\",\"ingestion_timestamp\":\"1660372234457\",\"kayzen_os\":null,\"kayzen_date\":null,\"country_partitioned\":\"ESP\"}";
            //callFuction(user);
        }

        public void run() {
        }
    }

    public static class MultithreadingRunnable implements Runnable {
        public void run() {
            callFuction();
        }
    }




    public static void main(String args[]) throws InterruptedException, ExecutionException {


//        BigQueryFetchData bigQueryFetchData = new BigQueryFetchData();
//        JSONArray users = bigQueryFetchData.handleUploadRequest();
        System.out.println("numbers of users = "+ noOfUsers);
        System.out.println("numbers of segments = "+ noOfSegments);
        System.out.println("numbers of threads = "+ noOfThreads);
        System.out.print("Process Started at ..... ");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = new Date();
        System.out.println(dateFormat.format(date));
        ExecutorService exs = Executors.newFixedThreadPool(noOfThreads);
        List<Future<?>> todo = new ArrayList<>();
        for (int i = 0; i < noOfUsers; i++) {
            todo.add(exs.submit(new MultithreadingRunnable()));
            //System.out.println("done till -> "+ i);
        }
        for(Future<?> f : todo){
            f.get();
        }
        exs.shutdown();

        System.out.print("Process completed at .... ");
        Date date2 = new Date();
        System.out.println(dateFormat.format(date2));

    }




    public static void callFuction(){
        Gson gson = new Gson();
        //Map<String, LinkedTreeMap> map3 = gson.fromJson("{ \"segmentid\": \"Female_new\", \"query\": { \"type\": \"and\", \"value\": [ { \"operator\": \"=\", \"attribute\": \"demographic_country\", \"type\": \"predicate\", \"value\": \"'ESP'\" }, { \"operator\": \"=\", \"attribute\": \"demographic_gender\", \"type\": \"predicate\", \"value\": \"'Male'\" } ] } }", Map.class);
        String user = "{\"zuid\":\"0001b481-6a93-407d-b897-593dee6176bd\",\"Demographic_Gender\":{\"value\":\"Male\",\"dpid\":\"1332\"},\"Demographic_MaxAge\":{\"value\":null,\"dpid\":null},\"Demographic_MinAge\":{\"value\":null,\"dpid\":null},\"Demographics_CardType\":{\"value\":null,\"dpid\":null},\"Demographic_CustomerCategory\":{\"value\":null,\"dpid\":null},\"Demographic_Language\":{\"value\":null,\"dpid\":null},\"Demographic_Nationality\":{\"value\":null,\"dpid\":null},\"Demographics_Customer\":{\"value\":null,\"dpid\":null},\"Demographic_IncomeBucket\":{\"value\":null,\"dpid\":null},\"Demographics_Bank_Type\":{\"value\":null,\"dpid\":null},\"Demographic_Education\":{\"value\":null,\"dpid\":null},\"Demographic_JobFunction\":{\"value\":null,\"dpid\":null},\"Demographic_HomeOwnerStatus\":{\"value\":null,\"dpid\":null},\"Demographic_CreditCardUser\":{\"value\":null,\"dpid\":null},\"Demographic_DebitCardUser\":{\"value\":null,\"dpid\":null},\"Demographic_EmploymentStatus\":{\"value\":null,\"dpid\":null},\"Demographic_EnergySupplier\":{\"value\":null,\"dpid\":null},\"Demographic_Loan\":{\"value\":null,\"dpid\":null},\"Demographic_MaritalStatus\":{\"value\":null,\"dpid\":null},\"Demographic_Mortgage\":{\"value\":null,\"dpid\":null},\"Demographic_Pension\":{\"value\":null,\"dpid\":null},\"Demographic_ResidencyType\":{\"value\":null,\"dpid\":null},\"Demographic_ResidencyStatus\":{\"value\":null,\"dpid\":null},\"Demographic_Csp\":{\"value\":null,\"dpid\":null},\"Demographic_Customer\":{\"value\":null,\"dpid\":null},\"Demographic_Gender_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_MinAge_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_MaxAge_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_Gender_nielsen_otr\":{\"value\":null},\"Demographic_MaxAge_nielsen_otr\":{\"value\":null},\"Demographic_MinAge_nielsen_otr\":{\"value\":null},\"Demographic_IndustryType\":{\"value\":null,\"dpid\":null},\"Demographic_Generation\":{\"value\":null,\"dpid\":null},\"Demographic_LifeEvent\":{\"value\":null,\"dpid\":null},\"Demographics_Relationship\":{\"value\":null,\"dpid\":null},\"Demographic_Country\":{\"value\":\"ESP\",\"dpid\":\"1332\"},\"demographic_birthmonth\":{\"value\":null,\"dpid\":null},\"demographic_parentalstatus\":{\"value\":null,\"dpid\":null},\"id\":\"fc208db3-e9cb-4f16-830a-ab44e32b5c85-tuct7a56ea3\",\"type\":\"id_mid_31\",\"dpid\":\"0\",\"id_timestamp\":\"1655440674\",\"ingestion_timestamp\":\"1660372234457\",\"kayzen_os\":null,\"kayzen_date\":null,\"country_partitioned\":\"ESP\"}";
        Map<String, LinkedTreeMap> map4 = gson.fromJson("{ \"segmentid\": \"Female_new1\", \"query\": { \"type\":\"and\",\"value\":[{\"operator\":\"=\",\"attribute\":\"household_householdsize\",\"type\":\"predicate\",\"value\":\"3\"},{\"operator\":\"=\",\"attribute\":\"demographic_country\",\"type\":\"predicate\",\"value\":\"'ESP'\"}] } }", Map.class);

        for(int i=0;i< noOfSegments;i++){
            diffFunction(map4,user);
        }
        //String user = "{\"zuid\":\"0001b481-6a93-407d-b897-593dee6176bd\",\"Demographic_Gender\":{\"value\":\"Male\",\"dpid\":\"1332\"},\"Demographic_MaxAge\":{\"value\":null,\"dpid\":null},\"Demographic_MinAge\":{\"value\":null,\"dpid\":null},\"Demographics_CardType\":{\"value\":null,\"dpid\":null},\"Demographic_CustomerCategory\":{\"value\":null,\"dpid\":null},\"Demographic_Language\":{\"value\":null,\"dpid\":null},\"Demographic_Nationality\":{\"value\":null,\"dpid\":null},\"Demographics_Customer\":{\"value\":null,\"dpid\":null},\"Demographic_IncomeBucket\":{\"value\":null,\"dpid\":null},\"Demographics_Bank_Type\":{\"value\":null,\"dpid\":null},\"Demographic_Education\":{\"value\":null,\"dpid\":null},\"Demographic_JobFunction\":{\"value\":null,\"dpid\":null},\"Demographic_HomeOwnerStatus\":{\"value\":null,\"dpid\":null},\"Demographic_CreditCardUser\":{\"value\":null,\"dpid\":null},\"Demographic_DebitCardUser\":{\"value\":null,\"dpid\":null},\"Demographic_EmploymentStatus\":{\"value\":null,\"dpid\":null},\"Demographic_EnergySupplier\":{\"value\":null,\"dpid\":null},\"Demographic_Loan\":{\"value\":null,\"dpid\":null},\"Demographic_MaritalStatus\":{\"value\":null,\"dpid\":null},\"Demographic_Mortgage\":{\"value\":null,\"dpid\":null},\"Demographic_Pension\":{\"value\":null,\"dpid\":null},\"Demographic_ResidencyType\":{\"value\":null,\"dpid\":null},\"Demographic_ResidencyStatus\":{\"value\":null,\"dpid\":null},\"Demographic_Csp\":{\"value\":null,\"dpid\":null},\"Demographic_Customer\":{\"value\":null,\"dpid\":null},\"Demographic_Gender_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_MinAge_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_MaxAge_nielsen\":{\"value\":null,\"dpid\":null},\"Demographic_Gender_nielsen_otr\":{\"value\":null},\"Demographic_MaxAge_nielsen_otr\":{\"value\":null},\"Demographic_MinAge_nielsen_otr\":{\"value\":null},\"Demographic_IndustryType\":{\"value\":null,\"dpid\":null},\"Demographic_Generation\":{\"value\":null,\"dpid\":null},\"Demographic_LifeEvent\":{\"value\":null,\"dpid\":null},\"Demographics_Relationship\":{\"value\":null,\"dpid\":null},\"Demographic_Country\":{\"value\":\"ESP\",\"dpid\":\"1332\"},\"demographic_birthmonth\":{\"value\":null,\"dpid\":null},\"demographic_parentalstatus\":{\"value\":null,\"dpid\":null},\"id\":\"fc208db3-e9cb-4f16-830a-ab44e32b5c85-tuct7a56ea3\",\"type\":\"id_mid_31\",\"dpid\":\"0\",\"id_timestamp\":\"1655440674\",\"ingestion_timestamp\":\"1660372234457\",\"kayzen_os\":null,\"kayzen_date\":null,\"country_partitioned\":\"ESP\"}";

    }

    public static void diffFunction(Map<String, LinkedTreeMap> map4,String user){
        GSONClass gsonClass = new GSONClass();
        Map<String, String> segmentMapOutputData = gsonClass.parseSegmentData(map4);
        Map<String, String> userMapOutputData = gsonClass.parseUserData(user);

        Boolean value = gsonClass.compareSegmentUserData(userMapOutputData, segmentMapOutputData);
        //System.out.println(value);
        //System.out.println("ThreadID " +  Thread.currentThread().getId());
        //System.out.println(Thread.currentThread().getName());
    }

    public Map<String, String> parseUserData(String user){
        Gson gson = new Gson();
        Map<String, String> userData = gson.fromJson(user, Map.class);
        Map<String, String> outputUserData = new HashMap<>();
        for(Map.Entry<String, String> userDataMap : userData.entrySet()) {
            try {
                JsonObject jsonUserObject = gson.toJsonTree(userData.get(userDataMap.getKey())).getAsJsonObject();
                //System.out.println(jsonUserObject.get("value"));
                if(jsonUserObject.get("value")!=null)
                    outputUserData.put(userDataMap.getKey().toLowerCase().toString(), jsonUserObject.get("value").toString());
            }catch (IllegalStateException e){
                //System.out.println(userDataMap.getKey() + "----" + userDataMap.getValue());
            }
        }
        return outputUserData;
    }

    public Boolean compareSegmentUserData(Map<String, String> userMapOutputData,Map<String, String> segmentMapOutputData){

        Map<String, Boolean> matchedAttributes = new HashMap<>();
        List<Boolean> list = new ArrayList<>();
        //System.out.println(userMapOutputData);
        //System.out.println(segmentMapOutputData);
        boolean rowResult = true;


        if(segmentMapOutputData.get("TYPE").equals("\"and\"")){
            for(Map.Entry<String, String> mapUser : userMapOutputData.entrySet()){
                //System.out.println(mapUser.getKey());
                //System.out.println(segmentMapOutputData.get(mapUser.getKey()));
                //System.out.println(mapUser.getValue());
                if (segmentMapOutputData.get(mapUser.getKey())!=""){
                    if(mapUser.getValue().equals(segmentMapOutputData.get(mapUser.getKey()))) {
                        //System.out.println("In here");
                        matchedAttributes.put(mapUser.getKey(), true);
                        list.add(true);
                    }else{
                        matchedAttributes.put(mapUser.getKey(), false);
                        list.add(false);
                    }
                }else{
                    matchedAttributes.put(mapUser.getKey(), false);
                    list.add(false);

                }

            }

            for (Boolean el: list) {
                rowResult = el && rowResult;
            }
        }
        return rowResult;

    }

    public Map<String, String> parseSegmentData(Map<String, LinkedTreeMap> segment){
        Gson gson = new Gson();
        Map<String, String> outputSegmentData = new HashMap<>();

        for (Map.Entry<String, LinkedTreeMap> entry : segment.entrySet()){
            //System.out.println("[" + entry.getKey() + ", " + entry.getValue() + "]");
            //System.out.println(entry.getValue());
            if(entry.getKey().equals("query")) {
                JsonObject jsonObject = gson.toJsonTree(entry.getValue()).getAsJsonObject();
                //System.out.println(jsonObject.get("type"));
                outputSegmentData.put("TYPE", jsonObject.get("type").toString());

                JsonArray jsonObject1 = gson.toJsonTree(jsonObject.get("value")).getAsJsonArray();
                for(int i=0; i<jsonObject1.size(); i++) {
                    JsonObject jsonObject2 = gson.toJsonTree(jsonObject1.get(i)).getAsJsonObject();
                    //System.out.println(jsonObject2.get("operator"));
                    //System.out.println(jsonObject2.get("attribute"));
                    //outputSegmentData.put("ATTRIBUTE"+i, jsonObject2.get("attribute").toString());
                    //System.out.println(jsonObject2.get("type"));
                    //System.out.println(jsonObject2.get("value"));
                    //outputSegmentData.put("VALUE"+i, jsonObject2.get("value").toString());
                    outputSegmentData.put(jsonObject2.get("attribute").toString().replace("\"", ""), jsonObject2.get("value").toString().replace("'", ""));
                    //System.out.println("===================");

                }

            }
        }
        return outputSegmentData;
    }

}


