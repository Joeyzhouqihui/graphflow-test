package queryTest;

import ca.waterloo.dsg.graphflow.client.GraphflowClient;
import java.io.IOException;

public class QueryTest extends GraphflowClient{
    private static String GRPC_HOST = "localhost";
    private static int GRPC_PORT = 8080;

    private static String TestQuery = "create (1:P {name:\"zqh\")-[:like]->(2:P);";

    public QueryTest() throws IOException {
        super(GRPC_HOST, GRPC_PORT);
    }

    public void haveATry(){
        System.out.println("\nsend a test query to the server !\n");
        String result = queryServer(TestQuery);
        System.out.println("\nResult:\n" + result);
    }
}
