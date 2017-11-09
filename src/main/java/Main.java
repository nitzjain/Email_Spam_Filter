/**
 * Created by Manpreet Gandhi on 4/21/2016.
 */
import static spark.Spark.*;

public class Main {
    public static void main(String[] args) {
        get("/hello", (req, res) -> "Hello World");

    }
}