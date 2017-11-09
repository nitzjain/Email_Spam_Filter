import java.util.Arrays;

/**
 * Created by Manpreet Gandhi on 5/9/2016.
 */
public class test {

    public static void main(String[] args) {

        String test1 = "My name is Manpreet Gandhi";

        String[] test2 = {"i", " am ", " studying ", " in ", "San jose.", " 6 ", " 7 "};
        System.out.println("123 " + test2.length);

        String[]  features = new String[test2.length -2];

        System.out.println("output" + Arrays.asList(test1.split(" ")));
        System.out.println(Arrays.asList(test2));

        System.out.println(test1.substring(1+test1.indexOf("n")));

        for (int i = 2; i < features.length; i++) {
//                    if(i == 1){
//                        continue;
//                    }
//                    else {
            features[i-2] = test2[i];
//                        j++;
//                    }
        }

        System.out.println("features printing " + Arrays.asList(features));

    }
}
