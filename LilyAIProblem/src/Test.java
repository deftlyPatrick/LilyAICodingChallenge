import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class Test {

    public static void main(String[] args) {
        String imageUrl = "https://is4.revolveassets.com/images/p4/n/z/HATR-WA155_V1.jpg"; // Replace with your image URL

        try {
            URL url = new URL(imageUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                System.out.println("Image link is valid.");
            } else {
                System.out.println("Image link is not valid. Response code: " + responseCode);
            }
        } catch (IOException e) {
            System.out.println("Error occurred while checking image link validity: " + e.getMessage());
        }
    }

}


