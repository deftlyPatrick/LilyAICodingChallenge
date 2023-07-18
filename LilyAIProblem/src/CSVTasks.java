//import com.opencsv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSVTasks {

    public Set<String> productTypes ;
    public Set<String> genders;
    public String fileType;
    public String jdbcUrl, username, password, csvFile;
    public BufferedReader br;
    public Connection conn;

    public CSVTasks(String jdbcUrl, String username, String password, String csvFile, String inputFile, String outputFile) throws FileNotFoundException, SQLException {
        Set<String> productTypes = new HashSet<String>();
        Set<String> genders = new HashSet<String>();

        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;

        this.productTypes = productTypes;
        this.genders = genders;

        this.csvFile = csvFile;
        this.fileType = fileType;

        this.br = new BufferedReader(new FileReader(csvFile));

        this.conn = initializeConnection();
    }

    public CSVTasks(String jdbcUrl, String username, String password, String csvFile, String fileType) throws SQLException, FileNotFoundException {

        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;

        this.csvFile = csvFile;
        this.fileType = fileType;

        this.br = new BufferedReader(new FileReader(csvFile));

        this.conn = initializeConnection();
    }

    public void main(String[] args) throws FileNotFoundException, SQLException {

    }

    public Connection initializeConnection() throws SQLException {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC driver not found.");
            e.printStackTrace();
        }

        return DriverManager.getConnection(this.jdbcUrl, this.username, this.password);
    }

    public void createCSVFile(List<CSVRecord> data, String output, List<String> validImages) {
        File file = new File(output);

        if (file.exists()) {
            if (file.delete()) {
                System.out.println("Existing file deleted successfully.");
            } else {
                System.out.println("Failed to delete the existing file.");
            }
        }

        try (Writer writer = new FileWriter(output);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(getHeaders(data, "status")))) {

            Iterator<String> validImagesIterator = validImages.iterator();

            for (CSVRecord record : data) {
                List<String> combinedValues = new ArrayList<>();

                for (String field : record) {
                    combinedValues.add(field);
                }

                if (validImagesIterator.hasNext()) {
                    combinedValues.add(validImagesIterator.next());
                } else {
                    combinedValues.add("");
                }

                csvPrinter.printRecord(combinedValues);
            }

            System.out.println("CSV file created successfully.");
            csvPrinter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] getHeaders(List<CSVRecord> data, String additionalHeader) {
        if (data.size() > 0) {
            CSVRecord firstRecord = data.get(0);
            List<String> headers = new ArrayList<>(firstRecord.toMap().keySet());
            headers.add(additionalHeader);
            headers.add(0, "styles");
            headers.add(1, "title");
            headers.add(2, "description");
            headers.add(3, "gender");
            headers.add(4, "product_type");
            headers.add(5, "images");
            return headers.toArray(new String[0]);
        }
        return new String[0];
    }

    public void readCSVFileAndInsert() throws SQLException, IOException {
        String line;

        int count = 0;

        while ((line = this.br.readLine()) != null) {
            String[] fields = line.split(",");

            if (count > 0) {
                if (fields.length == 3) {
                    System.out.println(this.fileType + ": " + line);

                    int id = Integer.parseInt(fields[0]);
                    String displayName = fields[1];
                    String verticalDisplayName = fields[2];

                    if (!checkIdExists(id, "ProductTypes")){
                        insertIntoProductTypes(id, displayName, verticalDisplayName);
                        this.productTypes.add(displayName);
                    } else{
                        System.out.println("Value exists in the table" + "\n");
                    }
                }
                else if(fields.length == 2){
                    System.out.println(this.fileType + ": " + line);
                    int id = Integer.parseInt(fields[0]);
                    String displayName = fields[1];

                    if (!checkIdExists(id, "Genders")){
                        insertIntoGender(id, displayName);
                        this.genders.add(displayName);
                    } else{
                        System.out.println("Value exists in the table" + "\n");
                    }
                }
            }
            count += 1;
        }
    }

    public void createProductTypesTable() throws SQLException {
        String createTableQuery = "CREATE TABLE IF NOT EXISTS ProductTypes (" +
                "id INT PRIMARY KEY, " +
                "display_name VARCHAR(255), " +
                "vertical_display_name VARCHAR(255)" +
                ")";
        try (PreparedStatement statement = this.conn.prepareStatement(createTableQuery)) {
            statement.executeUpdate();
        }
    }

    public void createGenderTable() throws SQLException {
        String createTableQuery = "CREATE TABLE IF NOT EXISTS Genders (" +
                "id INT PRIMARY KEY, " +
                "display_name VARCHAR(255)) ";
        try (PreparedStatement statement = conn.prepareStatement(createTableQuery)) {
            statement.executeUpdate();
        }
    }

    public void insertIntoProductTypes(int id, String displayName, String verticalDisplayName) throws SQLException {
        String insertQuery = "INSERT INTO ProductTypes (id, display_name, vertical_display_name) VALUES (?, ?, ?)";
        try (PreparedStatement statement = this.conn.prepareStatement(insertQuery)) {
            statement.setInt(1, id);
            statement.setString(2, displayName);
            statement.setString(3, verticalDisplayName);
            statement.executeUpdate();
        }
    }

    public void insertIntoGender(int id, String displayName) throws SQLException {
        String insertQuery = "INSERT INTO Genders (id, display_name) VALUES (?, ?)";
        try (PreparedStatement statement = this.conn.prepareStatement(insertQuery)) {
            statement.setInt(1, id);
            statement.setString(2, displayName);
            statement.executeUpdate();
        }
    }

    public Set<String> checkTable(String tableName, Set<String> data) throws SQLException {
        System.out.println("\nChecking table: " + tableName);
        String query = "SELECT id, display_name FROM " + tableName;
        Statement statement = this.conn.createStatement();

        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next()) {
            int id = resultSet.getInt("id");

            String displayName = resultSet.getString("display_name");

            System.out.println("id: " + id + ", display_name: " + displayName);

            data.add(displayName);
        }

        return data;
    }

    public List<CSVRecord> readCSVData() throws IOException {
        Reader reader = new FileReader(this.csvFile);

        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);

        List<CSVRecord> records = csvParser.getRecords();

        csvParser.close();

        return records;
    }

    public List<CSVRecord> sortCSVData(List<CSVRecord> data, List<String> sortFormat, int formatValue){

        List<CSVRecord> filteredData = new ArrayList<>();

        boolean firstValue = false;

        for (CSVRecord dataValue: data){
            String value = dataValue.get(formatValue);

            if (sortFormat.contains(value) || !firstValue){
                filteredData.add(dataValue);
                firstValue = true;
            }
        }

        filteredData.sort(Comparator.comparing(record -> getIndexInDesiredOrder(record.get(formatValue), sortFormat)));

        return filteredData;
    }

    public List<String> checkCSVDataImages(List<CSVRecord> data, List<String> validImages, int formatValue){

        for (CSVRecord dataValue: data){
            String value = dataValue.get(formatValue);

            Pattern pattern = Pattern.compile("https?://[a-zA-Z0-9./_-]+");
            Matcher matcher = pattern.matcher(value);

            boolean imageExists = true;

            while (matcher.find()){
                String url = matcher.group();
                boolean currentImageExists = checkImageExists(url);

                if (!currentImageExists){
                    imageExists = false;
                    break;
                }

                System.out.println("\nCurrent url: " + url);
            }

            validImages.add(Boolean.toString(imageExists));
        }
        return validImages;
    }

    public int getIndexInDesiredOrder(String value, List<String> desiredOrder) {
        int index = desiredOrder.indexOf(value);
        if (index != -1) {
            return index;
        } else {
            return desiredOrder.size();
        }
    }

    public void deleteId(String tableName, int idToDelete) throws SQLException {
        System.out.println("\nChecking table: " + tableName);

        String query = "DELETE FROM " +  tableName + " WHERE id = ?";
        PreparedStatement statement = this.conn.prepareStatement(query);
        statement.setInt(1, idToDelete);

        int rowsAffected = statement.executeUpdate();

        if (rowsAffected > 0) {
            System.out.println("Record deleted successfully.");
        } else {
            System.out.println("No record found with the specified ID.");
        }
    }

    public boolean checkIdExists(int idToCheck, String table) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + table + " WHERE id = ?";

        try (PreparedStatement statement = this.conn.prepareStatement(query)) {
            statement.setInt(1, idToCheck);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                int count = resultSet.getInt(1);
                return count > 0;
            }
        }
    }

    public boolean checkImageExists(String imageLink){
        try {
            URL url = new URL(imageLink);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                System.out.println("Image link is valid.");
                return true;
            } else {
                System.out.println("Image link is not valid. Response code: " + responseCode);
                return false;
            }
        } catch (IOException e) {
            System.out.println("Error occurred while checking image link validity: " + e.getMessage());
            return false;
        }
    }

}


