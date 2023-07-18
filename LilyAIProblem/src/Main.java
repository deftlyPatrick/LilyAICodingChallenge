import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException, SQLException {

        String jdbcUrl = "jdbc:mysql://localhost:3306/lilyaidata";
        String username = "root";
        String password = "";

        Set<String> productTypes = new HashSet<String>();
        Set<String> genders = new HashSet<String>();

        Path currentRelativePath = Paths.get("");
        String fileProductTypes = currentRelativePath.toAbsolutePath().toString() + "\\src\\ProductTypes.csv";
        String fileGenders = currentRelativePath.toAbsolutePath().toString() + "\\src\\Genders.csv";

        String inputFile = currentRelativePath.toAbsolutePath().toString() + "\\src\\InputFile.csv";
        String outputFile = currentRelativePath.toAbsolutePath().toString() + "\\src\\OutputFile.csv";

        CSVTasks csvLoadProduct = new CSVTasks(jdbcUrl, username, password, fileProductTypes, "Product Types");
        CSVTasks csvLoadGenders = new CSVTasks(jdbcUrl, username, password, fileGenders, "Gender");

        csvLoadProduct.createProductTypesTable();
        csvLoadProduct.readCSVFileAndInsert();

        csvLoadGenders.createGenderTable();
        csvLoadGenders.readCSVFileAndInsert();

        productTypes = csvLoadProduct.checkTable("ProductTypes", productTypes);
        genders = csvLoadGenders.checkTable("Genders", genders);

        List<String> productTypesList = new ArrayList<>(productTypes);
        List<String> genderList = new ArrayList<>(genders);

        for (int i = 0; i < productTypesList.size(); i++){
            String original = productTypesList.get(i);
            String stripped = original.replace("\"", "");
            productTypesList.set(i, stripped);
        }

        for (int i = 0; i < genderList.size(); i++){
            String original = genderList.get(i);
            String stripped = original.replace("\"", "");
            genderList.set(i, stripped);
        }

        CSVTasks output = new CSVTasks(jdbcUrl, username, password, inputFile, outputFile);

        List<CSVRecord> outputCSV = output.readCSVData();

        List<String> validImages = new ArrayList<>();

        //sorts by gender
        outputCSV = output.sortCSVData(outputCSV, genderList, 3);

        //sorts by product type
        outputCSV = output.sortCSVData(outputCSV, productTypesList, 4);

        //checks if image is valid if not create a new column called status
        validImages = output.checkCSVDataImages(outputCSV, validImages, 5);

        output.createCSVFile(outputCSV, outputFile, validImages);
    }

}
