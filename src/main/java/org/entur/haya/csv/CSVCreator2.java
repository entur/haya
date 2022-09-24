package org.entur.haya.csv;

import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import org.entur.geocoder.csv.CSVCreator;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.geocoder.model.PeliasDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.stream.Stream;

public class CSVCreator2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVCreator2.class);

    public static InputStream create(Stream<PeliasDocument> peliasDocuments) {
        LOGGER.debug("Creating CSV file for pelias documents");

        try {
            File file = new File("/Users/mansoor.sajjad/local-gcs-storage/haya/working-directory/output.csv");
            Path path = file.toPath();
            try (Writer writer = new FileWriter(path.toString())) {
                StatefulBeanToCsv<PeliasDocument> sbc = new StatefulBeanToCsvBuilder<PeliasDocument>(writer)
                        .withQuotechar('\'')
                        .withEscapechar('\\')
                        .withSeparator(';')
                        .build();

                sbc.write(peliasDocuments);
                return new FileInputStream(file);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
