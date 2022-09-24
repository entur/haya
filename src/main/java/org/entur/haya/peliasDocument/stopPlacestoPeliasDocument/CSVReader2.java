package org.entur.haya.peliasDocument.stopPlacestoPeliasDocument;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import org.entur.geocoder.csv.CSVCreator;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.geocoder.model.PeliasDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class CSVReader2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVCreator.class);

    public static Stream<PeliasDocument> read(Path csvFilePath) {

        LOGGER.debug("Reading pelias documents from " + csvFilePath);

        try {
            Reader reader = Files.newBufferedReader(csvFilePath);
            CsvToBean<PeliasDocument> cb = new CsvToBeanBuilder<PeliasDocument>(reader)
                    .withType(PeliasDocument.class)
                    .withSeparator(';')
                    .withEscapeChar('\\')
                    .withQuoteChar('\'')
                    .build();
            //            List<PeliasDocument> parse = cb.parse();
            //            return (PeliasDocumentList) parse;
            return cb.stream();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
