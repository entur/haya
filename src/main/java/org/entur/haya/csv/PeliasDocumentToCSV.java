package org.entur.haya.csv;

import com.opencsv.CSVWriter;
import org.entur.geocoder.model.PeliasDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

import static org.entur.geocoder.model.Parents.wrapValidParentFieldsInLists;
import static org.entur.haya.csv.CSVHeader.*;

public final class PeliasDocumentToCSV {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeliasDocumentToCSV.class);

    public InputStream makeCSVFromPeliasDocument(List<PeliasDocument> peliasDocuments) {

        LOGGER.debug("Creating CSV file for " + peliasDocuments.size() + " pelias documents");

        var csvDocumentsAsStringArrays = peliasDocuments.parallelStream()
                .map(PeliasDocumentToCSV::createCSVDocument)
                .map(PeliasDocumentToCSV::createStringArray)
                .toList();

        return writeStringArraysToCSVFile(csvDocumentsAsStringArrays);
    }

    private static String[] createStringArray(HashMap<CSVHeader, CSVValue> csvDocument) {
        return Stream.of(CSVHeader.values())
                .map(header -> csvDocument.computeIfAbsent(header, h -> CSVValue("")))
                .map(CSVValue::toString)
                .toArray(String[]::new);
    }

    private static FileInputStream writeStringArraysToCSVFile(List<String[]> stringArrays) {
        LOGGER.debug("Writing CSV data to output stream");
        try {
            File file = File.createTempFile("temp", "csv");
            try (FileOutputStream fis = new FileOutputStream(file)) {
                try (var writer = new CSVWriter(new OutputStreamWriter(fis))) {
                    writer.writeNext(Stream.of(CSVHeader.values()).map(CSVHeader::columnName).toArray(String[]::new));
                    for (String[] array : stringArrays) {
                        writer.writeNext(array);
                    }
                }
            }
            return new FileInputStream(file);
        } catch (Exception exception) {
            throw new RuntimeException("Fail to create csv.", exception);
        }
    }

    private static HashMap<CSVHeader, CSVValue> createCSVDocument(PeliasDocument peliasDocument) {

        var map = new HashMap<CSVHeader, CSVValue>();
        map.put(ID, CSVValue(peliasDocument.getSourceId()));
        map.put(INDEX, CSVValue(peliasDocument.getIndex()));
        map.put(TYPE, CSVValue(peliasDocument.getLayer()));
        map.put(SOURCE, CSVValue(peliasDocument.getSource()));
        map.put(SOURCE_ID, CSVValue(peliasDocument.getSourceId()));
        map.put(LAYER, CSVValue(peliasDocument.getLayer()));
        map.put(POPULARITY, CSVValue(peliasDocument.getPopularity()));
        map.put(CATEGORY, CSVJsonValue(peliasDocument.getCategories()));
        if (peliasDocument.getParents() != null) {
            map.put(PARENT, CSVJsonValue(wrapValidParentFieldsInLists(peliasDocument.getParents().parents())));
        }

        map.put(NAME, CSVValue(peliasDocument.getDefaultName()));
        if (peliasDocument.getCenterPoint() != null) {
            map.put(LATITUDE, CSVValue(peliasDocument.getCenterPoint().lat()));
            map.put(LONGITUDE, CSVValue(peliasDocument.getCenterPoint().lon()));
        }
        if (peliasDocument.getAddressParts() != null) {
            map.put(ADDRESS_STREET, CSVValue(peliasDocument.getAddressParts().street()));
            map.put(ADDRESS_NUMBER, CSVValue(peliasDocument.getAddressParts().number()));
            map.put(ADDRESS_ZIP, CSVValue(peliasDocument.getAddressParts().zip()));
            // TODO: Test, if address name is required, Name is not supported by pelias csv-importer
        }

        return map;
    }

    private static CSVValue CSVValue(Object value) {
        return new CSVValue(value, false);
    }

    private static CSVValue CSVJsonValue(Object value) {
        return new CSVValue(value, true);
    }
}
