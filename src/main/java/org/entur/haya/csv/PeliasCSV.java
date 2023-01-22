package org.entur.haya.csv;

import com.opencsv.CSVWriter;
import org.entur.geocoder.model.ParentFields;
import org.entur.geocoder.model.ParentType;
import org.entur.geocoder.model.PeliasDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.entur.haya.csv.CSVHeaders.*;

public final class PeliasCSV {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeliasCSV.class);
    private static final List<String> availableLanguageCodes = List.of("en", "et", "fi", "fr", "no", "ru", "se", "sv", "fk");
    private static final List<String> csvHeaders = Stream.of(
                    ID, INDEX, TYPE, NAME, ALIAS,
                    LATITUDE, LONGITUDE, ADDRESS_STREET,
                    ADDRESS_NUMBER, ADDRESS_ZIP, POPULARITY,
                    CATEGORY, DESCRIPTION, SOURCE, SOURCE_ID,
                    LAYER, PARENT/*, TARIFF_ZONE, TARIFF_ZONE_AUTHORITIES*/)
            .toList();

    private static final List<String> allHeaders = Stream.concat(
                    csvHeaders.stream(),
                    Stream.concat(
                            availableLanguageCodes.stream().map(code -> makeCsvHeaderForLanguageCode(NAME, code)),
                            availableLanguageCodes.stream().map(code -> makeCsvHeaderForLanguageCode(ALIAS, code))))
            .toList();

    public static InputStream create(Stream<PeliasDocument> peliasDocuments) {
        LOGGER.debug("Creating CSV file for pelias documents");

        try {
            File file = File.createTempFile("output", "csv");
            try (CSVWriter writer = new CSVWriter(new FileWriter(file.toPath().toString()))) {
                writer.writeNext(allHeaders.toArray(String[]::new));
                writer.writeAll(peliasDocuments
                        .filter(doc -> !doc.getParents().hasParentType(ParentType.UNKNOWN))
                        .map(PeliasCSV::createStringArray)
                        .toList());
            }
            return new FileInputStream(file);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static String[] createStringArray(PeliasDocument peliasDocument) {
        return Stream.concat(
                csvHeaders.stream().map(header -> getCSVValueForHeader(peliasDocument, header)),
                Stream.concat(
                        availableLanguageCodes.stream().map(code -> CSVValue(peliasDocument.getAlternativeNames().get(code))),
                        availableLanguageCodes.stream().map(code -> CSVJsonValue(peliasDocument.getAlternativeAlias().get(code)))
                )).map(CSVValue::toString).toArray(String[]::new);
    }

    private static CSVValue getCSVValueForHeader(PeliasDocument peliasDocument, String header) {
        CSVValue csvValue = switch (header) {
            case ID, SOURCE_ID -> CSVValue(peliasDocument.getPeliasId().id());
            case INDEX -> CSVValue(peliasDocument.getIndex());
            case TYPE, LAYER -> CSVValue(peliasDocument.getPeliasId().layer());
            case SOURCE -> CSVValue(peliasDocument.getPeliasId().source());
            case POPULARITY -> CSVValue(peliasDocument.getPopularity());
            case NAME -> CSVValue(peliasDocument.getDefaultName());
            case CATEGORY -> peliasDocument.getCategories().isEmpty() ? null : CSVJsonValue(peliasDocument.getCategories());
            case DESCRIPTION -> peliasDocument.getDescriptionMap().isEmpty() ? null : CSVJsonValue(peliasDocument.getDescriptionMap());
//            case TARIFF_ZONE -> peliasDocument.getTariffZones().isEmpty() ? null : CSVJsonValue(peliasDocument.getTariffZones());
//            case TARIFF_ZONE_AUTHORITIES -> peliasDocument.getTariffZoneAuthorities().isEmpty() ? null : CSVJsonValue(peliasDocument.getTariffZoneAuthorities());
            case ALIAS -> peliasDocument.getDefaultAlias() != null ? CSVJsonValue(List.of(peliasDocument.getDefaultAlias())) : null;
            case LATITUDE -> peliasDocument.getCenterPoint() != null ? CSVValue(peliasDocument.getCenterPoint().lat()) : null;
            case LONGITUDE -> peliasDocument.getCenterPoint() != null ? CSVValue(peliasDocument.getCenterPoint().lon()) : null;
            case PARENT -> peliasDocument.getParents() != null ? CSVJsonValue(transformParentFieldsToPeliasParent(peliasDocument.getParents().parents())) : null;
            case ADDRESS_STREET -> peliasDocument.getAddressParts() != null ? CSVValue(peliasDocument.getAddressParts().street()) : null;
            case ADDRESS_NUMBER -> peliasDocument.getAddressParts() != null ? CSVValue(peliasDocument.getAddressParts().number()) : null;
            case ADDRESS_ZIP -> peliasDocument.getAddressParts() != null ? CSVValue(peliasDocument.getAddressParts().zip()) : null;
            default -> null;
        };
        return csvValue != null ? csvValue : CSVValue("");
    }

    private static String makeCsvHeaderForLanguageCode(String prefix, String languageCode) {
        return prefix + "_" + languageCode;
    }

    /**
     * See the comments on following PR to learn why we need to wrap parent fields in lists.
     * https://github.com/pelias/csv-importer/pull/97#issuecomment-1203920795
     */
    public static Map<String, List<PeliasParent>> transformParentFieldsToPeliasParent(Map<ParentType, ParentFields> parentFields) {
        return parentFields.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().value(),
                        entry -> List.of(new PeliasParent(entry.getValue())))
                );
    }

    record PeliasParent(String source, String id, String name, String abbr) {
        public PeliasParent(ParentFields parentFields) {
            this(parentFields.peliasId().source(), parentFields.peliasId().id(), parentFields.name(), parentFields.abbr());
        }
    }

    private static CSVValue CSVValue(Object value) {
        return new CSVValue(value, false);
    }

    private static CSVValue CSVJsonValue(Object value) {
        return new CSVValue(value, true);
    }
}
