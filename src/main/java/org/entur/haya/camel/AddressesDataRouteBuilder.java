package org.entur.haya.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.processor.aggregate.GroupedBodyAggregationStrategy;
import org.apache.camel.processor.aggregate.UseLatestAggregationStrategy;
import org.entur.geocoder.Utilities;
import org.entur.geocoder.blobStore.BlobStoreFiles;
import org.entur.geocoder.camel.ErrorHandlerRouteBuilder;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.haya.adminUnitsCache.AdminUnitsCache;
import org.entur.haya.blobStore.HayaBlobStoreService;
import org.entur.haya.blobStore.KakkaBlobStoreService;
import org.entur.haya.csv.CSVCreator2;
import org.entur.haya.peliasDocument.stopPlacestoPeliasDocument.CSVReader2;
import org.entur.haya.peliasDocument.stopPlacestoPeliasDocument.ParentsInfoEnricher;
import org.entur.netex.NetexParser;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class AddressesDataRouteBuilder extends ErrorHandlerRouteBuilder {

    private static final Logger logger = LoggerFactory.getLogger(AddressesDataRouteBuilder.class);

    private static final String OUTPUT_FILENAME_HEADER = "hayaOutputFilename";
    private static final String ADMIN_UNITS_CACHE_PROPERTY = "AdminUnitsCache";

    @Value("${blobstore.gcs.kakka.adminUnits.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String adminUnitsFile;

    @Value("${blobstore.gcs.haya.peliasDocuments.folder:pelias-documents}")
    private String peliasDocumentsFolder;

    @Value("${haya.workdir:/tmp/haya/geocoder}")
    private String hayaWorkDir;

    private final KakkaBlobStoreService kakkaBlobStoreService;
    private final HayaBlobStoreService hayaBlobStoreService;

    public AddressesDataRouteBuilder(
            KakkaBlobStoreService kakkaBlobStoreService,
            HayaBlobStoreService hayaBlobStoreService,
            @Value("${haya.camel.redelivery.max:3}") int maxRedelivery,
            @Value("${haya.camel.redelivery.delay:5000}") int redeliveryDelay,
            @Value("${haya.camel.redelivery.backoff.multiplier:3}") int backOffMultiplier) {
        super(maxRedelivery, redeliveryDelay, backOffMultiplier);
        this.kakkaBlobStoreService = kakkaBlobStoreService;
        this.hayaBlobStoreService = hayaBlobStoreService;
    }

    @Override
    public void configure() {

        from("direct:makeCSV")
                .to("direct:cacheAdminUnits")
                .process(this::listPeliasDocumentCSVFiles)
                .process(exchange -> {
                    Message in = exchange.getIn();
                    List<BlobStoreFiles.File> files = in.getBody(BlobStoreFiles.class).getFiles();
                    in.setBody(files);
                    in.setHeader("CompletionSize", files.size());
                })
                .split().body()
                .process(this::loadPeliasDocumentCSVFile)
                .process(this::unzipPeliasDocumentsCSVFileToWorkingDirectory)
                .aggregate(constant(true), new UseLatestAggregationStrategy()).completionSize(header("CompletionSize"))
                .process(this::listUnzippedPeliasDocumentFiles)
                .split().body()
                .process(this::readPeliasDocumentsCSVFile)
                .aggregate(constant(true), new ConcatStreamAggregationStrategy()).completionSize(header("CompletionSize"))
                .process(this::enrichWithParentInfo)
                .process(exchange -> {
                    Stream<PeliasDocument> peliasDocumentStream = exchange.getIn().getBody(Stream.class);
                    CSVCreator2.create(peliasDocumentStream);
                })
//                .process(this::createPeliasDocumentsForAllIndividualAddresses)
//                .process(this::addPeliasDocumentForStreets)
                .process(this::setOutputFilenameHeader)
                .process(this::zipCSVFile)
                .process(this::uploadCSVFile)
                .process(this::updateCurrentFile);

        from("direct:cacheAdminUnits")
                .process(this::loadAdminUnitsFile)
                .process(this::unzipStopPlacesToWorkingDirectory)
                .process(this::parseStopPlacesNetexFile)
                .process(this::buildAdminUnitCache);
    }

    private void loadAdminUnitsFile(Exchange exchange) {
        logger.debug("Loading admin units file");
        exchange.getIn().setBody(
                kakkaBlobStoreService.getBlob(adminUnitsFile),
                InputStream.class
        );
    }

    private void unzipStopPlacesToWorkingDirectory(Exchange exchange) {
        logger.debug("Unzipping admin units file");
        ZipUtilities.unzipFile(
                exchange.getIn().getBody(InputStream.class),
                hayaWorkDir + "/adminUnits"
        );
    }

    private void parseStopPlacesNetexFile(Exchange exchange) {
        logger.debug("Parsing the admin units Netex file.");
        var parser = new NetexParser();
        try (Stream<Path> paths = Files.walk(Paths.get(hayaWorkDir + "/adminUnits"))) {
            paths.filter(Files::isRegularFile).filter(path -> {
                try {
                    return !Files.isHidden(path);
                } catch (IOException e) {
                    return false;
                }
            }).findFirst().ifPresent(path -> {
                try (InputStream inputStream = new FileInputStream(path.toFile())) {
                    exchange.getIn().setBody(parser.parse(inputStream));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void buildAdminUnitCache(Exchange exchange) {
        logger.debug("Building admin units cache");
        var netexEntitiesIndex = exchange.getIn().getBody(NetexEntitiesIndex.class);
        var adminUnitsCache = AdminUnitsCache.buildNewCache(netexEntitiesIndex);
        exchange.setProperty(ADMIN_UNITS_CACHE_PROPERTY, adminUnitsCache);
    }

    private void listPeliasDocumentCSVFiles(Exchange exchange) {
        logger.debug("Loading pelias documents csv files");
        exchange.getIn().setBody(
                hayaBlobStoreService.listBlobStoreFiles(peliasDocumentsFolder),
                InputStream.class
        );
    }

    private void loadPeliasDocumentCSVFile(Exchange exchange) {
        BlobStoreFiles.File file = exchange.getIn().getBody(BlobStoreFiles.File.class);
        logger.debug("Loading pelias documents file: " + file.getFileNameOnly());
        exchange.getIn().setBody(
                hayaBlobStoreService.getBlob(file.getName()),
                InputStream.class
        );
    }

    private void unzipPeliasDocumentsCSVFileToWorkingDirectory(Exchange exchange) {
        logger.debug("Unzipping pelias documents file " + exchange.getIn().getHeader("pelias-document-filename"));
        ZipUtilities.unzipFile(
                exchange.getIn().getBody(InputStream.class),
                hayaWorkDir + "/pelias-document-csv"
        );
    }

    private void listUnzippedPeliasDocumentFiles(Exchange exchange) {
        logger.debug("List unzipped pelias document files");
        try (Stream<Path> paths = Files.walk(Paths.get(hayaWorkDir + "/pelias-document-csv"))) {
            exchange.getIn().setBody(paths.filter(Utilities::isValidFile).toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void readPeliasDocumentsCSVFile(Exchange exchange) {
        Path filePath = exchange.getIn().getBody(Path.class);
        logger.debug("Read addresses CSV file " + filePath.getFileName());
        exchange.getIn().setBody(CSVReader2.read(filePath));
    }

    private void enrichWithParentInfo(Exchange exchange) {
        logger.debug("Enriching the parent information");
        @SuppressWarnings("unchecked")
        Stream<PeliasDocument> peliasDocumentStream = exchange.getIn().getBody(Stream.class);
        AdminUnitsCache adminUnitsCache = exchange.getProperty(ADMIN_UNITS_CACHE_PROPERTY, AdminUnitsCache.class);
        AtomicInteger integer = new AtomicInteger(0);
        ParentsInfoEnricher parentsInfoEnricher = new ParentsInfoEnricher(adminUnitsCache, integer);
        exchange.getIn().setBody(
                peliasDocumentStream
                        .parallel()
                        .map(parentsInfoEnricher::enrichParentsInfo)
        );
    }
/*
    private void createPeliasDocumentsForAllIndividualAddresses(Exchange exchange) {
        logger.debug("Create peliasDocuments for addresses");

        Collection<KartverketAddress> addresses = exchange.getIn().getBody(Collection.class);
        AdminUnitsCache adminUnitsCache = exchange.getProperty(ADMIN_UNITS_CACHE_PROPERTY, AdminUnitsCache.class);

        long startTime = System.nanoTime();

        // Create documents for all individual addresses
        List<PeliasDocument> peliasDocuments = addresses.parallelStream()
                .map(peliasDocument -> addressMapper.toPeliasDocument(peliasDocument, adminUnitsCache))
                .collect(Collectors.toList());

        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;

        logger.debug("Create documents for all individual addresses duration(ms): " + duration);

        exchange.getIn().setBody(peliasDocuments);
    }

    private void addPeliasDocumentForStreets(Exchange exchange) {
        logger.debug("Add peliasDocuments for streets.");

        List<PeliasDocument> peliasDocuments = exchange.getIn().getBody(List.class);

        long startTime = System.nanoTime();

        Comparator<PeliasDocument> peliasDocumentComparator = Comparator
                .comparing(PeliasDocument::addressParts, Comparator.comparing(AddressParts::getStreet))
                .thenComparing(PeliasDocument::parent, Comparator.comparing((Parent parent) -> parent.getParentFields().get(Parent.FieldName.LOCALITY).id()));

        List<PeliasDocument> sortedPeliasDocuments = peliasDocuments.stream().sorted(peliasDocumentComparator).toList();

        //Create separate document per unique street
        peliasDocuments.addAll(addressToStreetMapper.createStreetPeliasDocumentsFromAddresses(peliasDocuments));

        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;

        logger.debug("Add peliasDocuments for streets duration(ms): " + duration);

        exchange.getIn().setBody(peliasDocuments);
    }
*/

    private void setOutputFilenameHeader(Exchange exchange) {
        exchange.getIn().setHeader(
                OUTPUT_FILENAME_HEADER,
                "haya_export_geocoder_" + System.currentTimeMillis()
        );
    }

    private void zipCSVFile(Exchange exchange) {
        logger.debug("Zipping the created csv file");
        ByteArrayInputStream zipFile = ZipUtilities.zipFile(
                exchange.getIn().getBody(InputStream.class),
                exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class) + ".csv"
        );
        exchange.getIn().setBody(zipFile);
    }

    private void uploadCSVFile(Exchange exchange) {
        logger.debug("Uploading the CSV file");
        hayaBlobStoreService.uploadBlob(
                exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class) + ".zip",
                exchange.getIn().getBody(InputStream.class)
        );
    }

    private void updateCurrentFile(Exchange exchange) {
        logger.debug("Updating the current file");
        String currentCSVFileName = exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class) + ".zip";
        hayaBlobStoreService.uploadBlob(
                "current",
                new ByteArrayInputStream(currentCSVFileName.getBytes())
        );
    }
}