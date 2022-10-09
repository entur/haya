package org.entur.haya.camel;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.UseLatestAggregationStrategy;
import org.entur.geocoder.Utilities;
import org.entur.geocoder.ZipUtilities;
import org.entur.geocoder.blobStore.BlobStoreFiles;
import org.entur.geocoder.camel.ErrorHandlerRouteBuilder;
import org.entur.geocoder.csv.CSVReader;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.haya.adminUnitsCache.AdminUnitsCache;
import org.entur.haya.blobStore.HayaBlobStoreService;
import org.entur.haya.blobStore.KakkaBlobStoreService;
import org.entur.haya.csv.*;
import org.entur.haya.adminUnitsCache.ParentsInfoEnricher;
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
import java.util.List;
import java.util.stream.Stream;

@Component
public class HayaRouteBuilder extends ErrorHandlerRouteBuilder {

    private static final Logger logger = LoggerFactory.getLogger(HayaRouteBuilder.class);

    private static final String OUTPUT_FILENAME_HEADER = "hayaOutputFilename";
    private static final String ADMIN_UNITS_CACHE_PROPERTY = "AdminUnitsCache";
    public static final String COMPLETION_SIZE = "CompletionSize";

    @Value("${blobstore.gcs.kakka.adminUnits.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String adminUnitsFile;

    @Value("${blobstore.gcs.haya.import.folder:import}")
    private String importFolder;

    @Value("${haya.workdir:/tmp/haya/geocoder}")
    private String hayaWorkDir;

    private final KakkaBlobStoreService kakkaBlobStoreService;
    private final HayaBlobStoreService hayaBlobStoreService;

    public HayaRouteBuilder(
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
                .split().body()
                .process(this::loadPeliasDocumentCSVFile)
                .process(this::unzipPeliasDocumentsCSVFileToWorkingDirectory)
                .aggregate(constant(true), new UseLatestAggregationStrategy())
                .completionSize(header(COMPLETION_SIZE))
                .process(this::listUnzippedPeliasDocumentFiles)
                .split().body()
                .process(this::readPeliasDocumentsCSVFile)
                .process(this::enrichWithParentInfo)
                .aggregate(constant(true), new ConcatStreamAggregationStrategy())
                .completionSize(header(COMPLETION_SIZE))
                .process(this::terminateStreamToCreatePeliasCSV)
                .process(this::setOutputFilenameHeader)
                .process(this::zipCSVFile)
                .process(this::uploadCSVFile)
                .process(this::copyCSVFileAsLatestToConfiguredBucket);

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
        exchange.setProperty(ADMIN_UNITS_CACHE_PROPERTY, AdminUnitsCache.buildNewCache(netexEntitiesIndex));
    }

    private void listPeliasDocumentCSVFiles(Exchange exchange) {
        logger.debug("Listing the pelias documents zip files");

        BlobStoreFiles blobStoreFiles = hayaBlobStoreService.listBlobStoreFiles(importFolder);
        List<BlobStoreFiles.File> files = blobStoreFiles.getFiles();

        exchange.getIn().setHeader(COMPLETION_SIZE, files.size());
        exchange.getIn().setBody(files);
    }

    private void loadPeliasDocumentCSVFile(Exchange exchange) {
        BlobStoreFiles.File file = exchange.getIn().getBody(BlobStoreFiles.File.class);
        logger.debug("Loading pelias documents file: " + file.getFileNameOnly());
        exchange.getIn().setBody(hayaBlobStoreService.getBlob(file.getName()));
    }

    private void unzipPeliasDocumentsCSVFileToWorkingDirectory(Exchange exchange) {
        logger.debug("Unzipping the file ");
        ZipUtilities.unzipFile(
                exchange.getIn().getBody(InputStream.class),
                hayaWorkDir + "/pelias-document-csv"
        );
    }

    private void listUnzippedPeliasDocumentFiles(Exchange exchange) {
        logger.debug("List unzipped pelias document files");
        try (Stream<Path> paths = Files.walk(Paths.get(hayaWorkDir + "/pelias-document-csv"))) {
            List<Path> pathList = paths.filter(Utilities::isValidFile).toList();
            exchange.getIn().setHeader(COMPLETION_SIZE, pathList.size());
            exchange.getIn().setBody(pathList);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void readPeliasDocumentsCSVFile(Exchange exchange) {
        Path filePath = exchange.getIn().getBody(Path.class);
        logger.debug("Read addresses CSV file " + filePath.getFileName());
        exchange.getIn().setBody(CSVReader.read(filePath));
    }

    private void enrichWithParentInfo(Exchange exchange) {
        logger.debug("Enriching the parent information");
        @SuppressWarnings("unchecked")
        Stream<PeliasDocument> peliasDocumentStream = exchange.getIn().getBody(Stream.class);
        AdminUnitsCache adminUnitsCache = exchange.getProperty(ADMIN_UNITS_CACHE_PROPERTY, AdminUnitsCache.class);
        ParentsInfoEnricher parentsInfoEnricher = new ParentsInfoEnricher(adminUnitsCache);
        exchange.getIn().setBody(
                peliasDocumentStream
                        .map(parentsInfoEnricher::enrichParentsInfo)
        );
    }

    private void terminateStreamToCreatePeliasCSV(Exchange exchange) {
        logger.debug("Create Pelias CSV file");
        @SuppressWarnings("unchecked")
        Stream<PeliasDocument> peliasDocumentStream = exchange.getIn().getBody(Stream.class);
        exchange.getIn().setBody(
                PeliasCSV.create(peliasDocumentStream)
        );
    }

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

    private void copyCSVFileAsLatestToConfiguredBucket(Exchange exchange) {
        logger.debug("Coping latest file to moradin");
        String currentCSVFileName = exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class) + ".zip";
        hayaBlobStoreService.copyBlobAsLatestToTargetBucket(currentCSVFileName);
    }
}