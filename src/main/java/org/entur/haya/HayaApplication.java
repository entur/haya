package org.entur.haya;

import org.entur.geocoder.model.PeliasDocument;
import org.entur.haya.adminUnitsCache.AdminUnitsCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

import java.io.InputStream;
import java.util.stream.Stream;

@SpringBootApplication
@EnableRetry
public class HayaApplication implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(HayaApplication.class);

    private final HayaService hs;

    public HayaApplication(HayaService hs) {
        this.hs = hs;
    }

    public static void main(String[] args) {
        SpringApplication.run(HayaApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        AdminUnitsCache adminUnitsCache = Stream.of(hs.loadAdminUnitsFile())
                .map(hs::unzipAdminUnitsToWorkingDirectory)
                .map(hs::parseAdminUnitsNetexFile)
                .map(hs::buildAdminUnitCache)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Failed to create admin unit cache."));

        hs.listPeliasDocumentCSVFiles().stream()
                .map(hs::loadPeliasDocumentCSVFile)
                .forEach(hs::unzipPeliasDocumentsCSVFileToWorkingDirectory);

        Stream<PeliasDocument> reduce = hs.listUnZippedFiles().stream()
                .map(hs::readPeliasDocuments)
                .map(peliasDocumentStream -> hs.enrichWithParentInfo(peliasDocumentStream, adminUnitsCache))
                .reduce(Stream.empty(), Stream::concat);

        InputStream peliasCSV = hs.createPeliasCSV(reduce);
        zipAndUploadCSVFile(peliasCSV);
    }

    private void zipAndUploadCSVFile(InputStream inputStream) {
        String outputFilename = hs.getOutputFilename();
        InputStream csvZipFile = hs.zipCSVFile(inputStream, outputFilename);
        hs.uploadCSVFile(csvZipFile, outputFilename);
        hs.copyCSVFileAsLatestToConfiguredBucket(outputFilename);
        logger.info("Uploaded zipped csv files to haya and moradin");
    }
}
