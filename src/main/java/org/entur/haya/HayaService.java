package org.entur.haya;

import org.entur.geocoder.Utilities;
import org.entur.geocoder.ZipUtilities;
import org.entur.geocoder.blobStore.BlobStoreFiles;
import org.entur.geocoder.csv.CSVReader;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.haya.adminUnitsCache.AdminUnitsCache;
import org.entur.haya.adminUnitsCache.ParentsInfoEnricher;
import org.entur.haya.blobStore.HayaBlobStoreService;
import org.entur.haya.blobStore.KakkaBlobStoreService;
import org.entur.haya.csv.PeliasCSV;
import org.entur.netex.NetexParser;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

@Service
public class HayaService {

    private static final Logger logger = LoggerFactory.getLogger(HayaService.class);

    @Value("${blobstore.gcs.kakka.adminUnits.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String adminUnitsFile;

    @Value("${blobstore.gcs.haya.import.folder:import}")
    private String importFolder;

    @Value("${haya.workdir:/tmp/haya/geocoder}")
    private String hayaWorkDir;

    private final KakkaBlobStoreService kakkaBlobStoreService;
    private final HayaBlobStoreService hayaBlobStoreService;

    public HayaService(
            KakkaBlobStoreService kakkaBlobStoreService,
            HayaBlobStoreService hayaBlobStoreService) {
        this.kakkaBlobStoreService = kakkaBlobStoreService;
        this.hayaBlobStoreService = hayaBlobStoreService;
    }

    @Retryable(
            value = Exception.class,
            maxAttemptsExpression = "${haya.retry.maxAttempts:3}",
            backoff = @Backoff(
                    delayExpression = "${haya.retry.maxDelay:5000}",
                    multiplierExpression = "${haya.retry.backoff.multiplier:3}"))
    protected InputStream loadAdminUnitsFile() {
        logger.info("Loading admin units file");
        return kakkaBlobStoreService.getBlob(adminUnitsFile);
    }

    protected Path unzipAdminUnitsToWorkingDirectory(InputStream inputStream) {
        logger.info("Unzipping admin units file");
        var targetFolder = hayaWorkDir + "/adminUnits";
        ZipUtilities.unzipFile(inputStream, targetFolder);
        try (Stream<Path> paths = Files.walk(Paths.get(targetFolder))) {
            return paths
                    .filter(Utilities::isValidFile)
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Unzipped file not found."));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected NetexEntitiesIndex parseAdminUnitsNetexFile(Path path) {
        logger.info("Parsing the admin units Netex file");
        var parser = new NetexParser();
        try (InputStream inputStream = new FileInputStream(path.toFile())) {
            return parser.parse(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected AdminUnitsCache buildAdminUnitCache(NetexEntitiesIndex netexEntitiesIndex) {
        logger.info("Building admin units cache");
        return AdminUnitsCache.buildNewCache(netexEntitiesIndex);
    }

    @Retryable(
            value = Exception.class,
            maxAttemptsExpression = "${haya.retry.maxAttempts:3}",
            backoff = @Backoff(
                    delayExpression = "${haya.retry.maxDelay:5000}",
                    multiplierExpression = "${haya.retry.backoff.multiplier:3}"))
    protected List<BlobStoreFiles.File> listPeliasDocumentCSVFiles() {
        logger.info("Listing the pelias documents zip files");
        BlobStoreFiles blobStoreFiles = hayaBlobStoreService.listBlobStoreFiles(importFolder);
        return blobStoreFiles.getFiles();
    }

    @Retryable(
            value = Exception.class,
            maxAttemptsExpression = "${haya.retry.maxAttempts:3}",
            backoff = @Backoff(
                    delayExpression = "${haya.retry.maxDelay:5000}",
                    multiplierExpression = "${haya.retry.backoff.multiplier:3}"))
    protected InputStream loadPeliasDocumentCSVFile(BlobStoreFiles.File file) {
        logger.info("Loading pelias documents file: " + file.getFileNameOnly());
        return hayaBlobStoreService.getBlob(file.getName());
    }

    protected void unzipPeliasDocumentsCSVFileToWorkingDirectory(InputStream inputStream) {
        logger.info("Unzipping the file");
        ZipUtilities.unzipFile(inputStream, hayaWorkDir + "/pelias-document-csv");
    }

    protected List<Path> listUnZippedFiles() {
        logger.info("Listing unzipping the file");
        try (Stream<Path> paths = Files.walk(Paths.get(hayaWorkDir + "/pelias-document-csv"))) {
            return paths.filter(Utilities::isValidFile).toList();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Stream<PeliasDocument> readPeliasDocuments(Path path) {
        logger.info("Read CSV file " + path.getFileName());
        return CSVReader.read(path);
    }

    protected Stream<PeliasDocument> enrichWithParentInfo(Stream<PeliasDocument> peliasDocumentStream,
                                                          AdminUnitsCache adminUnitsCache) {
        logger.info("Enriching the parent information");
        ParentsInfoEnricher parentsInfoEnricher = new ParentsInfoEnricher(adminUnitsCache);
        return peliasDocumentStream.map(parentsInfoEnricher::enrichParentsInfo);
    }

    protected InputStream createPeliasCSV(Stream<PeliasDocument> peliasDocumentStream) {
        logger.info("Create Pelias CSV file");
        return PeliasCSV.create(peliasDocumentStream);
    }

    protected String getOutputFilename() {
        return "haya_export_geocoder_" + System.currentTimeMillis();
    }

    protected InputStream zipCSVFile(InputStream inputStream, String filename) {
        logger.info("Zipping the created csv file");
        return ZipUtilities.zipFile(inputStream, filename + ".csv");
    }

    @Retryable(
            value = Exception.class,
            maxAttemptsExpression = "${haya.retry.maxAttempts:3}",
            backoff = @Backoff(
                    delayExpression = "${haya.retry.maxDelay:5000}",
                    multiplierExpression = "${haya.retry.backoff.multiplier:3}"))
    protected void uploadCSVFile(InputStream inputStream, String filename) {
        logger.info("Uploading the CSV file");
        hayaBlobStoreService.uploadBlob(filename + ".zip", inputStream);
    }

    @Retryable(
            value = Exception.class,
            maxAttemptsExpression = "${haya.retry.maxAttempts:3}",
            backoff = @Backoff(
                    delayExpression = "${haya.retry.maxDelay:5000}",
                    multiplierExpression = "${haya.retry.backoff.multiplier:3}"))
    protected void copyCSVFileAsLatestToConfiguredBucket(String filename) {
        logger.info("Coping latest file to moradin");
        hayaBlobStoreService.copyBlobAsLatestToTargetBucket(filename + ".zip");
    }
}
