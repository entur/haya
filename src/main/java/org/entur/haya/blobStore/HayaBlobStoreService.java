package org.entur.haya.blobStore;

import org.entur.geocoder.blobStore.BlobStoreRepository;
import org.entur.geocoder.blobStore.BlobStoreService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class HayaBlobStoreService extends BlobStoreService {

    @Value("${blobstore.gcs.moradin.bucket.name:moradin-dev}")
    private String targetBucketName;

    @Value("${blobstore.gcs.moradin.latest.filename_without_extension:haya_latest}")
    private String targetFilename;

    @Value("${blobstore.gcs.moradin.import.folder:import}")
    private String targetFolder;

    public HayaBlobStoreService(
            @Value("${blobstore.gcs.haya.bucket.name:haya-dev}") String bucketName,
            @Autowired BlobStoreRepository repository) {
        super(bucketName, repository);
    }

    public void copyBlobAsLatestToTargetBucket(String sourceName) {
        super.copyBlob(sourceName, targetBucketName, targetFolder + "/" + targetFilename + ".zip");
    }
}