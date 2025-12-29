package com.impact.utils.gcp;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;

import java.io.IOException;

public class SecretManager {

    public static String getSecret(String projectId, String secretId, String versionId) throws IOException {
        String secretValue = null;
        try(SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretId, versionId);
            AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
            secretValue = response.getPayload().getData().toStringUtf8();
            //System.out.printf("Plaintext: %s\n", payload);
        }
        return secretValue;
    }
}
