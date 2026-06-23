# GCP setup for local development and maintenance

Maintainer access to the staging project (`neuro-d3-staging`) uses your own Google identity with Application Default Credentials (ADC). No service account key files, no static keys.

1. Install the gcloud CLI on Mac

```
brew install --cask google-cloud-sdk
```

2. Authenticate

```
gcloud auth login
gcloud auth application-default login
gcloud config set project neuro-d3-staging
```

`gcloud auth login` authorizes the CLI itself. `gcloud auth application-default login` writes the ADC file that client libraries (gcsfs, google-cloud-storage, Terraform) pick up automatically.

3. Verify

```
gcloud projects describe neuro-d3-staging
gcloud storage ls gs://neuro-d3-staging-find-reuse-cache/
```

The second command succeeds (possibly listing nothing) once the bucket exists and your identity has access. Project owners have access implicitly; otherwise ask a maintainer to grant `roles/storage.objectAdmin` on the bucket.

4. Notes

* On the staging GCE VM and inside its containers, credentials come from the attached `neuro-d3-airflow` service account via the metadata server. Nothing to configure there.
* For unusual hosts (CI runners, non-GCP VMs), set `GOOGLE_APPLICATION_CREDENTIALS` to point at a provisioned ADC file, or use fixture seeding (`SEED_SOURCE=fixtures`) which needs no cloud access.
