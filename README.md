# Google Cloud Storage client

This is reimplementation of the most common commands from `gcloud storage` in
the Google SDK. The main purpose is to avoid needing to install the huge sdk
during CI.

```sh

$ ./gcs-client
NAME:
   gcs-client - A new cli application

USAGE:
   gcs-client [global options] [command [command options]]

COMMANDS:
   cp        Copy files and objects to and from Google Cloud Storage
   cat       Print the content of GCS objects
   rm        Delete GCS objects
   mv        Move or rename GCS objects
   ls        List GCS buckets and objects
   hash      Calculate or retrieve hashes for GCS objects
   restore   Restore soft-deleted GCS objects
   sign-url  Generates a signed URL that gives unauthenticated access to a GCS resource
   version
   help, h   Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --project string, -p string  Google Cloud Project ID [$GOOGLE_CLOUD_PROJECT]
   --help, -h                   show help
```

## Auth

This is built on the Google Cloud Go SDK, so it uses the same authentication
methods. Should work with Application Default Credentials, service account keys,
and Workload Identity Federation.

## Shell Completion

To enable shell completion, source the completion script for your shell:

### Bash

```sh
source <(gcs-client completion bash)
```

### Zsh

```sh
source <(gcs-client completion zsh)
```

### Fish

```sh
gcs-client completion fish | source
```

## GitHub Action (experimental)

You can use this in your GitHub Actions workflows to quickly set up the `gcs-client`:

```yaml
# Option 1: Set up the binary in PATH
- uses: marcusramberg/gcs-client@v0.2.2

# Option 2: Run a specific command without modifying PATH
- uses: marcusramberg/gcs-client@v0.2.2
  with:
    command: ls gs://my-bucket
```
