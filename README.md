# MySync

CSC-492: Senior Project
Hayden Baker

## Getting Started

### Building
Install Rust-Nightly
``` curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh ```
- Make sure you install the nightly toolchain

Make sure that the rust compiler (rustc) is installed
``` rustc --version ```
- You should see ```rustc 1.4x.x-nightly .... ``` as the version

If new VM, update apt
``` apt-get update ```

Install necessary packages to build the binaries
``` apt-get install build-essential pkg-config libssl-dev```

### Configuration
To configure the service to work, you must edit the sync-client/server configuration files with:
    - valid AWS credentials
    - a client-id
    - sync directory (must be of following format: ```/dir1/dir2/```)
    - s3 bucket name
    - the downstream queue handle
    - the sqs handle prefix (i.e. ```https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxxxxxx```)

```
Give examples
```

### Installing

```
Give the example
```


## Deployment
