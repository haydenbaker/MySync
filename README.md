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
Configuration files for both of the services exist, by default, at ```~/.config/sync-client``` and ```~/.config/sync-server```

To configure the services to work, you must edit the sync-client/server configuration files with:
- valid AWS credentials
- a client-id, only the client config requires this
- sync directory (must be of following format: ```/dir1/dir2/```), only the client config requires this
- s3 bucket name
- the downstream queue handle
- the sqs handle prefix (i.e. ```https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxxxxxx```)


### Running
Both services can be run in development mode by:
```cargo run --bin client```
```cargo run --bin server```

### Building Binaries
However, you should really build the binaries for the individual services
```cargo build --bin client --release```
```cargo build --bin server --release```  
Both binaries should be built and placed within ```target/release```
Be wary that building in release mode will take a few minutes, since this application relies on a few heavyweight crates

## Deployment
