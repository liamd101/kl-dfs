// https://docs.rs/tonic-build/0.4.1/tonic_build/
// https://kvwu.io/posts/setting-up-a-grpc-protobuf-server-with-tonic/
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = &[
        "protocols/datanode.proto",
        "protocols/client.proto",
        "protocols/basic.proto",
    ];
    tonic_build::configure()
        .build_server(true)
        .compile(proto_files, &["protocols"])?;
    Ok(())
}
