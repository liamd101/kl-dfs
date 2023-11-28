// extern crate prost_build;
// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let proto_files = &["protocols/basic.proto", "protocols/client.proto", "protocols/node.proto", "protocols/datanode.proto"];
//     prost_build::compile_protos(proto_files, &["src/"])?;
//     Ok(())
// }

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     tonic_build::compile_protos("protocols/basic.proto")?;
//     tonic_build::compile_protos("protocols/datanode.proto")?;
//     Ok(())
// }

// https://docs.rs/tonic-build/0.4.1/tonic_build/
// https://kvwu.io/posts/setting-up-a-grpc-protobuf-server-with-tonic/
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let proto_files = &["protocols/datanode.proto", "protocols/client.proto", "protocols/basic.proto"];
    let proto_files = &["protocols/datanode.proto", "protocols/client.proto", "protocols/basic.proto"];
    tonic_build::configure()
        .build_server(true)
        .compile(proto_files, &["protocols"])?;
    Ok(())
}
