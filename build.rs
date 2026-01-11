fn main() {
    #[cfg(feature = "grpc")]
    {
        tonic_build::configure()
            .build_server(true)
            .build_client(true) // 也生成客户端，方便测试
            .protoc_arg("--experimental_allow_proto3_optional") // 可选，根据需要
            .compile_protos(&["proto/graphdb.proto"], &["proto"]) // 使用 compile_protos 而不是 compile
            .expect("Failed to compile gRPC proto");
    }
}
