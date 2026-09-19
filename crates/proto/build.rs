//! 生成 gRPC 面（S3-0）。
//!
//! 依赖本机的 `protoc`（走 prost-build，需要它）。
//! 若在 CI/新机器上缺 `protoc`，两条正路：装 protobuf-compiler，或改用
//! `protoc-bin-vendored` 并把可执行文件路径喂给 `tonic_prost_build::configure().protoc_executable(...)`。
//! 本轮环境已有 `libprotoc 36.1`，故直接用系统 protoc。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/meta.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/meta.proto"], &["proto"])?;
    Ok(())
}
