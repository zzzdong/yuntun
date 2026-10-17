//! 生成 gRPC 面（S3-0）。
//!
//! **自带 `protoc`**（`protoc-bin-vendored`）：早先这里要求本机装 `protobuf-compiler`，
//! 于是 CI / 新机器 / cross 容器各要装一遍，版本还不一致。现在把 protoc 作为构建依赖
//! 固定下来 —— 生成结果与运行环境无关，也省掉一条"先装 protoc"的隐形前置。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/meta.proto");
    println!("cargo:rerun-if-changed=proto/shard.proto");

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(
            config,
            &["proto/meta.proto", "proto/shard.proto"],
            &["proto"],
        )?;
    Ok(())
}
