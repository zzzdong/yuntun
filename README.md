# Yuntun 数据库

Yuntun 是一个面向可观察数据的数据库，基于 Rust 开发，使用 Arrow 和 DataFusion 技术栈。

## 项目概述

Yuntun 数据库设计用于处理可观察数据，如监控指标、日志和追踪数据。它提供了以下核心功能：

- 支持 Influx Line Protocol 数据摄入
- 基于内存和 Parquet 文件的混合存储
- 使用 DataFusion 提供 SQL 查询能力
- 模块化架构设计，易于扩展

## 架构设计

Yuntun 采用模块化架构，主要由以下服务组成：

1. **Catalog 服务**：管理数据库和表信息，包括表结构和分片信息
2. **Store 服务**：负责数据存储，支持内存中的 RecordBatch 和 Parquet 文件存储
3. **Ingest 服务**：接收数据摄入请求，支持 Influx Line Protocol
4. **Query 服务**：使用 DataFusion 提供 SQL 查询能力

## 技术栈

- **Rust**：使用 1.93 版本，2024  edition
- **Arrow**：57 版本，用于内存数据表示
- **DataFusion**：51 版本，用于 SQL 查询处理
- **Object Store**：0.13 版本，用于底层存储
- **InfluxDB Line Protocol**：2.0.0 版本，用于数据摄入
- **Actix Web**：用于 HTTP 服务

## 安装方法

### 前提条件

- Rust 1.93 或更高版本
- Cargo 包管理器

### 安装步骤

1. 克隆仓库：

```bash
git clone https://github.com/yourusername/yuntun.git
cd yuntun
```

2. 构建项目：

```bash
cargo build --release
```

3. 运行程序：

```bash
cargo run --release
```

## 使用示例

### 数据摄入

使用 HTTP POST 请求向 `/ingest` 端点发送 Influx Line Protocol 格式的数据：

```bash
curl -X POST http://localhost:8080/ingest \
  -d "cpu,host=server01,region=us-west value=0.64 1434055562000000000"
```

### SQL 查询

使用 HTTP POST 请求向 `/query` 端点发送 SQL 查询：

```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM cpu"}'
```

## 项目结构

```
yuntun/
├── src/
│   ├── bin/
│   │   └── main.rs       # 主程序入口
│   ├── catalog/           # Catalog 服务
│   ├── store/             # Store 服务
│   ├── ingest/            # Ingest 服务
│   ├── query/             # Query 服务
│   ├── core/              # 核心功能
│   └── lib.rs             # 库入口
├── Cargo.toml             # 依赖配置
└── README.md              # 项目说明
```

## 开发指南

### 运行测试

```bash
cargo test
```

### 代码风格

项目使用 Rust 标准代码风格，建议使用 `rustfmt` 进行代码格式化：

```bash
cargo fmt
```

### 代码质量

使用 `clippy` 进行代码质量检查：

```bash
cargo clippy
```

## 未来计划

- 支持更多数据摄入协议
- 优化查询性能
- 支持分布式部署
- 添加更多存储后端
- 增强监控和告警功能

## 许可证

本项目采用 MIT 许可证。
