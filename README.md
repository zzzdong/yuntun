# Yuntun 数据库

Yuntun 是一个面向可观察数据的数据库，基于 Rust 开发，使用 Arrow 和 DataFusion 技术栈，提供多种协议支持。

## 项目概述

Yuntun 数据库设计用于处理可观察数据，如监控指标、日志和追踪数据。它提供了以下核心功能：

- 支持 Influx Line Protocol 数据摄入
- 基于内存和 Parquet 文件的混合存储
- 使用 DataFusion 提供 SQL 查询能力
- 支持 Flight SQL 协议
- 支持 PostgreSQL Wire Protocol (PgWire)
- 模块化架构设计，易于扩展
- 统一的存储路径规划

## 架构设计

Yuntun 采用模块化架构，主要由以下服务组成：

1. **Catalog 服务**：管理数据库和表信息，包括表结构和分片信息
2. **Meta 服务**：管理元数据，使用 fjall 数据库存储
3. **Store 服务**：负责数据存储，支持内存中的 RecordBatch 和 Parquet 文件存储
4. **Ingest 服务**：接收数据摄入请求，支持 Influx Line Protocol
5. **Query 服务**：使用 DataFusion 提供 SQL 查询能力
6. **Flight SQL 服务**：提供 Flight SQL 协议支持
7. **PgWire 服务**：提供 PostgreSQL Wire Protocol 支持

## 技术栈

- **Rust**：使用 1.93 版本，2024  edition
- **Arrow**：57 版本，用于内存数据表示
- **DataFusion**：51 版本，用于 SQL 查询处理
- **Object Store**：0.13 版本，用于底层存储
- **InfluxDB Line Protocol**：2.0.0 版本，用于数据摄入
- **Axum**：0.8.8 版本，用于 HTTP 服务
- **Tonic**：0.14 版本，用于 gRPC 和 Flight SQL 服务
- **DataFusion PostgreSQL**：0.14.0 版本，用于 PgWire 协议支持
- **Fjall**：3.0.1 版本，用于元数据存储

## 存储结构

Yuntun 使用统一的存储路径规划，所有数据文件都存储在 `./storage/` 目录中：

```
./storage/
├── meta/          # 元数据存储目录（使用 fjall 数据库）
├── data/          # Parquet 数据文件存储目录
├── wal/           # WAL（预写日志）存储目录（预留）
├── index/         # 索引文件存储目录（预留）
└── tmp/           # 临时文件存储目录（预留）
```

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

### 服务端点

启动后，Yuntun 提供以下服务端点：

- **HTTP 服务**：http://localhost:8080
  - 健康检查：POST http://localhost:8080/health
  - 数据摄入：POST http://localhost:8080/ingest
  - SQL 查询：POST http://localhost:8080/query
- **Flight SQL 服务**：grpc://localhost:50051
- **PgWire 服务**：postgresql://localhost:5432

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

### 使用 PostgreSQL 客户端

使用 psql 或其他 PostgreSQL 客户端连接到 PgWire 服务：

```bash
psql -h localhost -p 5432 -U postgres
```

### 使用 Flight SQL 客户端

使用 Arrow Flight SQL 客户端连接到 Flight SQL 服务：

```python
from pyarrow.flight import FlightClient

client = FlightClient("grpc://localhost:50051")
# 执行 SQL 查询
```

## 项目结构

```
yuntun/
├── src/
│   ├── bin/
│   │   └── main.rs       # 主程序入口
│   ├── catalog/           # Catalog 服务
│   ├── meta/              # Meta 服务
│   ├── store/             # Store 服务
│   ├── ingest/            # Ingest 服务
│   ├── query/             # Query 服务
│   ├── flight_sql/        # Flight SQL 服务
│   ├── pgwire/            # PgWire 服务
│   ├── core/              # 核心功能
│   └── lib.rs             # 库入口
├── Cargo.toml             # 依赖配置
├── .gitignore             # Git 忽略文件
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

### 查看服务状态

服务启动后，可以通过以下方式查看服务状态：

```bash
curl -X POST http://localhost:8080/health
```

## 未来计划

- 支持更多数据摄入协议
- 优化查询性能
- 支持分布式部署
- 添加更多存储后端
- 增强监控和告警功能
- 实现 WAL 机制，提高数据可靠性
- 实现索引功能，加速查询

## 许可证

本项目采用 MIT 许可证。
