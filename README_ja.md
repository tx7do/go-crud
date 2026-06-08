<p align="center">
  <h1 align="center">go-crud · ユニバーサルデータアクセスレイヤーツールキット</h1>
  <p align="center">
    <strong>単一のジェネリック Repository インターフェースで8つのデータストレージエンジンを統一</strong>
  </p>
  <p align="center">
    <em>ボイラープレートの繰り返しを終わりに — すべてのコード行をビジネス価値に集中</em>
  </p>
</p>

<p align="center">
  <a href="README.md">中文</a> · <a href="README_en.md">English</a> · <a href="README_ja.md">日本語</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat-square&logo=Go" alt="Go Version" />
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square" alt="License" />
  <img src="https://img.shields.io/badge/PRs-Welcome-brightgreen?style=flat-square" alt="PRs Welcome" />
  <img src="https://pkg.go.dev/badge/github.com/tx7do/go-crud.svg" alt="Go Reference" />
</p>

---

## プロジェクトの特徴

- **統一データアクセスレイヤー**：単一のジェネリック Repository インターフェースで GORM、Ent、MongoDB、ClickHouse、Apache Doris、Elasticsearch、OpenSearch、InfluxDB の8つのデータエンジンをカバー — 反復的なボイラープレートに別れを
- **3つのページネーション戦略**：Offset / Page / Token の3つのページネーションモードで、伝統的な Web ページングから無限スクロールまで全シナリオをカバー
- **構造化フィルタエンジン**：29+ の演算子で AND/OR 多階層ネストをサポート、JSON と Google AIP の両方のフィルタ構文に対応、パラメータ化クエリで SQL インジェクションを防止
- **Protocol Buffers 契約**：Protobuf で標準化されたページネーション、フィルタリング、ソート定義 — gRPC マイクロサービスに最適な適合、インターフェース即ドキュメント
- **Redis キャッシュ層**：Cache-Aside パターンと SingleFlight スタンピード保護を内蔵、1行のコードでキャッシュを有効化
- **監査ログ**：統一された Auditor インターフェース、Context インジェクション、フルチェーンの操作トレースとデータ変更記録
- **データアクセス制御**：Viewer コンテキストでマルチテナント分離をサポート、5段階のデータスコープ（SELF / UNIT / USER / ALL / NONE）できめ細かな行レベル権限を実現
- **完全な型安全性**：Go 1.24+ ジェネリクスベース、`mapper.CopierMapper` による DTO ↔ Entity 双方向マッピング、コンパイル時に型エラーを検出
- **Upsert サポート**：GORM / ClickHouse / Doris でネイティブ Upsert（INSERT ON CONFLICT）をサポート、競合時の自動更新
- **ツリークエリ**：Ent モジュールにツリー構造アセンブルを内蔵、ParentID から階層関係を自動構築

---

## 対応データエンジン

| エンジン | タイプ | 状態 | ユースケース |
|----------|--------|:----:|-------------|
| [GORM](./gorm) | リレーショナル ORM | ✅ | MySQL、PostgreSQL、SQLite、SQL Server 等の主要リレーショナルデータベース |
| [Ent](./entgo) | リレーショナル ORM (コード生成) | ✅ | MySQL、PostgreSQL、SQLite — コンパイル時型安全性、Facebook オープンソース |
| [MongoDB](./mongodb) | ドキュメント DB | ✅ | 半構造化データ、柔軟なスキーマ、コンテンツ管理 |
| [ClickHouse](./clickhouse) | カラムナ OLAP | ✅ | 大規模ログ分析、メトリクス集計、ユーザー行動分析、リアルタイムデータウェアハウス |
| [Apache Doris](./doris) | カラムナ OLAP | ✅ | リアルタイム BI ダッシュボード、インタラクティブ分析、高速 Stream Load 取り込み |
| [Elasticsearch](./elasticsearch) | 検索エンジン | ✅ | フルテキスト検索、ログ分析、ハイライト結果、集計分析 |
| [OpenSearch](./opensearch) | 検索エンジン | ✅ | Elasticsearch オープンソース代替、ベクトル検索、セキュリティ分析 |
| [InfluxDB](./influxdb) | 時系列 DB | ✅ | IoT モニタリング、DevOps メトリクス、時系列データ分析 |
| [Cassandra](./cassandra) | ワイドカラム DB | 🚧 | 高可用性書き込み、クロスデータセンターレプリケーション（開発中） |

---

## システムアーキテクチャ

```mermaid
graph TB
    subgraph API["API 契約層"]
        Proto["Protobuf 定義<br/>PagingRequest · PaginationRequest<br/>FilterExpr · Sorting · FieldMask"]
    end

    subgraph Infra["インフラストラクチャ層"]
        Pagination["Pagination<br/>ページング戦略 · フィルタエンジン · ソート変換"]
        Cache["Cache<br/>Redis Cache-Aside · SingleFlight スタンピード保護"]
        Audit["Audit<br/>監査ログ · Context インジェクション · 変更追跡"]
        Viewer["Viewer<br/>アイデンティティコンテキスト · 権限チェック · 5段階データスコープ"]
    end

    subgraph DAL["データアクセス層"]
        GORM["GORM"]
        ENT["Ent"]
        Mongo["MongoDB"]
        CH["ClickHouse"]
        Doris["Apache Doris"]
        ES["Elasticsearch"]
        OS["OpenSearch"]
        Influx["InfluxDB"]
    end

    API --> Pagination
    Pagination --> DAL
    Cache --> DAL
    Audit --> DAL
    Viewer --> DAL
```

---

## プロジェクト構造

```
go-crud/
├── api/                          # Protocol Buffers 契約定義と生成コード
│   ├── protos/pagination/v1/     # .proto ソースファイル (PagingRequest / FilterExpr / Sorting)
│   └── gen/go/pagination/v1/     # buf 生成 Go コード
├── pagination/                   # コア：ページネーション、フィルタリング、ソーティング
│   ├── paginator/                # ページネータ実装 (Page / Offset / Token)
│   ├── filter/                   # フィルタコンバータ (JSON 構文 / Google AIP 構文)
│   └── sorting/                  # ソートフォーマットコンバータ
├── cache/                        # Redis キャッシュ層 (Cache-Aside + SingleFlight スタンピード保護)
├── audit/                        # 統一監査ログインターフェース (Auditor · Entry · Context)
├── viewer/                       # Viewer コンテキスト (アイデンティティ · 権限 · 5段階データスコープ)
├── gorm/                         # GORM データアクセス層 (CRUD · Upsert · キャッシュ · ソフトデリート)
├── entgo/                        # Ent データアクセス層 (CRUD · ツリークエリ · キャッシュ · トランザクション)
├── mongodb/                      # MongoDB データアクセス層 (CRUD · QueryBuilder)
├── clickhouse/                   # ClickHouse データアクセス層 (CRUD · バッチ書き込み · Upsert)
├── doris/                        # Apache Doris データアクセス層 (CRUD · Stream Load · SQL クエリ)
├── elasticsearch/                # Elasticsearch クライアントとユーティリティ
├── opensearch/                   # OpenSearch クライアントとユーティリティ
├── influxdb/                     # InfluxDB データアクセス層 (Flux クエリ)
└── cassandra/                    # Cassandra データアクセス層 (開発中)
```

---

## コア機能

### データアクセス層

各 DAL モジュールは統一されたジェネリック Repository ラッパーを提供し、`mapper.CopierMapper[DTO, ENTITY]` による DTO ↔ Entity 双方向自動マッピングを実現：

| 機能 | GORM | Ent | MongoDB | ClickHouse | Doris | ES / OS | InfluxDB |
|------|:----:|:---:|:-------:|:----------:|:-----:|:-------:|:--------:|
| Create / Get / Update / Delete | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ページネーションクエリ (Page / Offset / Token) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 構造化フィルタリング (29+ 演算子) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 複数フィールドソート | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| フィールド選択 (FieldMask) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| バッチ書き込み (BatchCreate) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Upsert (INSERT ON CONFLICT) | ✅ | — | — | ✅ | ✅ | — | — |
| ソフトデリート | ✅ | — | — | — | — | — | — |
| カウント (Count / CountWithOptions) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 存在チェック (Exists) | ✅ | ✅ | ✅ | — | — | — | ✅ |
| Redis キャッシュ | ✅ | ✅ | — | — | — | — | — |
| ツリークエリ | — | ✅ | — | — | — | — | — |
| トランザクションサポート | ✅ | ✅ | — | — | ✅ | — | — |
| Stream Load | — | — | — | — | ✅ | — | — |
| Raw SQL クエリ | — | — | — | — | ✅ | ✅ | — |

### フィルタ演算子

Protobuf で定義された構造化フィルタエンジン、29+ の演算子をサポート：

| カテゴリ | 演算子 |
|----------|--------|
| 基本比較 | `EQ` `NEQ` `GT` `GTE` `LT` `LTE` |
| パターンマッチング | `LIKE` `ILIKE` `NOT_LIKE` |
| セット操作 | `IN` `NIN` |
| NULL チェック | `IS_NULL` `IS_NOT_NULL` |
| 範囲と正規表現 | `BETWEEN` `REGEXP` `IREGEXP` |
| 文字列操作 | `CONTAINS` `STARTS_WITH` `ENDS_WITH` `ICONTAINS` `ISTARTS_WITH` `IENDS_WITH` |
| JSON / 配列 | `JSON_CONTAINS` `ARRAY_CONTAINS` `EXISTS` |
| フルテキスト検索 | `SEARCH` `EXACT` `IEXACT` |

`FilterExpr` を介した `AND` / `OR` 多階層ネスト組み合わせで、任意に複雑なクエリロジックを表現可能。

### ページネーション戦略

| モード | ユースケース | 説明 |
|--------|-------------|------|
| **Page-Based** | 伝統的な Web ページング | ページ番号 + ページサイズ、総ページ数表示のあるリストに適している |
| **Offset-Based** | API スキップページクエリ | オフセット + リミット、柔軟なページジャンプに適している |
| **Token-Based** | 無限スクロール / ストリーミング | カーソルベースのページネーション、安定したパフォーマンスでオフセットドリフトなし |

### キャッシュ層

| 機能 | 説明 |
|------|------|
| Cache-Aside パターン | リードスルーとライトインバリデーション、データの一貫性を保証 |
| SingleFlight スタンピード保護 | 同時リクエストの自動統合でバックエンドデータベースを保護 |
| キャッシュタイプごとの独立 TTL | 単一アイテムとリストキャッシュで異なる有効期限ポリシーをサポート |
| メトリクスモニタリング | キャッシュヒット率、レイテンシ等のメトリクス収集を内蔵 |

### 監査ログ

| 機能 | 説明 |
|------|------|
| Auditor インターフェース | 統一された監査ログ記録インターフェース、同期・非同期バッファリングの両方をサポート |
| Context インジェクション | Context を介した監査情報の透過的伝播、非侵入型統合 |
| Entry データモデル | 操作者、操作タイプ、変更内容を含む標準化された監査レコード構造 |
| Noop 実装 | 監査不要時にオーバーヘッドゼロの no-op 実装を内蔵 |

### データアクセス制御

| スコープレベル | 説明 |
|---------------|------|
| **SELF** | 現在のユーザーが作成 / 所有するデータのみ |
| **UNIT** | 組織レベルの分離、現在の部署と下部門をサポート |
| **USER** | 指定されたユーザーリスト |
| **ALL** | フィルタインジェクションなしのフルアクセス |
| **NONE** | すべてのデータアクセスを拒否 |

---

## 技術スタック

| レイヤー | 技術 | 説明 |
|----------|------|------|
| 言語 | Go 1.24+ | ジェネリクスサポートの高性能コンパイル言語 |
| ORM | GORM / Ent | 主要なリレーショナル ORM — ニーズに合わせて選択 |
| ドキュメント DB | MongoDB | NoSQL ドキュメントストレージ |
| OLAP エンジン | ClickHouse / Apache Doris | カラムナストレージ、極致の分析パフォーマンス |
| 検索エンジン | Elasticsearch / OpenSearch | フルテキスト検索とデータ分析 |
| 時系列 DB | InfluxDB | 時系列データ収集と分析 |
| キャッシュ | Redis | スタンピード保護付きインメモリデータストア |
| DTO マッピング | go-utils/mapper | ジェネリック CopierMapper、双方向自動マッピング |
| API 定義 | Protobuf + buf.build | コントラクトファースト API 設計、クロス言語サポート |
| ロギング | go-kratos/log | Kratos フレームワークログ統合 |
| オブザーバビリティ | OpenTelemetry | 分散トレーシングとメトリクス (GORM / Ent) |

---

## クイックスタート

### インストール

```bash
# 必要なモジュールのみインストール

go get github.com/tx7do/go-crud/gorm         # GORM
go get github.com/tx7do/go-crud/entgo         # Ent
go get github.com/tx7do/go-crud/mongodb       # MongoDB
go get github.com/tx7do/go-crud/clickhouse    # ClickHouse
go get github.com/tx7do/go-crud/doris         # Apache Doris
go get github.com/tx7do/go-crud/elasticsearch # Elasticsearch
go get github.com/tx7do/go-crud/opensearch    # OpenSearch
go get github.com/tx7do/go-crud/influxdb      # InfluxDB
```

### 例：GORM Repository

```go
package main

import (
    "context"
    "fmt"

    "github.com/tx7do/go-crud/gorm"
    "github.com/tx7do/go-utils/mapper"
    paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// 1. Entity を定義（データベーステーブルマッピング）
type UserEntity struct {
    ID    uint64 `gorm:"primaryKey;autoIncrement"`
    Name  string `gorm:"column:name;type:varchar(100)"`
    Email string `gorm:"column:email;type:varchar(200)"`
}

func (UserEntity) TableName() string { return "users" }

// 2. Repository を作成
func main() {
    ctx := context.Background()

    m := mapper.NewCopierMapper[User, UserEntity]()
    repo := gorm.NewRepository[User, UserEntity](m)

    // レコード作成
    user, _ := repo.Create(ctx, db, &User{Name: "John", Email: "john@example.com"}, nil)

    // ページネーションクエリ
    page := uint32(1)
    pageSize := uint32(10)
    result, _ := repo.ListWithPaging(ctx, db, &paginationV1.PagingRequest{
        Page:     &page,
        PageSize: &pageSize,
    })
    fmt.Printf("Total: %d, Items: %d\n", result.Total, len(result.Items))
}
```

### 例：ClickHouse Repository

```go
package main

import (
    "github.com/tx7do/go-crud/clickhouse"
    "github.com/tx7do/go-utils/mapper"
)

func main() {
    // クライアント作成
    client, _ := clickhouse.NewClient(
        clickhouse.WithDsn("clickhouse://default:123456@localhost:9000/my_database"),
    )

    // Repository 作成
    m := mapper.NewCopierMapper[Event, EventEntity]()
    repo := clickhouse.NewRepository[Event, EventEntity](client, m, "events", logger)

    // バッチインサート
    events := []*Event{{...}, {...}}
    repo.BatchCreate(ctx, events, nil)

    // ページネーションクエリ
    result, _ := repo.ListWithPaging(ctx, req)
}
```

### 例：構造化フィルタリング

```go
// Protobuf FilterExpr で複雑なクエリを構築
page := uint32(1)
pageSize := uint32(10)
result, _ := repo.ListWithPaging(ctx, db, &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
    FilteringType: &paginationV1.PagingRequest_FilterExpr{
        FilterExpr: &paginationV1.FilterExpr{
            Type: paginationV1.ExprType_AND,
            Conditions: []*paginationV1.FilterCondition{
                {Field: "status", Op: paginationV1.Operator_EQ, Value: &paginationV1.FilterCondition_Value{Value: "active"}},
                {Field: "age", Op: paginationV1.Operator_GTE, Value: &paginationV1.FilterCondition_Value{Value: "18"}},
            },
        },
    },
})
```

---

## 同種プロジェクトとの比較

| 機能 | go-crud | 手書き Repository | 他の CRUD ライブラリ |
|------|---------|-------------------|---------------------|
| マルチエンジン統一 API | ✅ 8エンジン | ❌ 各エンジンを手書き | ❌ 通常1エンジンのみ |
| ジェネリック型安全性 | ✅ DTO ↔ Entity 双方向マッピング | ⚠️ 実装次第 | ⚠️ 部分的 |
| Protocol Buffers 契約 | ✅ 標準化されたインターフェース定義 | ❌ | ❌ |
| 構造化フィルタエンジン | ✅ 29+ 演算子 + AND/OR ネスト | ❌ | ⚠️ 基本フィルタリング |
| 3つのページネーション戦略 | ✅ Page / Offset / Token | ❌ | ❌ |
| 内蔵キャッシュ | ✅ Cache-Aside + スタンピード保護 | ❌ | ❌ |
| 監査ログ | ✅ フルチェーントレース | ❌ | ❌ |
| データアクセス制御 | ✅ 5段階データスコープ | ❌ | ❌ |
| Upsert サポート | ✅ INSERT ON CONFLICT | ⚠️ 手動実装 | ❌ |
| OLAP エンジンサポート | ✅ ClickHouse + Doris | ❌ | ❌ |
| 検索エンジンサポート | ✅ Elasticsearch + OpenSearch | ❌ | ❌ |
| 時系列 DB サポート | ✅ InfluxDB | ❌ | ❌ |

---

## コントリビュート

Issue や Pull Request を歓迎します。コントリビュート前に以下をご確認ください：

- コードが `go vet` チェックを通過すること
- 新機能に対応するユニットテストがあること
- プロジェクトの既存のコーディング規約に従うこと

## ライセンス

このプロジェクトは [MIT ライセンス](./LICENSE) の下で公開されています。自由に使用、変更、配布できます。
