# Changelog

## [0.0.2](https://github.com/cloudquery/plugin-sdk-java/compare/v0.0.1...v0.0.2) (2023-08-24)


### Bug Fixes

* Package group ([#96](https://github.com/cloudquery/plugin-sdk-java/issues/96)) ([274339d](https://github.com/cloudquery/plugin-sdk-java/commit/274339d74c3b9928ddbf1efd0429d425ae1ad268))

## 0.0.1 (2023-08-24)


### Features

* `io.cloudquery.scalar.Binary` implementation ([#20](https://github.com/cloudquery/plugin-sdk-java/issues/20)) ([b9b73d1](https://github.com/cloudquery/plugin-sdk-java/commit/b9b73d1d577d8daddb0b705bb53c5827df0ac7b7))
* `io.cloudquery.scalar.Bool` ([#27](https://github.com/cloudquery/plugin-sdk-java/issues/27)) ([2a91c92](https://github.com/cloudquery/plugin-sdk-java/commit/2a91c92bcf74057bca0877f1ba7c5538ba7c4f5a)), closes [#26](https://github.com/cloudquery/plugin-sdk-java/issues/26)
* adding basic support for tables ([#19](https://github.com/cloudquery/plugin-sdk-java/issues/19)) ([22b2350](https://github.com/cloudquery/plugin-sdk-java/commit/22b235093539f77d3591a7edccdbf22f7335ad6b))
* adding JSON scalar ([#82](https://github.com/cloudquery/plugin-sdk-java/issues/82)) ([fc92542](https://github.com/cloudquery/plugin-sdk-java/commit/fc92542cb402d1ac0241aa781847eb8f2d211f87)), closes [#63](https://github.com/cloudquery/plugin-sdk-java/issues/63)
* adding Table filterDFS functionaility ([#21](https://github.com/cloudquery/plugin-sdk-java/issues/21)) ([02d8515](https://github.com/cloudquery/plugin-sdk-java/commit/02d85152da9731f021d58abf5f6575f87374e7f6))
* Date scalars ([#36](https://github.com/cloudquery/plugin-sdk-java/issues/36)) ([adc6ba2](https://github.com/cloudquery/plugin-sdk-java/commit/adc6ba2c76bd257cda99b0e0e7c2e17c95e1968e)), closes [#34](https://github.com/cloudquery/plugin-sdk-java/issues/34)
* Duration scalar ([#42](https://github.com/cloudquery/plugin-sdk-java/issues/42)) ([7529438](https://github.com/cloudquery/plugin-sdk-java/commit/7529438472b2a537940c68276fcd3f2348710f85)), closes [#39](https://github.com/cloudquery/plugin-sdk-java/issues/39)
* Encode resources with data ([#88](https://github.com/cloudquery/plugin-sdk-java/issues/88)) ([2c7060f](https://github.com/cloudquery/plugin-sdk-java/commit/2c7060f9d75d0159334b57256a86778499303743))
* Generics in scalars ([#56](https://github.com/cloudquery/plugin-sdk-java/issues/56)) ([bc7d6e3](https://github.com/cloudquery/plugin-sdk-java/commit/bc7d6e390c89c46c544514f27168c057806797f9))
* Implement `getTables` ([#71](https://github.com/cloudquery/plugin-sdk-java/issues/71)) ([085c51f](https://github.com/cloudquery/plugin-sdk-java/commit/085c51f4792a24a121079073cfeaf984f422a681))
* Implement concurrency and relations resolving ([#91](https://github.com/cloudquery/plugin-sdk-java/issues/91)) ([0a470b7](https://github.com/cloudquery/plugin-sdk-java/commit/0a470b7277fb2aed0c306022e6a11f45147cb5c6))
* Init logger, add initial MemDB plugin ([#70](https://github.com/cloudquery/plugin-sdk-java/issues/70)) ([20ebb42](https://github.com/cloudquery/plugin-sdk-java/commit/20ebb422ccf2a56a02a90d50d7f03d131ff99d01))
* int/uint/float/string scalars ([#59](https://github.com/cloudquery/plugin-sdk-java/issues/59)) ([39ec6e6](https://github.com/cloudquery/plugin-sdk-java/commit/39ec6e69383629b1bd9ec80274bd05f271afb99e)), closes [#53](https://github.com/cloudquery/plugin-sdk-java/issues/53) [#54](https://github.com/cloudquery/plugin-sdk-java/issues/54) [#58](https://github.com/cloudquery/plugin-sdk-java/issues/58) [#60](https://github.com/cloudquery/plugin-sdk-java/issues/60)
* Resolve CQId, add CQIds to MemDB plugin ([#95](https://github.com/cloudquery/plugin-sdk-java/issues/95)) ([9d7f1bd](https://github.com/cloudquery/plugin-sdk-java/commit/9d7f1bd1bfee2ba3fa3fb25baf519a77d0961f44))
* Scalar Timestamp ([#46](https://github.com/cloudquery/plugin-sdk-java/issues/46)) ([4220e92](https://github.com/cloudquery/plugin-sdk-java/commit/4220e9295b9def7ffd8154a3706ae152c12427a7)), closes [#44](https://github.com/cloudquery/plugin-sdk-java/issues/44)
* **sync:** Initial insert message support ([#81](https://github.com/cloudquery/plugin-sdk-java/issues/81)) ([bd729bb](https://github.com/cloudquery/plugin-sdk-java/commit/bd729bbb7c18a9767f238027d38cc76f956c9eec))
* **sync:** Send migrate messages ([#79](https://github.com/cloudquery/plugin-sdk-java/issues/79)) ([dd2c1a5](https://github.com/cloudquery/plugin-sdk-java/commit/dd2c1a5c590b8d828f12d3b95d6b58b2ef095bef))


### Bug Fixes

* Add `jackson-annotations` to `build.gradle` ([#83](https://github.com/cloudquery/plugin-sdk-java/issues/83)) ([ead7dd9](https://github.com/cloudquery/plugin-sdk-java/commit/ead7dd90e52f4b6277d1e4c1410aeab331451f96))
* **deps:** Update dependency com.google.guava:guava to v32 ([#15](https://github.com/cloudquery/plugin-sdk-java/issues/15)) ([ce8028b](https://github.com/cloudquery/plugin-sdk-java/commit/ce8028b72d3e0d6dcf1733481d3115c6c6cf0b54))
* **deps:** Update dependency io.grpc:grpc-protobuf to v1.57.1 ([#10](https://github.com/cloudquery/plugin-sdk-java/issues/10)) ([bcfa29c](https://github.com/cloudquery/plugin-sdk-java/commit/bcfa29c0161030b8c01da8e22ab2ba08fee76d52))
* **deps:** Update dependency io.grpc:grpc-services to v1.57.1 ([#11](https://github.com/cloudquery/plugin-sdk-java/issues/11)) ([71c2ea1](https://github.com/cloudquery/plugin-sdk-java/commit/71c2ea1e97d9749731e90cec726cd2389c5606b2))
* **deps:** Update dependency io.grpc:grpc-stub to v1.57.1 ([#12](https://github.com/cloudquery/plugin-sdk-java/issues/12)) ([c65e5d6](https://github.com/cloudquery/plugin-sdk-java/commit/c65e5d631962572b5d85a3dbb001af502ef9d7e7))
* **deps:** Update dependency io.grpc:grpc-testing to v1.57.1 ([#13](https://github.com/cloudquery/plugin-sdk-java/issues/13)) ([a7b1fa6](https://github.com/cloudquery/plugin-sdk-java/commit/a7b1fa65d901237319ff045623fc46ec9725a485))
* **deps:** Update plugin org.gradle.toolchains.foojay-resolver-convention to v0.6.0 ([#14](https://github.com/cloudquery/plugin-sdk-java/issues/14)) ([443990c](https://github.com/cloudquery/plugin-sdk-java/commit/443990c7b46be5cf00bd4b65d5f545b6b441c9ec))
* Flatten tables in getTables gRPC call ([#80](https://github.com/cloudquery/plugin-sdk-java/issues/80)) ([8c9872a](https://github.com/cloudquery/plugin-sdk-java/commit/8c9872a463b689b9ddb57c520bdee488ce7fdf72))
* Pass options to tables method ([#78](https://github.com/cloudquery/plugin-sdk-java/issues/78)) ([4b77a2f](https://github.com/cloudquery/plugin-sdk-java/commit/4b77a2fd29da9b81346303588f741b42a828e988))


### Miscellaneous Chores

* Release 0.0.1 ([e169dbc](https://github.com/cloudquery/plugin-sdk-java/commit/e169dbcd0745390a27778da4ae1789ef0649da5d))
