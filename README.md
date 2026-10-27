# it4931
Lưu trữ và xử lý dữ liệu lớn 20261

## Lambda vs Kappa demo

Reproducible laptop-scale benchmark: one e-wallet spending-statistics problem implemented with the Lambda and the Kappa
architecture on identical input, checked against an independent SQL oracle. Spec: `claude_code_prompt_lambda_kappa_demo.md`.

* Design, decisions, pinned versions: `docs/DESIGN.md` · Progress per phase: `docs/PROGRESS.md`
* How to run: `docs/RUNBOOK.md` · Results: `docs/RESULTS.md`

```
shared/  config, rule spec + test vectors, SQL rules, JSON schema      python/  generator, oracle, bench, query-api, tests
jvm/     rules-java, Flink jobs (one Maven reactor)                    spark/   PySpark archiver + batch
infra/   Dockerfiles, Kafka/ClickHouse init, Grafana                   docs/ results/ data/(generated)
```
