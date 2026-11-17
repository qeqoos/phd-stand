# corpus — збір provenance npm (P2, RQ1–RQ2)

`collect_provenance.py` — лише stdlib Python ≥3.9.

```sh
python3 corpus/collect_provenance.py --top 50 --fetch-workflow       # пілот
python3 corpus/collect_provenance.py --top 5000 --fetch-workflow     # повний корпус
python3 corpus/collect_provenance.py --list pkgs.txt                 # свій список
```

Рамка: `npm-high-impact` (`top-download`, версія фіксується в `summary.json`).
Потрібен доступ до `registry.npmjs.org` і (для `--fetch-workflow`) `raw.githubusercontent.com`.

Вихід у `results/corpus-<дата>-<джерело>-n<N>/`:

- `packages.csv` / `.jsonl` — рядок на пакет (repo, commit, workflow path/ref, buildType, builder, runner_environment, resolvedDependencies);
- `summary.json` — частки й розподіли;
- `attestations/` — сирі відповіді реєстру (для відтворюваності);
- `workflows/` — workflow-файл на коміті з provenance (вхід для `analyzer/`).

Підтримує provenance SLSA v1 і v0.2 (старі релізи npm CLI).
