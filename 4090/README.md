# RTX 4090 Compatibility Launchers

The extractor now has one shared implementation in the repository root. Files
in this directory only preserve the older commands while selecting the `4090`
hardware profile.

```powershell
python 4090/main.py status
python 4090/main.py run --all
python 4090/review.py --limit 20
```

The preferred equivalent is:

```powershell
python main.py run --profile 4090 --all
```

Both forms use the root `data/` checkpoint and output directories.

The `4090` profile uses seven CPU parser workers feeding one shared GPU model
with candidate/inference/encoding batches of `150`/`1600`/`256`. Run
`scripts\benchmark.ps1 -Profile 4090` on that PC to create its ignored local
autotuning override.
