# Pinned UN M49 snapshot

This backend owns its data. Building, publishing, Docker builds and service startup
do not read the Android repository and never fetch geographic data from the network.

- `botgram-m49.json`: embedded runtime data; World plus 29 selectable geographic regions and 249 ISO alpha-2 mappings.
- `botgram-m49.sha256`: build-time pin; MSBuild fails if the bundled file differs.
- `manifest.json`: application snapshot version, official URL, retrieval date, source/policy/data SHA-256 values and counts. The version is a retrieval snapshot label, not a UN release number.
- `source.json`: normalized facts extracted from the official English download table. The original HTML is cached outside the repository on D:; its hash is recorded in the manifest.
- `policy.json`: the explicit allowed geographic region set, Chinese labels, display abbreviations and country overrides. World is reserved for registration/migration fallback; all other regions are selectable. Country M49 codes are not GID prefixes.

Official source: https://unstats.un.org/unsd/methodology/m49/overview/

The captured official table has 248 country/area ISO mappings. The explicit compatibility
rule `TW → 030` preserves the application's existing behavior; it is not presented as
a separate row from that official table. Unknown ISO codes fall back to World `001`.
Names in Chinese and letter abbreviations are maintained by the application policy.

Validate offline (Node.js 22+; Node is not required to build or run the backend):

```sh
node tests/m49.mjs
```

To deliberately update from the official network source:

```sh
node tools/update-m49.mjs --version YYYY-MM-DD
node tests/m49.mjs
```

Use the actual retrieval date. Downloads default to `D:\BotgramBuild\downloads` on
Windows; `--cache-dir` must remain on D: on this computer. On other platforms the
default is an OS temporary cache directory. If a proxy is needed, download with a
proxy-capable tool first, then import the downloaded snapshot explicitly:

```powershell
curl.exe -L --fail https://unstats.un.org/unsd/methodology/m49/overview/ -o D:\BotgramBuild\downloads\un-m49-overview.html
node tools/update-m49.mjs --version YYYY-MM-DD --html D:\BotgramBuild\downloads\un-m49-overview.html
node tests/m49.mjs
```

The updater rejects a changed official region set, conflicting overrides or changed
region names until the policy is explicitly reviewed. It updates local source/data/
manifest/checksum files only; it is not invoked by compilation, registration or startup.
Review and commit the changed snapshot and pins together. If synchronizing the Android
repository, copy `botgram-m49.json` to its `Shared/` directory and `manifest.json` to
`Shared/botgram-m49.manifest.json`, then run its `Tools/Verify-BotgramM49.ps1`. Both
repositories must carry the same snapshot version and data SHA-256; neither build reads
the other repository. Backend-only builds and deployments need no Android checkout.
