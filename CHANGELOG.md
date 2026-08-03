# CHANGELOG


## [1.2.1](https://github.com/slother/arris-exporter/compare/v1.2.0...v1.2.1) (2026-08-03)


### Bug Fixes

* emit one series per channel to stop duplicate-series warnings ([#25](https://github.com/slother/arris-exporter/issues/25)) ([5c3acb0](https://github.com/slother/arris-exporter/commit/5c3acb0445a2af18ac1bf2005851449fd7e5aece))

## [1.2.0](https://github.com/slother/arris-exporter/compare/v1.1.0...v1.2.0) (2026-08-03)


### Features

* add Grafana dashboard and screenshots ([#10](https://github.com/slother/arris-exporter/issues/10)) ([0a436bd](https://github.com/slother/arris-exporter/commit/0a436bd479e995f393e07a1ece7cf665f99596ce))
* add semantic versioning with python-semantic-release ([c6bb230](https://github.com/slother/arris-exporter/commit/c6bb23030168abae7f3a6651e58a8f3511e60834))
* configurable log level, release-please, shared CI workflows ([#22](https://github.com/slother/arris-exporter/issues/22)) ([e70d035](https://github.com/slother/arris-exporter/commit/e70d03519a878a7d56a93e721252375293520652))


### Bug Fixes

* add HTTP retry with backoff to prevent scrape gaps ([#12](https://github.com/slother/arris-exporter/issues/12)) ([cef4be0](https://github.com/slother/arris-exporter/commit/cef4be0fa8078ad2e86a3e0c9c3e9ea154b7ade1))
* prevent duplicate samples for no-label metrics ([#17](https://github.com/slother/arris-exporter/issues/17)) ([4a7d60c](https://github.com/slother/arris-exporter/commit/4a7d60c77c38d57bf3e504b9203b59bcc776dee1))

## [1.1.0](https://github.com/slother/arris-exporter/compare/v1.0.0...v1.1.0) (2026-08-03)


### Features

* configurable log level, release-please, shared CI workflows ([#22](https://github.com/slother/arris-exporter/issues/22)) ([e70d035](https://github.com/slother/arris-exporter/commit/e70d03519a878a7d56a93e721252375293520652))

## v0.1.0 (2026-04-01)

### Continuous Integration

- Bump actions/checkout from 4 to 6
  ([`9d203ac`](https://github.com/slother/arris-exporter/commit/9d203acc88ff657d0455110fedf3d1b30dd45732))

Bumps [actions/checkout](https://github.com/actions/checkout) from 4 to 6. - [Release
  notes](https://github.com/actions/checkout/releases) -
  [Changelog](https://github.com/actions/checkout/blob/main/CHANGELOG.md) -
  [Commits](https://github.com/actions/checkout/compare/v4...v6)

--- updated-dependencies: - dependency-name: actions/checkout dependency-version: '6'

dependency-type: direct:production

update-type: version-update:semver-major ...

Signed-off-by: dependabot[bot] <support@github.com>

- Bump actions/setup-python from 5 to 6
  ([`0a7f562`](https://github.com/slother/arris-exporter/commit/0a7f56236a08a08cd9c1057bf11be218d67a9bed))

Bumps [actions/setup-python](https://github.com/actions/setup-python) from 5 to 6. - [Release
  notes](https://github.com/actions/setup-python/releases) -
  [Commits](https://github.com/actions/setup-python/compare/v5...v6)

--- updated-dependencies: - dependency-name: actions/setup-python dependency-version: '6'

dependency-type: direct:production

update-type: version-update:semver-major ...

Signed-off-by: dependabot[bot] <support@github.com>

- Bump docker/build-push-action from 6 to 7
  ([`72ea584`](https://github.com/slother/arris-exporter/commit/72ea58409d1ad8643fd9c79ef439dbb8ab276981))

Bumps [docker/build-push-action](https://github.com/docker/build-push-action) from 6 to 7. -
  [Release notes](https://github.com/docker/build-push-action/releases) -
  [Commits](https://github.com/docker/build-push-action/compare/v6...v7)

--- updated-dependencies: - dependency-name: docker/build-push-action dependency-version: '7'

dependency-type: direct:production

update-type: version-update:semver-major ...

Signed-off-by: dependabot[bot] <support@github.com>

- Bump docker/login-action from 3 to 4
  ([`c71f2e3`](https://github.com/slother/arris-exporter/commit/c71f2e3f6bd0df0f789fbef042c32c609f7848e8))

Bumps [docker/login-action](https://github.com/docker/login-action) from 3 to 4. - [Release
  notes](https://github.com/docker/login-action/releases) -
  [Commits](https://github.com/docker/login-action/compare/v3...v4)

--- updated-dependencies: - dependency-name: docker/login-action dependency-version: '4'

dependency-type: direct:production

update-type: version-update:semver-major ...

Signed-off-by: dependabot[bot] <support@github.com>

- Bump docker/setup-buildx-action from 3 to 4
  ([`d9ff1ab`](https://github.com/slother/arris-exporter/commit/d9ff1ab4f5447d062dd568d84b88a0e0fe11a0b9))

Bumps [docker/setup-buildx-action](https://github.com/docker/setup-buildx-action) from 3 to 4. -
  [Release notes](https://github.com/docker/setup-buildx-action/releases) -
  [Commits](https://github.com/docker/setup-buildx-action/compare/v3...v4)

--- updated-dependencies: - dependency-name: docker/setup-buildx-action dependency-version: '4'

dependency-type: direct:production

update-type: version-update:semver-major ...

Signed-off-by: dependabot[bot] <support@github.com>

### Features

- Add semantic versioning with python-semantic-release
  ([`c6bb230`](https://github.com/slother/arris-exporter/commit/c6bb23030168abae7f3a6651e58a8f3511e60834))

Conventional commits (feat:/fix:/feat!:) trigger automatic version bumps, git tags, GitHub releases,
  and versioned Docker image pushes.
