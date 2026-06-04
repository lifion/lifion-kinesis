# Releasing

This is how a release goes out. Cutting one comes down to a single button in the Actions tab. The only manual gate is the merge into `main`; once that lands, the rest of the pipeline (publish, tag, GitHub release, back-merge) runs on its own.

## The pipeline

1. **Cut Release** (`.github/workflows/release-cut.yml`, manual). Bumps the version on `develop`, regenerates `CHANGELOG.md`, pushes a `release/X.Y.Z` branch, and opens a PR into `main`.
2. **You review and merge that PR into `main`.** This is the gate. Nothing publishes until you merge.
3. **Publish Module** (`.github/workflows/publish-module.yml`, on push to `main`). Re-runs lint and tests, publishes to npm with provenance over OIDC (skips if the version is already published), then tags `vX.Y.Z` and creates the GitHub Release with generated notes.
4. **Back-merge** (`.github/workflows/backmerge.yml`, on push to `main`). Opens a `main` → `develop` PR so the version bump and changelog flow back.

## Cutting a release

1. Go to **Actions → Cut Release → Run workflow**.
2. Pick the **bump** (`major`, `minor`, or `patch`), or fill in **custom_version** for an explicit value like `2.0.0` or `2.0.0-rc.1`. The custom value wins if both are set.
3. Run it. The action checks out `develop`, lints and tests it, runs `npm version <bump> --no-git-tag-version` (which regenerates `CHANGELOG.md` via the `version` npm script), commits as `X.Y.Z`, pushes `release/X.Y.Z`, and opens the PR.

It always cuts from `develop`, whatever ref you launch it from.

## Reviewing the release PR

- Read `CHANGELOG.md`. auto-changelog builds it from commit subjects, so it will not mark breaking changes. Hand-edit the entry to call those out (for the 2.0.0 line, the #332 polling read-error change is breaking: fatal read errors now warn and self-heal instead of surfacing as a stream `'error'` event).
- Confirm the version and the diff look right.

## Merging to `main`

The PR is opened by the Actions token, so GitHub suppresses workflow runs on it (the recursion-prevention rule), which means the required checks will not post on their own. The branch was already linted and tested at cut time, and Publish Module re-validates before it publishes. So either admin-merge, or push an empty commit to the release branch to trigger the matrix first:

```bash
git commit --allow-empty -m "ci: trigger checks" && git push
```

Merge with a regular merge commit. Once it lands on `main`, publish, tag, release, and the back-merge PR all fire on their own.

## After the release

- Merge the back-merge PR (`main` → `develop`) once it is green. It is opened by the token too, so admin-merge applies.
- Leave `support/1.x` alone for a main-line release. It tracks v1 and must not be fast-forwarded to `main` once v2 has shipped.
- Post any "fix is out" notes on the issues the release closed.

## v1 maintenance releases

Patches to the v1 line come off `support/1.x`, not through this action. Bump there, then run **Publish Module** by hand (**Actions → Publish Module → Run workflow**) with `dist_tag` set to `v1` so the publish does not move npm's `latest`.

## Manual fallback

If you need to cut by hand (the action is down, or you want signed commits), the equivalent is:

```bash
git checkout develop && git pull
npm version <major|minor|patch> --no-git-tag-version   # bumps + regenerates CHANGELOG
git checkout -b release/$(node -p "require('./package.json').version")
git commit -am "$(node -p "require('./package.json').version")"
git push -u origin HEAD
gh pr create --base main --title "Release v$(node -p "require('./package.json').version")"
```

Then review, merge into `main`, and let the pipeline take it from there.
