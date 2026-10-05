# Changesets

Run `vp run changeset` for consumer-visible changes. All six public packages share
one fixed group and one beta version. Run `vp run changeset status` to inspect the
pending release plan and `vp run changeset:version` to advance the group together.
See the release guide in `docs/src/content/docs/RELEASE.md` for publication.

Pending changesets live directly in this directory. Changesets moves consumed
beta release notes to `pre/` and keeps only the prerelease mode and tag in
`pre.json`. Retain the archived notes for the eventual stable release.
