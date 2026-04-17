# Changesets

Each PR that changes a published package must include a changeset. Run:

```bash
pnpm changeset
```

and commit the generated markdown file. The release bot uses these to bump versions and write CHANGELOG.md entries.
