# Security Policy

## Supported Versions

The latest published minor is supported. Older versions may receive backports for high-severity issues on a best-effort basis.

## Reporting a Vulnerability

Please use [GitHub's private security advisory flow](https://github.com/marcinnurbanski/fake-redis/security/advisories/new) rather than opening a public issue. We aim to acknowledge reports within 72 hours.

## Scope

`fake-redis` is a testing utility. It is **not** intended for production use or to hold real secrets. Reports of memory disclosure between isolated test runs, RCE via command dispatch, or similar are in scope. Denial-of-service from adversarial client input is generally out of scope but still welcome.
