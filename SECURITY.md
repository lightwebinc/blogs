# Security

Please report a vulnerability privately through GitHub: the repository's
**Security** tab, **Report a vulnerability**. Do not open a public issue for
it.

A report is most useful with the version (`blogs -version`), what you ran,
what you expected, and what happened instead. We aim to acknowledge within
three working days.

In scope:

- anything that lets `read`, `verify-interval` or `archive` report a batch
  or an interval as verified when it is not the producer's, not at the
  position claimed, or not committed by a mined anchor;
- anything that lets a host admit or serve a transaction the rules in
  [docs/spec.md](docs/spec.md) refuse, or keep serving a retracted batch;
- anything that lets a host's answer reach a terminal unfiltered, or
  exhaust a reader's memory;
- anything that leaks a producer's keys or spends its coin pool other than
  as the producer asked;
- the images and the host module bundle.
