# msgproc [![Rust](https://github.com/shorii/msgproc/actions/workflows/rust.yml/badge.svg?branch=main)](https://github.com/shorii/msgproc/actions/workflows/rust.yml) [![codecov](https://codecov.io/gh/shorii/msgproc/branch/main/graph/badge.svg?token=GE538GMH70)](https://codecov.io/gh/shorii/msgproc)

## Prerequisite

To compile librdkafka, the following packages are needed.

```sh
$ sudo apt-get install libpthread-stubs0-dev \
> make \
> build-essential \
> zlib1g-dev \ #optional
> cmake \ #optional
> libssl-dev \ #optional
> libsasl2-dev \ #optional
> libzstd-dev #optional
```

## Documentation

* [API Documentation](https://shorii.github.io/msgproc/msgproc/)

## Build

```sh
$ cargo build
```

## Test

```sh
$ cargo test
```
