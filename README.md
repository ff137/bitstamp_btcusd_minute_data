# Bitstamp BTC/USD 1-minute OHLC Data

> Forked from [mczielinski/kaggle-bitcoin](https://github.com/mczielinski/kaggle-bitcoin)

## Project Overview

This repository provides up-to-date Bitcoin (BTC/USD) historical 1-minute OHLC data from Bitstamp.

Bulk historical data is saved in `data/historical/btcusd_bitstamp_1min_2012-2025.csv.gz`.

A daily GitHub action runs at midnight UTC to fetch the latest data and append it to a daily update file.

The daily updates (since the bulk data) are saved in `data/recent/btcusd_bitstamp_1min_latest.csv`.

See [scripts/README.md](scripts/README.md) for steps on how this repository was onboarded.

We hope this repository makes your life easier!
