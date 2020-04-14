#!/bin/bash

for repo in \
  stock-tickers \
  ip-to-country \
  corona-virus \
  corona-virus-state-action \
  bad-words \
  coin-metrics-data \
  us-congress \
  common-crawl-index-stats \
  metabiota-epidemic-data-demo \
  open-flights \
  cannabis-testing-wa \
  coin-metrics-daily-data \
  irs-soi \
  ca-license-plate-reviews \
  github-typos \
  us-baby-names \
  world-happiness-report \
  usda-all-foods \
  dolt-sqllogictest-results \
  nba \
  powerlifting \
  english-homophones \
  google-image-labels-to-word-net \
  marijuana-strains \
  million-songs \
  crackstation-password-dictionary \
  classified-iris-measurements \
  units-of-measure \
  jeopardy \
  stock-market
  do
    dolt clone Liquidata/$repo
    if [ -d "$repo" ]
    then
      pushd "$repo"
      time dolt migrate > "$repo-migrate-time.txt"
      popd
    fi
  done