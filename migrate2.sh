#!/bin/bash

for repo in \
  word-net \
  image-net \
  multi-nli \
  snli \
  squad \
  phishing \
  google-landmarks
  do
    dolt clone Liquidata/$repo
    if [ -d "$repo" ]
    then
      pushd "$repo"
      time dolt migrate > "$repo-migrate-time.txt"
      popd
    fi
  done