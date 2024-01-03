#!/bin/sh

git add --all
git commit -m "`date "+%F %T"`"
git push -u origin main