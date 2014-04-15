#!/bin/bash
#svn ci -m "memory linking bugfix"
while [ 1 ]
do
  svn ci -m "$1"
  a=$?
  if [ $a -eq 0 ];then
    echo svn ci ok $a
    break
  else
    echo $a retry svn -m "\"$1\""
  fi
done
