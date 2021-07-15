# TODO 1 2B
# TODO 2 3B

for ((i = 1; i <= 100; i++)); do
 echo "$i"
 # run check project or function,rember change to your test target
 check_results=$( go test -v -run  TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B ./kv/test_raftstore)
 # clean test cache
 $(go clean -testcache)
  # delete badger tmp file,rember change to your dir
  rm -r /tinykvTmp/*
 if [[ $check_results =~ "FAIL" ]]; then
  echo "$check_results" > result.txt
  rm -r /tinykvTmp/*
  break
 fi
done