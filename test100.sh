# TODO 1 2B
# TODO 2 3B

for ((i = 1; i <= 100; i++)); do
 echo "$i"
 # check_results=$(make project2b)
 check_results=$( go test -v -run  TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B ./kv/test_raftstore)
 $(go clean -testcache)
  rm -r /tinykvTmp/*
 if [[ $check_results =~ "FAIL" ]]; then
  echo "$check_results" > result.txt
  rm -r /tinykvTmp/*
  break
 fi
done