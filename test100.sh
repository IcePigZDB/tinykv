#/bin/bash
for ((i = 1; i <= 100; i++)); do
	# check_results=$(make project2b)
	check_results=$( go test -v -run TestManyPartitionsOneClient2B ./kv/test_raftstore )
	$(go clean -testcache)
	rm -r /tinykvTmp
	if [[ $check_results =~ "FAIL" ]]; then
		echo "$check_results" > result.txt
		rm -r /tinykvTmp
		break
	fi
done
