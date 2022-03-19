clearFunc() {
	DES_FOLDER=/tmp
	for FOLDER in $(ls $DES_FOLDER); do
		#  截取test
		test=$(expr substr $FOLDER 1 4)
		if [ "$test" = "test" ]; then
			$(rm -fr $DES_FOLDER/$FOLDER)
		fi
	done
}
for ((i = 1; i <= 150; i++)); do
	echo "$i"
	check_results=$(make project2c)
	#check_results=$( go test -v -run  TestSnapshotUnreliableRecoverConcurrentPartition2C ./kv/test_raftstore )
	$(go clean -testcache)
	clearFunc
	if [[ $check_results =~ "FAIL" ]]; then
		echo "$check_results" > result.txt
		clearFunc
		break
	fi
done