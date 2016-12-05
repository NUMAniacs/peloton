numa_alloc_cost.cpp
	Test the cost of malloc and numa_alloc.

	When executing 1M times allocating of 1024 integers (each time allocate 4KB and 4GB in total), malloc costs 1662ms and numa_alloc costs 3089ms. The ratio is about 1.86x. When freeing this memory, free costs 986ms and numa_free costs 1097ms. The ratio is about 1.11x.


numa_alloc_on_nodes.cpp
	Test single thread randomly reading 512MB memory from local node or remote node.

	When reading from local node, it costs 1507ms.
	When reading from remote node, it costs 3692ms.

	The ratio is about 2.45x.

numa_alloc_thread.cpp
	Test multiple threads randomly reading 512MB memory for 10s.

	Total count is scaled down 1000 times. (1 in total count represents 1000 reads)

	Ratio is total_count / time.

	// thread running local_work() // read memory from local node
	// 1 thread
	time: 10000.000000, total count: 756552, ratio: 75.655200
	// 2 threads
	time: 10000.000000, total count: 1563416, ratio: 156.341600
	// 12 threads
	time: 10000.000000, total count: 8599710, ratio: 859.971000
	// 14 threads
	time: 10000.000000, total count: 10005989, ratio: 1000.598900
	// 18 threads
	time: 10000.000000, total count: 12575281, ratio: 1257.528100
	// 23 threads
	time: 10000.000000, total count: 11570858, ratio: 1157.085800
	// 24 threads
	time: 10000.000000, total count: 9322305, ratio: 932.230500

	// thread running remote_work() // read memory from remote note
	// 1 thread
	time: 10000.000000, total count: 426001, ratio: 42.600100
	// 2 threads
	time: 10000.000000, total count: 845079, ratio: 84.507900
	// 12 threads
	time: 10000.000000, total count: 4753722, ratio: 475.372200
	// 14 threads
	time: 10000.000000, total count: 5473118, ratio: 547.311800
	// 18 threads
	time: 10000.000000, total count: 7159255, ratio: 715.925500
	// 23 threads
	time: 10000.000000, total count: 5880617, ratio: 588.061700
	// 24 threads
	time: 10000.000000, total count: 6093335, ratio: 609.333500