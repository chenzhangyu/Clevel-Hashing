#include "unittest.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <iterator>
#include <thread>
#include <vector>
#include <cstdio>
#include <sstream>

#include "../../examples/libpmemobj_cpp_examples_common.hpp"
#include <libpmemobj++/experimental/cceh.hpp>

#define LAYOUT "CCEH"
#define KEY_LEN 15
#define VALUE_LEN 16

namespace nvobj = pmem::obj;

namespace
{

typedef nvobj::experimental::CCEH
	persistent_map_type;

struct root {
	nvobj::persistent_ptr<persistent_map_type> cons;
};

} /* Annoymous namespace */

int
main(int argc, char *argv[])
{
	START();

	// parse inputs
	if (argc != 3) {
		printf("usage: %s <pool_path> <load_file>\n\n", argv[0]);
		printf("    pool_path: the pool file required for PMDK\n");
		printf("    load_file: an insert-only workload file\n");
		exit(1);
	}

	const char *path = argv[1];

	nvobj::pool<root> pop;
	remove(path); // delete the mapped file.

    pop = nvobj::pool<root>::create(
        path, LAYOUT, PMEMOBJ_MIN_POOL * 2048, S_IWUSR | S_IRUSR);
    auto proot = pop.root();

    {
        nvobj::transaction::manual tx(pop);

        proot->cons = nvobj::make_persistent<persistent_map_type>(2U);

        nvobj::transaction::commit();
    }

    auto map = proot->cons;
	printf("initial capacity: %ld\n", map->Capacity());

	FILE *ycsb,*fout;
	uint8_t key[KEY_LEN];
	char buf[1024];
	char *pbuf = buf;
	size_t len = 1024;

	if ((ycsb = fopen(argv[2], "r")) == nullptr)
	{
		printf("failed to read %s\n", argv[2]);
		exit(1);
	}
	fout = fopen("cceh_exp2_load_factor.csv", "w");

	fprintf(fout, "inserted,capacity,load_factor\n");
    // start benchmark
	size_t inserted = 0;
	while (getline(&pbuf, &len, ycsb) != -1)
	{
		if (strncmp(buf, "INSERT", 6) == 0)
		{
			memcpy(key, buf + 7, KEY_LEN - 1);

			auto r = map->insert(key, key, KEY_LEN, VALUE_LEN, 0);
			if (r.found)
			{
				inserted++;

				if (inserted % 10000 == 0)
					{
						std::cout << "Load factor: "
							<< (inserted - 1) * 1.0 / map->Capacity()
							<< " inserted: " << inserted
							<< " capacity: " << map->Capacity() << std::endl;
						fprintf(fout, "%ld,%ld,%f\n", inserted, map->Capacity(),
							(inserted - 1) * 1.0 / map->Capacity());
					}

			}
			else
			{
				printf("Error for insertion\n");
				exit(1);
			}
		}
	}
	fclose(fout);
	fclose(ycsb);

    printf("Capacity after insertion: %ld\n", map->Capacity());
	std::cout << inserted << " items are inserted" << std::endl;


	pop.close();

	return 0;
}