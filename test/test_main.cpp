
#include <stdio.h>
#include <stdlib.h>
#include "gtest/gtest.h"

int main(int argc, char **argv)
{
        ::testing::InitGoogleTest(&argc, argv);
        system("rm -rf out/test_out");    // Hey, it's a test, chill out...
        system("mkdir -p out/test_out");
        return RUN_ALL_TESTS();
}

