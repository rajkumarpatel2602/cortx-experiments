/**
 * Example kv store
 */
#include <cstdio>
#include <benchmark/benchmark.h>

#include <daos.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define BM_KEY_64B      64
#define BM_KEY_128B     128
#define BM_KEY_256B     256
#define BM_KEY_512B     512
#define BM_KEY_1024B    1024


#define BM_1K_BUF   1024
#define BM_4K_BUF   (1024 * 4)
#define BM_8K_BUF   (1024 * 8)
#define BM_16K_BUF  (1024 * 16)
#define BM_32K_BUF  (1024 * 32)


#define KEY_SIZES 5
#define VAL_SIZES 5

#define ITERATION_CNT 100

static char          node[ 128 ] = "new_node";
static daos_handle_t poh;
static daos_handle_t coh;
static int           rank, rankn;
#define FAIL( fmt, ... )                                          \
    do {                                                            \
        fprintf(stderr, "Process (%s): " fmt " aborting\n",     \
                node, ## __VA_ARGS__);                          \
        exit(1);                                                \
    } while (0)

#define ASSERT( cond, ... )                                       \
    do {                                                            \
        if (!(cond))                                            \
        FAIL(__VA_ARGS__);                              \
    } while (0)

enum
{
    OBJ_DKEY,
    OBJ_AKEY
};

#define ENUM_DESC_BUF 512
#define ENUM_DESC_NR  3




#define BUFLEN 100

daos_handle_t oh;
char          rbuf[ BUFLEN ];
daos_obj_id_t oid;
int           i, rc;

uuid_t pool_uuid, co_uuid;


int setup_main( )
{

    /** initialize DAOS by connecting to local agent */
    rc = daos_init( );
    ASSERT( rc == 0, "daos_init failed with %d", rc );

    rc = uuid_parse("b6accbac-ec27-41a8-a8b8-29daf5ba7402", pool_uuid );

    /** Call connect on rank 0 only and broadcast handle to others */
    if ( rank == 0 ) //TODO define rank
    {
        rc = daos_pool_connect( pool_uuid, NULL, DAOS_PC_RW, &poh,
                NULL, NULL );
        ASSERT( rc == 0, "pool connect failed with %d", rc );
    }

    if ( rank == 0 )
    {
        /** generate uuid for container */
        uuid_generate( co_uuid );

        /** create container */
        rc = daos_cont_create( poh, co_uuid, NULL /* properties */,
                NULL /* event */ );
        ASSERT( rc == 0, "container create failed with %d", rc );

        /** open container */
        rc = daos_cont_open( poh, co_uuid, DAOS_COO_RW, &coh, NULL,
                NULL );
        ASSERT( rc == 0, "container open failed with %d", rc );
    }

    /** share container handle with peer tasks */
    //printf( "### KV STORE ###\n" );

    if ( rank == 0 )
        //printf( "Example of DAOS High level KV type:\n" );

    oid.hi = 0;
    oid.lo = 4;

    /** the KV API requires the flat feature flag be set in the oid */
    daos_obj_generate_id( &oid, DAOS_OF_KV_FLAT, OC_SX, 0 );

    rc = daos_kv_open( coh, oid, DAOS_OO_RW, &oh, NULL );
    ASSERT( rc == 0, "KV open failed with %d", rc );


}
void tear_down(){

    rc = daos_cont_close( coh, NULL );
    ASSERT( rc == 0, "cont close failed" );

    rc = daos_pool_disconnect( poh, NULL );
    ASSERT( rc == 0, "disconnect failed" );

    /** teardown the DAOS stack */
    rc = daos_fini( );
    ASSERT( rc == 0, "daos_fini failed with %d", rc );

}



// bnechmark function is getting called here.
static void KV_REMOVE_FUNCTION(benchmark::State& state) {

    /* perform setup */
    setup_main();
    char key_name[10]={0};

    /* allocate key and value buffers */
    char *key_buf=(char *)calloc(state.range(0), 1); // key buffer allocated
    
    /* actual computation starts here */
    for (auto _ : state) {
        
        /* call daos_kv_put for state.range(2) times */
        for (int i=0;i < state.range(2); i++){
            
            state.PauseTiming();

            /* generate different key */
            memset(key_buf,'x', state.range(0)-1);
            sprintf( key_name, "key_%d", i);
            strncpy((char *)key_buf, (char *)key_name, strlen(key_name) );
            
            state.ResumeTiming();
            
            /* actual function to mearsure time */

            daos_kv_remove( oh, DAOS_TX_NONE, 0, key_buf, NULL );
        }
    }
   
    /* free resources */
    free(key_buf);

    /* tear down */
    tear_down();
}



// bnechmark function is getting called here.
static void KV_PUT_FUNCTION(benchmark::State& state) {

    /* perform setup */
    setup_main();
    char key_name[10]={0};

    /* allocate key and value buffers */
    char *key_buf=(char *)calloc(state.range(0), sizeof(char)); // key buffer allocated
    char *val_buf=(char *)calloc(state.range(1), sizeof(char)); // value buffer allocated
    memset(val_buf, 'z', state.range(1)-1); // populate with some random value.

    /* actual computation starts here */
    for (auto _ : state) {
        
        /* call daos_kv_put for state.range(2) times */
        for (int i=0;i < state.range(2); i++){
            
            state.PauseTiming();

            /* generate different key */
            memset(key_buf,'x', state.range(0)-1);
            sprintf( key_name, "key_%d", i);
            strncpy((char *)key_buf, (char *)key_name, strlen(key_name) );
            
            state.ResumeTiming();
            
            /* actual function to mearsure time */
            daos_kv_put( oh, DAOS_TX_NONE, 0, (char *)key_buf, state.range(1), val_buf, NULL );

        }
    }

    //snippet to verify authenticity of last key-value storage.
#if 0
    char rbuf[state.range(1)]={0};
    daos_size_t size=0;
    rc = daos_kv_get( oh, DAOS_TX_NONE, 0, key_buf, &size, NULL, NULL );
    ASSERT( rc == 0, "KV get failed with %d", rc );

    rc = daos_kv_get( oh, DAOS_TX_NONE, 0, key_buf, &size, rbuf, NULL );
    ASSERT( rc == 0, "KV get failed with %d", rc );
    printf("rebuf for keybuf %s is is : %s\n", key_buf, rbuf);
#endif   

    /* free resources */
    free((char *)key_buf);
    free((char *)val_buf);

    /* tear down */
    tear_down();
}




// Register the function as a benchmark
BENCHMARK(KV_PUT_FUNCTION)
    ->Args({BM_KEY_64B , BM_1K_BUF, ITERATION_CNT}) 
    ->Args({BM_KEY_64B , BM_4K_BUF, ITERATION_CNT})     
    ->Args({BM_KEY_64B , BM_8K_BUF, ITERATION_CNT})     
    ->Args({BM_KEY_64B , BM_16K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_64B , BM_32K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_128B , BM_1K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_128B , BM_4K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_128B , BM_8K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_128B , BM_16K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_128B , BM_32K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_256B , BM_1K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_256B , BM_4K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_256B , BM_8K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_256B , BM_16K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_256B , BM_32K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_512B , BM_1K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_512B , BM_4K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_512B , BM_8K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_512B , BM_16K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_512B , BM_32K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_1024B , BM_1K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_1024B , BM_4K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_1024B , BM_8K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_1024B , BM_16K_BUF, ITERATION_CNT})  
    ->Args({BM_KEY_1024B , BM_32K_BUF, ITERATION_CNT})  
    -> Iterations(1)    
    ->Unit(benchmark::kMillisecond); 

BENCHMARK(KV_REMOVE_FUNCTION)
    ->Args({BM_KEY_64B , BM_1K_BUF, ITERATION_CNT}) 
    ->Args({BM_KEY_64B , BM_4K_BUF, ITERATION_CNT})     
    ->Args({BM_KEY_64B , BM_8K_BUF, ITERATION_CNT})     
    ->Args({BM_KEY_64B , BM_16K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_64B , BM_32K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_128B , BM_1K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_128B , BM_4K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_128B , BM_8K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_128B , BM_16K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_128B , BM_32K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_256B , BM_1K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_256B , BM_4K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_256B , BM_8K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_256B , BM_16K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_256B , BM_32K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_512B , BM_1K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_512B , BM_4K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_512B , BM_8K_BUF, ITERATION_CNT})    
    ->Args({BM_KEY_512B , BM_16K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_512B , BM_32K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_1024B , BM_1K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_1024B , BM_4K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_1024B , BM_8K_BUF, ITERATION_CNT})   
    ->Args({BM_KEY_1024B , BM_16K_BUF, ITERATION_CNT})  
    ->Args({BM_KEY_1024B , BM_32K_BUF, ITERATION_CNT})  
    -> Iterations(1)    
    ->Unit(benchmark::kMillisecond);    
// // Run the benchmark

BENCHMARK_MAIN();
