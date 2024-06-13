#include "clevel.h"
#include "clevel_filter.h"
#include "clevel_single_filter.h"
#include "generator.h"
#include "plush.h"
#include "plush_single_filter.h"
#include "race.h"
#include "sephash.h"
#include "sephash_zip.h"
#include "split_batch.h"
#include "split_hash.h"
#include "split_hash_idle.h"
#include "split_inline_dep.h"
#include "split_search_base.h"
#include "split_search_fptable.h"
#include "split_search_fptable_wocache.h"
#include <set>
#include <stdint.h>
#include <iostream>
#define ORDERED_INSERT
Config config;
uint64_t load_num;
using ClientType = SEPHASH::Client;
using ServerType = SEPHASH::Server;
using Slice = SEPHASH::Slice;
const uint64_t MAX_LOAD = 1e8;
const uint64_t MAX_RUN = 1e8;

inline uint64_t GenKey(uint64_t key)
{
#ifdef ORDERED_INSERT
    return key;
#else
    return FNVHash64(key);
#endif
}


/**
 * cli: 客户端
 * cli_id: 客户端编号
 * coro_id: 协程编号
*/
template <class Client>
    requires KVTrait<Client, Slice *, Slice *>
task<> load(Client *cli, uint64_t cli_id, uint64_t coro_id, std::vector<uint64_t> & ycsb_load_key)
{
    co_await cli->start(config.num_machine * config.num_cli * config.num_coro);
    uint64_t tmp_key;
    Slice key, value;
    std::string tmp_value = std::string(32, '1');
    value.len = tmp_value.length();
    value.data = (char *)tmp_value.data();
    key.len = sizeof(uint64_t);
    key.data = (char *)&tmp_key;
    // load_num是全局设置的num
    uint64_t num_op = load_num / (config.num_machine * config.num_cli * config.num_coro);
    if(cli_id == 0 && coro_id == 0) {
        printf("Load OP per Coro is %d\n", num_op);
    }
    for (uint64_t i = 0; i < num_op; i++)
    {
        tmp_key = ycsb_load_key[(cli_id * config.num_coro + coro_id)*num_op + i];
        co_await cli->insert(&key, &value);
    }
    co_await cli->stop();
    co_return;
}

template <class Client>
    requires KVTrait<Client, Slice *, Slice *>
task<> run(Client *cli, uint64_t cli_id, uint64_t coro_id, char * ycsb_run_method, std::vector<uint64_t> & ycsb_run_key)
{
    co_await cli->start(config.num_machine * config.num_cli * config.num_coro);
    uint64_t tmp_key;
    char buffer[1024];
    Slice key, value, ret_value, update_value;

    ret_value.data = buffer;

    std::string tmp_value = std::string(32, '1');
    value.len = tmp_value.length();
    value.data = (char *)tmp_value.data();

    std::string tmp_value_2 = std::string(32, '2');
    update_value.len = tmp_value_2.length();
    update_value.data = (char *)tmp_value_2.data();

    key.len = sizeof(uint64_t);
    key.data = (char *)&tmp_key;

    double op_frac;
    std::string op;
    double read_frac = config.insert_frac + config.read_frac;
    double update_frac = config.insert_frac + config.read_frac + config.update_frac;
    xoshiro256pp op_chooser;
    xoshiro256pp key_chooser;
    uint64_t num_op = config.num_op / (config.num_machine * config.num_cli * config.num_coro);
    uint64_t load_avr = load_num / (config.num_machine * config.num_cli * config.num_coro);
    // uint64_t load_avr = num_op;
    for (uint64_t i = 0; i < num_op; i++)
    {
        op_frac = op_chooser();
        op = ycsb_run_method[(cli_id * config.num_coro + coro_id)*num_op+i];
        tmp_key = ycsb_run_key[(cli_id * config.num_coro + coro_id)*num_op+i];
        if (op.compare("i") == 0)
        {
            // log_err("run insert:%lu",tmp_key);
            // co_await cli->insert(&key, &value);
            ret_value.len = 0;
            co_await cli->search(&key, &ret_value);
        }
        else if (op.compare("r") == 0)
        {
            ret_value.len = 0;
            co_await cli->search(&key, &ret_value);
        }
        else if (op.compare("u") == 0)
        {
            co_await cli->update(&key, &update_value);
        }
        else
        {
            co_await cli->remove(&key);
        }
    }
    co_await cli->stop();
    co_return;
}

void load_ycsb(std::string file, char * method_array, std::vector<uint64_t> & key_array, int num) {
    std::ifstream fss;
    uint64_t key;
    fss.open(file);
    char temp[100];
    if (!fss)
    {
        std::cout << "No file." << std::endl;
        exit(1);
    }
    else
    {
        for (int i = 0; i < num; i++)
        {
            fss >> method_array[i];
            fss >> key_array[i];

            fss.getline(temp, 32);
        }
        fss.close();
    }
}



int main(int argc, char *argv[])
{
    config.ParseArg(argc, argv);
    load_num = config.load_num;
    if (config.is_server)
    {
        ServerType ser(config);
        while (true)
            ;
    }
    else
    {
        // client buf_size每个250MB
        uint64_t cbuf_size = (1ul << 20) * 250;
        // 为每个client分配buf
        char *mem_buf = (char *)malloc(cbuf_size * (config.num_cli * config.num_coro + 1));
        // rdma_dev dev("mlx5_1", 1, config.gid_idx);
        rdma_dev dev("mlx5_0", 1, config.gid_idx);
        // rdma_dev dev(nullptr, 1, config.gid_idx);
        // mr管理，用于管理client注册的mr
        std::vector<ibv_mr *> lmrs(config.num_cli * config.num_coro + 1, nullptr);
        std::vector<rdma_client *> rdma_clis(config.num_cli + 1, nullptr);
        std::vector<rdma_conn *> rdma_conns(config.num_cli + 1, nullptr);
        std::vector<rdma_conn *> rdma_wowait_conns(config.num_cli + 1, nullptr);
        std::mutex dir_lock;
        std::vector<BasicDB *> clis;
        std::thread ths[80];

        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            rdma_clis[i] = new rdma_client(dev, so_qp_cap, rdma_default_tempmp_size, config.max_coro, config.cq_size);
            // 连接RDMA Server
            rdma_conns[i] = rdma_clis[i]->connect(config.server_ip);
            assert(rdma_conns[i] != nullptr);
            rdma_wowait_conns[i] = rdma_clis[i]->connect(config.server_ip);

            assert(rdma_wowait_conns[i] != nullptr);
            // 管理每个Client的Coro
            for (uint64_t j = 0; j < config.num_coro; j++)
            {
                lmrs[i * config.num_coro + j] =
                    // 每个Client保留着cbuf_size即250MB的空间
                    dev.create_mr(cbuf_size, mem_buf + cbuf_size * (i * config.num_coro + j));
                BasicDB *cli;
                cli = new ClientType(config, lmrs[i * config.num_coro + j], rdma_clis[i], rdma_conns[i],
                                     rdma_wowait_conns[i], config.machine_id, i, j);
                clis.push_back(cli);
            }
        }

        // For Rehash Thread
        std::atomic_bool exit_flag{true};
        bool rehash_flag = typeid(ClientType) == typeid(CLEVEL::Client) ||
                           typeid(ClientType) == typeid(ClevelFilter::Client) ||
                           typeid(ClientType) == typeid(ClevelSingleFilter::Client);
        ;

        if (config.machine_id == 0 && rehash_flag)
        {
            // rdma_clis[config.num_cli] = new rdma_client(dev, so_qp_cap, rdma_default_tempmp_size, config.max_coro,
            // config.cq_size); rdma_conns[config.num_cli] = rdma_clis[config.num_cli]->connect(config.server_ip);
            // rdma_wowait_conns[config.num_cli] = rdma_clis[config.num_cli]->connect(config.server_ip);

            // lmrs[config.num_cli * config.num_coro] =
            //         dev.create_mr(cbuf_size, mem_buf + cbuf_size * (config.num_cli * config.num_coro));
            // ClientType * rehash_cli = new ClientType(config, lmrs[config.num_cli * config.num_coro],
            // rdma_clis[config.num_cli],
            // rdma_conns[config.num_cli],rdma_wowait_conns[config.num_cli],config.machine_id, config.num_cli,
            // config.num_coro); auto th = [&](rdma_client *rdma_cli) {
            //     rdma_cli->run(rehash_cli->rehash(exit_flag));
            // };
            // ths[config.num_cli] = std::thread(th,rdma_clis[config.num_cli]);
        }

        printf("YCSB Loadstart\n");
        char suffix = 'a' + config.machine_id;

        std::string load_file = config.ycsb_load;
        std::string run_file = config.ycsb_run;

        load_file += "_";
        load_file += suffix;
        run_file += "_";
        run_file += suffix;
        
        std::cout << "Load file: " << load_file << " Run file: " << run_file << std::endl;

        uint64_t ycsb_load_num = load_num / config.num_machine;
        uint64_t ycsb_run_num = config.num_op / config.num_machine;

        char *run_method = (char *)malloc(sizeof(char) * MAX_LOAD);
        char *load_method = (char *)malloc(sizeof(char) * MAX_LOAD);
        std::vector<uint64_t> load_key(MAX_LOAD);

        std::vector<uint64_t> run_key(MAX_LOAD);
        // uint64_t * load_key = (uint64_t *)malloc(sizeof(uint64_t *) * MAX_LOAD);
        // std::string * load_value = (std::string *)malloc(sizeof(std::string *) * MAX_LOAD);
        // std::string * load_method = (std::string *)malloc(sizeof(std::string *) * MAX_LOAD);
        // uint64_t * run_key = (uint64_t *)malloc(sizeof(uint64_t *) * MAX_LOAD);
        // std::string * run_method = (std::string *)malloc(sizeof(std::string *) * MAX_LOAD);
            
        load_ycsb(load_file, load_method, load_key, ycsb_load_num);
        load_ycsb(run_file, run_method, run_key, ycsb_run_num);

        printf("Load start\n");
        
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            auto th = [&](rdma_client *rdma_cli, uint64_t cli_id) {
                std::vector<task<>> tasks;
                for (uint64_t j = 0; j < config.num_coro; j++)
                {
                    tasks.emplace_back(load((ClientType *)clis[cli_id * config.num_coro + j], cli_id, j, load_key));
                }
                rdma_cli->run(gather(std::move(tasks)));
            };
            ths[i] = std::thread(th, rdma_clis[i], i);
        }
        auto start = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            ths[i].join();
        }
        auto end = std::chrono::steady_clock::now();
        double op_cnt = 1.0 * load_num;
        double duration = std::chrono::duration<double, std::milli>(end - start).count();
        printf("Load duration:%.2lfms, OP_Cnt=%.2lf\n", duration, op_cnt);
        printf("Load IOPS:%.2lfKops\n", op_cnt / duration);
        fflush(stdout);
        // ths[config.num_cli].join();

        printf("Run start\n");
        auto op_per_coro = config.num_op / (config.num_machine * config.num_cli * config.num_coro);
        
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            auto th = [&](rdma_client *rdma_cli, uint64_t cli_id) {
                std::vector<task<>> tasks;
                for (uint64_t j = 0; j < config.num_coro; j++)
                {
                    tasks.emplace_back(run(
                                           (ClientType *)(clis[cli_id * config.num_coro + j]), cli_id, j, run_method, run_key));
                }
                rdma_cli->run(gather(std::move(tasks)));
            };
            ths[i] = std::thread(th, rdma_clis[i], i);
        }
        start = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            ths[i].join();
        }
        end = std::chrono::steady_clock::now();
        op_cnt = 1.0 * config.num_op;
        duration = std::chrono::duration<double, std::milli>(end - start).count();
        printf("Run duration:%.2lfms, OP_Cnt=%.2lf\n", duration, op_cnt);
        printf("Run IOPS:%.2lfKops\n", op_cnt / duration);
        fflush(stdout);

        exit_flag.store(false);
        if (config.machine_id == 0 && rehash_flag)
        {
            // ths[config.num_cli].join();
        }
        if (config.machine_id != 0 && rehash_flag)
        {
            // rdma_clis[config.num_cli] = new rdma_client(dev, so_qp_cap, rdma_default_tempmp_size, config.max_coro,
            // config.cq_size); rdma_conns[config.num_cli] = rdma_clis[config.num_cli]->connect(config.server_ip);
            // rdma_wowait_conns[config.num_cli] = rdma_clis[config.num_cli]->connect(config.server_ip);

            // lmrs[config.num_cli * config.num_coro] =
            //         dev.create_mr(cbuf_size, mem_buf + cbuf_size * (config.num_cli * config.num_coro));
            // ClientType* check_cli = new ClientType(config, lmrs[config.num_cli * config.num_coro],
            // rdma_clis[config.num_cli],
            // rdma_conns[config.num_cli],rdma_wowait_conns[config.num_cli],config.machine_id, config.num_cli,
            // config.num_coro);

            // auto th = [&](rdma_client *rdma_cli) {
            //     while(rdma_cli->run(check_cli->check_exit())){
            //         // log_err("waiting for rehash exit");
            //     }
            // };
            // ths[config.num_cli] = std::thread(th,rdma_clis[config.num_cli]);
            // ths[config.num_cli].join();
        }

        if (config.machine_id == 0)
        {
            // rdma_clis[0]->run(((ClientType*)clis[0])->cal_utilization());
        }

        // Reset Ser
        if (config.machine_id == 0)
        {
            rdma_clis[0]->run(((ClientType *)clis[0])->reset_remote());
        }

        free(mem_buf);
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            for (uint64_t j = 0; j < config.num_coro; j++)
            {
                rdma_free_mr(lmrs[i * config.num_coro + j], false);
                delete clis[i * config.num_coro + j];
            }
            delete rdma_wowait_conns[i];
            delete rdma_conns[i];
            delete rdma_clis[i];
        }
    }
}