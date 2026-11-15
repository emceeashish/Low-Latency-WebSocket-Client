#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include "ws_client.h"

void run_concurrency_test(int total_messages, int base_core) {
    MemoryPool pool;
    SPSCQueue<Message, QUEUE_CAPACITY> queue;

    int num_cores = std::max(1u, std::thread::hardware_concurrency());
    int prod_core = (base_core >= 0) ? (base_core % num_cores) : 0;
    int cons_core = (base_core >= 0) ? ((base_core + 1) % num_cores) : 1;

    int64_t t0 = now_ns();

    std::thread producer([&]() {
        pin_thread_to_core(prod_core);
        for (int i = 0; i < total_messages; i++) {
            uint8_t* block = nullptr;
            while ((block = pool.allocate()) == nullptr) {
                cpu_pause();
            }
            *reinterpret_cast<int*>(block) = i;
            Message msg{block, sizeof(int), false};
            while (!queue.push(msg)) {
                cpu_pause();
            }
        }
    });

    int verified = 0;
    std::thread consumer([&]() {
        pin_thread_to_core(cons_core);
        for (int i = 0; i < total_messages; i++) {
            Message msg;
            while (!queue.pop(msg)) {
                cpu_pause();
            }
            if (*reinterpret_cast<int*>(msg.data) == i) {
                verified++;
            }
            pool.deallocate(msg.data);
        }
    });

    producer.join();
    consumer.join();
    double elapsed_ms = double(now_ns() - t0) / 1e6;

    std::cout << "[stress-test] Verified " << verified << " / " << total_messages
              << " concurrent messages in " << std::fixed << std::setprecision(2) << elapsed_ms << " ms"
              << " | Pool blocks free: " << pool.free_blocks.size() << "/" << POOL_NUM_BLOCKS << "\n";
}

void run_benchmark(WebSocketClient& client, int count) {
    std::string payload(128, 'A');

    for (int i = 0; i < 15; i++) {
        client.send_text(payload);
        Message msg;
        if (!client.spin_receive(msg)) return;
        client.release_message(msg);
    }

    LatencyStats stats;
    for (int i = 0; i < count; i++) {
        int64_t t0 = now_ns();
        client.send_text(payload);

        Message msg;
        if (!client.spin_receive(msg)) break;
        int64_t t1 = now_ns();

        stats.record_ns(t0, t1);
        client.release_message(msg);
    }

    stats.print_report("WebSocket Round-Trip Latency (128B Payload)");
    std::cout << "[pool] Free blocks remaining: " << client.pool.free_blocks.size()
              << " / " << POOL_NUM_BLOCKS
              << " | Dropped: " << client.dropped.load() << "\n";
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    std::string port = "8765";
    int pin_core     = 1;
    int bench_count  = 500;
    int stress_count = 100000;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--host" && i + 1 < argc)        host = argv[++i];
        else if (arg == "--port" && i + 1 < argc)   port = argv[++i];
        else if (arg == "--pin" && i + 1 < argc)    pin_core = std::stoi(argv[++i]);
        else if (arg == "--bench" && i + 1 < argc)  bench_count = std::stoi(argv[++i]);
        else if (arg == "--stress" && i + 1 < argc) stress_count = std::stoi(argv[++i]);
    }

    if (stress_count > 0) {
        run_concurrency_test(stress_count, pin_core);
    }

    try {
        net::io_context ioc;
        WebSocketClient client(ioc, host, port, pin_core);
        client.connect();

        run_benchmark(client, bench_count);
        client.close();
    } catch (const std::exception& e) {
        std::cerr << "[ws] Note: Start echo server on " << host << ":" << port
                  << " for live socket RTT (" << e.what() << ")\n";
    }

    return 0;
}
