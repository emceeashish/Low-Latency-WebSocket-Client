#pragma once
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
    #include <immintrin.h>
#endif

#ifdef _WIN32
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <windows.h>
#else
    #include <pthread.h>
    #include <sched.h>
#endif

#include "memory_pool.h"
#include "spsc_queue.h"

namespace net       = boost::asio;
namespace beast     = boost::beast;
namespace websocket = beast::websocket;
using tcp           = net::ip::tcp;

constexpr size_t QUEUE_CAPACITY  = 1024;
constexpr size_t SOCKET_BUF_SIZE = 65536;

inline int64_t now_ns() {
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
}

inline void cpu_pause() {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
}

inline bool pin_thread_to_core(int core_id) {
    if (core_id < 0) return false;
#ifdef _WIN32
    DWORD_PTR mask = static_cast<DWORD_PTR>(1) << core_id;
    return SetThreadAffinityMask(GetCurrentThread(), mask) != 0;
#else
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0;
#endif
}

struct LatencyStats {
    std::vector<double> rtt_us;

    void record_ns(int64_t start_ns, int64_t end_ns) {
        rtt_us.push_back(double(end_ns - start_ns) / 1000.0);
    }

    void print_report(const std::string& title) const {
        if (rtt_us.empty()) return;

        std::vector<double> sorted = rtt_us;
        std::sort(sorted.begin(), sorted.end());
        size_t n = sorted.size();

        double sum  = std::accumulate(sorted.begin(), sorted.end(), 0.0);
        double mean = sum / double(n);

        auto pct = [&](double p) {
            size_t idx = size_t((p / 100.0) * double(n - 1));
            return sorted[idx];
        };

        std::cout << "\n======================================================\n"
                  << "  " << title << " (" << n << " messages)\n"
                  << "======================================================\n"
                  << std::fixed << std::setprecision(2)
                  << "  Min   : " << sorted.front() << " us\n"
                  << "  Mean  : " << mean           << " us\n"
                  << "  p50   : " << pct(50.0)      << " us\n"
                  << "  p95   : " << pct(95.0)      << " us\n"
                  << "  p99   : " << pct(99.0)      << " us\n"
                  << "  p99.9 : " << pct(99.9)      << " us\n"
                  << "  Max   : " << sorted.back()  << " us\n"
                  << "======================================================\n\n";
    }
};

struct WebSocketClient {
    tcp::resolver resolver;
    websocket::stream<tcp::socket> ws;
    std::string host;
    std::string port;
    int rx_core_id;

    std::atomic<bool> running{false};
    std::atomic<uint64_t> dropped{0};
    std::thread rx_thread;

    MemoryPool pool;
    SPSCQueue<Message, QUEUE_CAPACITY> rx_queue;

    WebSocketClient(net::io_context& ioc, std::string h, std::string p, int core = -1)
        : resolver(ioc), ws(ioc), host(std::move(h)), port(std::move(p)), rx_core_id(core) {}

    ~WebSocketClient() {
        close();
    }

    void connect() {
        auto endpoints = resolver.resolve(host, port);
        net::connect(ws.next_layer(), endpoints.begin(), endpoints.end());

        ws.next_layer().set_option(tcp::no_delay(true));
        ws.next_layer().set_option(net::socket_base::receive_buffer_size(SOCKET_BUF_SIZE));
        ws.next_layer().set_option(net::socket_base::send_buffer_size(SOCKET_BUF_SIZE));

        ws.handshake(host, "/");
        running.store(true, std::memory_order_release);

        std::cout << "[ws] Connected to " << host << ":" << port
                  << " | TCP_NODELAY=1 | Pool: " << POOL_NUM_BLOCKS << "x4KB\n";

        rx_thread = std::thread(&WebSocketClient::receive_loop, this);
    }

    void send_text(const std::string& text) {
        ws.text(true);
        ws.write(net::buffer(text));
    }

    // Spins until a message arrives, connection closes, or 5s deadline expires
    bool spin_receive(Message& msg, int64_t timeout_ns = 5'000'000'000LL) {
        int64_t deadline = now_ns() + timeout_ns;
        uint32_t spins = 0;
        while (!rx_queue.pop(msg)) {
            if (!running.load(std::memory_order_acquire)) {
                return false;
            }
            if ((++spins & 1023U) == 0 && now_ns() > deadline) {
                return false;
            }
            cpu_pause();
        }
        return true;
    }

    void release_message(Message& msg) {
        pool.deallocate(msg.data);
        msg.data = nullptr;
        msg.size = 0;
    }

    void close() {
        if (running.exchange(false, std::memory_order_acq_rel)) {
            boost::system::error_code ec;
            // shutdown_both (SHUT_RDWR) reliably wakes a thread blocked in synchronous ws.read()
            ws.next_layer().shutdown(tcp::socket::shutdown_both, ec);
            ws.next_layer().close(ec);
        }

        if (rx_thread.joinable()) {
            rx_thread.join();
        }
    }

    void receive_loop() {
        if (pin_thread_to_core(rx_core_id)) {
            std::cout << "[thread] Receiver thread pinned to CPU core " << rx_core_id << "\n";
        }

        beast::flat_buffer read_buf;
        read_buf.reserve(SOCKET_BUF_SIZE);

        while (running.load(std::memory_order_acquire)) {
            read_buf.clear();
            boost::system::error_code ec;

            ws.read(read_buf, ec);
            if (ec) break;

            uint8_t* block = pool.allocate();
            if (block == nullptr) {
                dropped.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            size_t bytes = std::min(read_buf.size(), POOL_BLOCK_SIZE);
            net::buffer_copy(net::buffer(block, bytes), read_buf.data());

            Message msg{block, uint32_t(bytes), ws.got_text()};
            if (!rx_queue.push(msg)) {
                pool.deallocate(block);
                dropped.fetch_add(1, std::memory_order_relaxed);
            }
        }
        running.store(false, std::memory_order_release);
    }
};
