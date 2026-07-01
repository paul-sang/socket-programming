/*      Chat - Server (Basic - One Thread Per Client)

g++ server.cpp -o server -lpthread
./server 45000

*/
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <sys/select.h>

#include <stdint.h>
#include <errno.h>

#include <unordered_map>
#include <string>
#include <mutex>
#include <map>
#include <math.h>
#include <unordered_set>
#include <thread>
#include <chrono>
#include <vector>
#include <cmath>
#include <functional>
#include <condition_variable>
#include <memory>
#include <atomic>
#include <queue>
using namespace std;

void remove_client(int socket_fd);

// Removed WriteJob and WriteQueue as disk writing has been reverted to direct buffered fwrite.

inline uint32_t compute_adler32_range(const char* buf, size_t len) {
    uint32_t a = 1, b = 0;
    size_t i = 0;
    while (i < len) {
        size_t block = std::min(len - i, (size_t)5552);
        for (size_t j = 0; j < block; j++) {
            a += static_cast<unsigned char>(buf[i + j]);
            b += a;
        }
        a %= 65521;
        b %= 65521;
        i += block;
    }
    return (b << 16) | a;
}

inline uint32_t compute_adler32(const std::string& data) {
    return compute_adler32_range(data.data(), data.length());
}

inline void fast_format_int(char* dest, int val, int width) {
    char* p = dest + width;
    while (p > dest) {
        p--;
        *p = '0' + (val % 10);
        val /= 10;
    }
}

inline int fast_parse_int(const char* src, int len) {
    int val = 0;
    for (int i = 0; i < len; i++) {
        val = val * 10 + (src[i] - '0');
    }
    return val;
}

bool is_sink_mode = false;

struct RttState {
    std::atomic<double> EstimatedRTT{100.0};
    std::atomic<double> DevRTT{10.0};
    std::atomic<double> TimeoutInterval{500.0};
};

struct TransactionState {
    int seq_num;
    int total_segments;
    std::unique_ptr<std::atomic<uint8_t>[]> ack_map;
    std::vector<std::chrono::steady_clock::time_point> send_time_map;
    std::unique_ptr<std::atomic<uint8_t>[]> was_retransmitted;
    std::atomic<int> highest_sent{0};
};

#include <queue>
#include <condition_variable>

struct WriteJob {
    std::vector<char> payload;
};

void rdt_send_internal(int socket_fd, const std::vector<char>& data);

struct SocketSession {
    int socket_fd;
    std::atomic<TransactionState*> active_tx{nullptr};
    RttState rtt_state;
    std::vector<char> last_file_buffer;
    std::vector<std::string> buffered_packets;
    std::mutex buffer_mutex;
    std::atomic<bool> is_receiving{false};
    
    std::queue<WriteJob> tx_queue;
    std::mutex tx_queue_mutex;
    std::condition_variable tx_queue_cv;
    std::atomic<bool> tx_worker_running{true};
    std::thread tx_worker_thread;
};

std::shared_ptr<SocketSession> get_session(int socket_fd) {
    static std::mutex session_init_mutex;
    static std::unordered_map<int, std::shared_ptr<SocketSession>> client_sessions;
    
    std::lock_guard<std::mutex> lock(session_init_mutex);
    auto it = client_sessions.find(socket_fd);
    if (it == client_sessions.end()) {
        auto session = std::make_shared<SocketSession>();
        session->socket_fd = socket_fd;
        session->tx_worker_thread = std::thread([session]() {
            while (session->tx_worker_running) {
                WriteJob job;
                {
                    std::unique_lock<std::mutex> lock(session->tx_queue_mutex);
                    session->tx_queue_cv.wait(lock, [session] {
                        return !session->tx_queue.empty() || !session->tx_worker_running;
                    });
                    if (!session->tx_worker_running && session->tx_queue.empty()) break;
                    job = std::move(session->tx_queue.front());
                    session->tx_queue.pop();
                }
                rdt_send_internal(session->socket_fd, job.payload);
            }
        });
        session->tx_worker_thread.detach();
        client_sessions[socket_fd] = session;
        return session;
    }
    return it->second;
}

#define MAX_USERNAME_LENGTH 255
#define MAX_MESSAGE_LENGTH 999 
#define SIZE_HEADER_LENGTH 3
#define MAX_CLIENTS 50

#define TARGET_USERNAME_BYTES 5
#define SENDER_USERNAME_BYTES 5
#define FILENAME_BYTES 3
#define FILESIZE_BYTES 22
#define SEQ_NUM_BYTES 6
#define TOTAL_SEGMENTS_BYTES 7
#define CURRENT_SEGMENT_BYTES 7
#define HEADER_OPCODE_BYTES 1
#define CHECKSUM_BYTES 6

int DATAGRAM_SIZE = 500;
constexpr int HEADER_SIZE = 26;
int PAYLOAD_SIZE = 474;
double INITIAL_WINDOW = -1.0;
int RECV_WINDOW_SIZE = 1024;

struct ClientRegistry {
    std::unordered_map<std::string, int> cli_to_sock;
    std::unordered_map<int, std::string> sock_to_cli;
};
std::shared_ptr<const ClientRegistry> global_registry = std::make_shared<ClientRegistry>();
std::mutex registry_write_mutex;

// --- CAPA DE RED ---
inline bool udp_send(int socket_fd, const char* data, size_t size) {
    int res = send(socket_fd, data, size, 0);
    if (res < 0) {
        printf("[-] udp_send error on fd %d: %s\n", socket_fd, strerror(errno));
        fflush(stdout);
        if (errno == ECONNREFUSED || errno == EPIPE || errno == EBADF || errno == ENOTCONN) {
            return false;
        }
    }
    return true;
}

void udp_send(int socket_fd, const string& str) {
    udp_send(socket_fd, str.data(), str.size());
}

int udp_recv_buf(int socket_fd, char* buffer, std::shared_ptr<SocketSession> session) {
    session->buffer_mutex.lock();
    if (!session->buffered_packets.empty()) {
        string pkt = session->buffered_packets.front();
        session->buffered_packets.erase(session->buffered_packets.begin());
        session->buffer_mutex.unlock();
        memcpy(buffer, pkt.data(), pkt.size());
        return pkt.size();
    }
    session->buffer_mutex.unlock();

    int bytes_received = recv(socket_fd, buffer, DATAGRAM_SIZE, 0);
    if (bytes_received <= 0) {
        if (bytes_received < 0) {
            printf("[-] recv error on fd %d: %s\n", socket_fd, strerror(errno));
        }
        return bytes_received;
    }
    return bytes_received;
}

// --- CAPA DE TRANSPORTE (RDT) ---
void rdt_send_ack(int socket_fd, int seq_num, int curr_segment, const struct sockaddr_in* dest_addr = nullptr) {
    char buf[DATAGRAM_SIZE];

    // Write total segments = 1
    fast_format_int(buf + CHECKSUM_BYTES, 1, TOTAL_SEGMENTS_BYTES);
    
    // Write current segment = 1
    fast_format_int(buf + CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES, 1, CURRENT_SEGMENT_BYTES);
    
    // Write seq_num
    fast_format_int(buf + CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES, seq_num, SEQ_NUM_BYTES);

    // Write payload: "A" + seg_idx_len (5 bytes) + segment_idx_str
    const int header_len = CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES + SEQ_NUM_BYTES; // 26
    buf[header_len] = 'A';
    
    // Fast count digits for curr_segment to avoid to_string heap allocation
    int temp = curr_segment;
    int digits = 0;
    if (temp == 0) digits = 1;
    else {
        while (temp > 0) {
            digits++;
            temp /= 10;
        }
    }
    
    fast_format_int(buf + header_len + 1, digits, 5);
    fast_format_int(buf + header_len + 6, curr_segment, digits);
    
    // Pad rest with '#'
    memset(buf + header_len + 6 + digits, '#', PAYLOAD_SIZE - 6 - digits);

    // Calculate hash
    uint32_t hash_value = compute_adler32_range(buf + CHECKSUM_BYTES, DATAGRAM_SIZE - CHECKSUM_BYTES) % 1000000;
    fast_format_int(buf, (int)hash_value, CHECKSUM_BYTES);

    if (dest_addr) {
        int res;
        while (true) {
            res = sendto(socket_fd, buf, DATAGRAM_SIZE, 0, (struct sockaddr*)dest_addr, sizeof(*dest_addr));
            if (res >= 0) break;
            if (errno != ENOBUFS && errno != EAGAIN && errno != EWOULDBLOCK) break;
            std::this_thread::yield();
        }
    } else {
        int res;
        while (true) {
            res = send(socket_fd, buf, DATAGRAM_SIZE, 0);
            if (res >= 0) break;
            if (errno != ENOBUFS && errno != EAGAIN && errno != EWOULDBLOCK) break;
            std::this_thread::yield();
        }
    }
}

struct DataSource {
    virtual size_t size() const = 0;
    virtual void read(size_t offset, size_t len, char* dest) const = 0;
    virtual ~DataSource() = default;
};

struct MemoryDataSource : public DataSource {
    const string& data;
    MemoryDataSource(const string& d) : data(d) {}
    size_t size() const override { return data.size(); }
    void read(size_t offset, size_t len, char* dest) const override {
        memcpy(dest, data.data() + offset, len);
    }
};


struct TempFileRouterDataSource : public DataSource {
    string filepath;
    mutable FILE* file;
    size_t total_len;

    static const size_t BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB
    mutable std::vector<char> read_buffer;
    mutable size_t buffer_start_offset = 0;
    mutable size_t buffer_valid_bytes = 0;
    mutable bool buffer_initialized = false;

    TempFileRouterDataSource(const string& path, size_t len) 
        : filepath(path), file(nullptr), total_len(len) {
        file = fopen(filepath.c_str(), "rb");
        read_buffer.resize(BUFFER_SIZE);
    }
    ~TempFileRouterDataSource() {
        if (file) fclose(file);
    }
    size_t size() const override { return total_len; }
    void read(size_t offset, size_t len, char* dest) const override {
        if (file) {
            if (!buffer_initialized || 
                offset < buffer_start_offset || 
                offset + len > buffer_start_offset + buffer_valid_bytes) {
                
                buffer_start_offset = (offset / BUFFER_SIZE) * BUFFER_SIZE;
                fseek(file, buffer_start_offset, SEEK_SET);
                buffer_valid_bytes = fread(read_buffer.data(), 1, BUFFER_SIZE, file);
                buffer_initialized = true;
            }

            if (offset >= buffer_start_offset && offset + len <= buffer_start_offset + buffer_valid_bytes) {
                memcpy(dest, read_buffer.data() + (offset - buffer_start_offset), len);
            } else {
                fseek(file, offset, SEEK_SET);
                size_t read_bytes = fread(dest, 1, len, file);
                if (read_bytes < len) {
                    memset(dest + read_bytes, '#', len - read_bytes);
                }
            }
        } else {
            memset(dest, '#', len);
        }
        if (offset == 0 && len > 0) {
            dest[0] = 'F'; // Swap opcode to uppercase file forwarding
        }
    }
};

string construct_packet(const DataSource& source, int curr_segment, int total_segments, int seq_num) {
    char str_total_segments[32];
    char str_seq_num[32];
    fast_format_int(str_total_segments, total_segments, TOTAL_SEGMENTS_BYTES);
    fast_format_int(str_seq_num, seq_num, SEQ_NUM_BYTES);

    string packet;
    packet.resize(DATAGRAM_SIZE);
    char* buf = &packet[0];

    memcpy(buf + CHECKSUM_BYTES, str_total_segments, TOTAL_SEGMENTS_BYTES);
    fast_format_int(buf + CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES, curr_segment, CURRENT_SEGMENT_BYTES);
    memcpy(buf + CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES, str_seq_num, SEQ_NUM_BYTES);

    const int header_len = CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES + SEQ_NUM_BYTES;
    size_t start_offset = (curr_segment - 1) * (size_t)PAYLOAD_SIZE;
    size_t source_size = source.size();
    
    if (start_offset < source_size) {
        size_t remaining = source_size - start_offset;
        if (remaining >= PAYLOAD_SIZE) {
            source.read(start_offset, PAYLOAD_SIZE, buf + header_len);
        } else {
            source.read(start_offset, remaining, buf + header_len);
            memset(buf + header_len + remaining, '#', PAYLOAD_SIZE - remaining);
        }
    } else {
        memset(buf + header_len, '#', PAYLOAD_SIZE);
    }

    uint32_t hash_value = compute_adler32_range(buf + CHECKSUM_BYTES, DATAGRAM_SIZE - CHECKSUM_BYTES) % 1000000;
    fast_format_int(buf, (int)hash_value, CHECKSUM_BYTES);

    return packet;
}

void construct_packet_inplace(char* dest_buf, const DataSource& source, int curr_segment, int total_segments, int seq_num) {
    char str_total_segments[32];
    char str_seq_num[32];
    fast_format_int(str_total_segments, total_segments, TOTAL_SEGMENTS_BYTES);
    fast_format_int(str_seq_num, seq_num, SEQ_NUM_BYTES);

    memcpy(dest_buf + CHECKSUM_BYTES, str_total_segments, TOTAL_SEGMENTS_BYTES);
    fast_format_int(dest_buf + CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES, curr_segment, CURRENT_SEGMENT_BYTES);
    memcpy(dest_buf + CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES, str_seq_num, SEQ_NUM_BYTES);

    const int header_len = CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES + SEQ_NUM_BYTES;
    size_t start_offset = (curr_segment - 1) * (size_t)PAYLOAD_SIZE;
    size_t source_size = source.size();
    
    if (start_offset < source_size) {
        size_t remaining = source_size - start_offset;
        if (remaining >= PAYLOAD_SIZE) {
            source.read(start_offset, PAYLOAD_SIZE, dest_buf + header_len);
        } else {
            source.read(start_offset, remaining, dest_buf + header_len);
            memset(dest_buf + header_len + remaining, '#', PAYLOAD_SIZE - remaining);
        }
    } else {
        memset(dest_buf + header_len, '#', PAYLOAD_SIZE);
    }

    uint32_t hash_value = compute_adler32_range(dest_buf + CHECKSUM_BYTES, DATAGRAM_SIZE - CHECKSUM_BYTES) % 1000000;
    fast_format_int(dest_buf, (int)hash_value, CHECKSUM_BYTES);
}



void rdt_send_internal_ds(int socket_fd, const DataSource& source) {
    auto session = get_session(socket_fd);
    auto start_prep = std::chrono::steady_clock::now();
    int total_segments = ceil((double)source.size() / PAYLOAD_SIZE);
    if (total_segments == 0) total_segments = 1;
    int seq_num = rand() % 1000000;

    std::vector<char> precomputed_data((size_t)total_segments * DATAGRAM_SIZE);
    for (int i = 1; i <= total_segments; i++) {
        construct_packet_inplace(&precomputed_data[(i - 1) * DATAGRAM_SIZE], source, i, total_segments, seq_num);
    }

    auto end_prep = std::chrono::steady_clock::now();
    double prep_time = std::chrono::duration<double, std::milli>(end_prep - start_prep).count();
    printf("[Timing] Sender Prep: Precomputed %d segments in %.2f ms\n", total_segments, prep_time);
    auto start_tx = std::chrono::steady_clock::now();
    
    double window_size = 2000000000.0;
    bool cc_enabled = false;
    if (INITIAL_WINDOW > 0.0) {
        window_size = INITIAL_WINDOW;
        cc_enabled = true;
    }
    double ssthresh = 1024.0;
    double W_max = ssthresh;
    double C_cubic = 0.4;
    double K_cubic = 1.0;
    if (cc_enabled) {
        K_cubic = pow(W_max * 0.2 / C_cubic, 1.0 / 3.0);
    }
    std::chrono::steady_clock::time_point loss_epoch_start = std::chrono::steady_clock::now();

    bool in_recovery = false;
    std::chrono::steady_clock::time_point recovery_start_time = std::chrono::steady_clock::now();
    int recovery_end_seg = 0;
    int highest_sent = 0;
    int send_base = 1;
    int last_fast_retransmit_seg = 0;
    
    auto local_acked = std::make_unique<uint8_t[]>(total_segments + 1);
    memset(local_acked.get(), 0, total_segments + 1);

    auto tx = std::make_unique<TransactionState>();
    tx->seq_num = seq_num;
    tx->total_segments = total_segments;
    tx->ack_map = std::make_unique<std::atomic<uint8_t>[]>(total_segments + 1);
    tx->send_time_map.resize(total_segments + 1);
    tx->was_retransmitted = std::make_unique<std::atomic<uint8_t>[]>(total_segments + 1);
    for (int i = 0; i <= total_segments; i++) {
        tx->ack_map[i].store(0, std::memory_order_relaxed);
        tx->was_retransmitted[i].store(0, std::memory_order_relaxed);
    }
    session->active_tx.store(tx.get(), std::memory_order_release);

    int total_sends = 0;
    int timeout_retransmissions = 0;
    int fast_retransmissions = 0;
    int total_timeouts = 0;
    double max_window_reached = window_size;
    double timeout_interval = 100.0;
    auto last_debug_print = std::chrono::steady_clock::now();
    auto last_heavy_check = std::chrono::steady_clock::now();

    std::vector<int> segments_to_send;
    std::vector<int> timeouts_to_retransmit;
    segments_to_send.reserve(2048);
    timeouts_to_retransmit.reserve(2048);

    while (true) {

        std::atomic_thread_fence(std::memory_order_acquire);
        timeout_interval = session->rtt_state.TimeoutInterval.load(std::memory_order_relaxed);

        auto loop_now = std::chrono::steady_clock::now();
        if (std::chrono::duration<double, std::milli>(loop_now - last_debug_print).count() > 1000.0) {
            last_debug_print = loop_now;
            int total_acked = 0;
            int total_sent_count = tx->highest_sent.load(std::memory_order_relaxed);
            for (int i = 1; i <= total_segments; i++) {
                if (tx->ack_map[i].load(std::memory_order_relaxed)) total_acked++;
            }
            int in_flight = total_sent_count - total_acked;
            double current_rtt = session->rtt_state.EstimatedRTT.load(std::memory_order_relaxed);
            printf("[Debug Sender] Base: %d/%d, Cwnd: %.1f, ssthresh: %.1f, ACKs: %d/%d, In-Flight: %d, RTT: %.1f ms, Timeouts: %d, FastRetrans: %d\n",
                   send_base, total_segments, window_size, ssthresh, total_acked, total_segments, in_flight, current_rtt, total_timeouts, fast_retransmissions);
            fflush(stdout);
        }

        int fast_retransmit_seg = 0;
        segments_to_send.clear();
        timeouts_to_retransmit.clear();
        bool timeout_occurred = false;

        if (in_recovery && send_base > recovery_end_seg) {
            in_recovery = false;
        }

        double active_window = std::min(window_size, (double)RECV_WINDOW_SIZE);
        double w_cubic = 0.0;
        bool w_cubic_calc = false;
        int max_window_check = std::min((double)total_segments, send_base + active_window + 10);
        
        for (int i = send_base; i <= max_window_check; i++) {
            if (tx->ack_map[i].load(std::memory_order_relaxed) && !local_acked[i]) {
                local_acked[i] = 1;
                if (cc_enabled) {
                    if (window_size < ssthresh) {
                        window_size = std::min(8192.0, window_size + 1.0);
                    } else {
                        if (!w_cubic_calc) {
                            auto now = std::chrono::steady_clock::now();
                            double t = std::chrono::duration<double>(now - loss_epoch_start).count();
                            double diff = t - K_cubic;
                            w_cubic = C_cubic * (diff * diff * diff) + W_max;
                            w_cubic_calc = true;
                        }
                        double w_linear = window_size + 1.0 / window_size;
                        if (w_cubic > w_linear) {
                            window_size = std::min(8192.0, w_cubic);
                        } else {
                            window_size = std::min(8192.0, w_linear);
                        }
                    }
                }
            }
        }
        if (cc_enabled && window_size > max_window_reached) {
            max_window_reached = window_size;
        }

        active_window = std::min(window_size, (double)RECV_WINDOW_SIZE);

        while (send_base <= total_segments && tx->ack_map[send_base].load(std::memory_order_relaxed)) {
            send_base++;
        }

        if (send_base <= total_segments) {
            int acked_after_base_count = 0;
            int max_fr_check = std::min(total_segments, (int)(send_base + active_window - 1));
            for (int i = send_base + 1; i <= max_fr_check; i++) {
                if (tx->ack_map[i].load(std::memory_order_relaxed)) {
                    acked_after_base_count++;
                }
            }
            if (last_fast_retransmit_seg != send_base && acked_after_base_count >= 3) {
                fast_retransmit_seg = send_base;
                last_fast_retransmit_seg = send_base;
                tx->send_time_map[send_base] = std::chrono::steady_clock::now();
                tx->was_retransmitted[send_base].store(1, std::memory_order_release);
                fast_retransmissions++;
                
                if (cc_enabled) {
                    auto now = std::chrono::steady_clock::now();
                    if (!in_recovery) {
                        in_recovery = true;
                        recovery_start_time = now;
                        recovery_end_seg = highest_sent;
                        W_max = window_size;
                        ssthresh = std::max((double)INITIAL_WINDOW, W_max * 0.9);
                        window_size = ssthresh;
                        loss_epoch_start = now;
                        K_cubic = pow(W_max * 0.2 / C_cubic, 1.0 / 3.0);
                    }
                }
                active_window = std::min(window_size, (double)RECV_WINDOW_SIZE);
            }

            int max_send_check = std::min(total_segments, (int)(send_base + active_window - 1));
            int start_new_send = std::max(send_base, highest_sent + 1);
            for (int i = start_new_send; i <= max_send_check; i++) {
                tx->send_time_map[i] = std::chrono::steady_clock::now();
                segments_to_send.push_back(i);
                total_sends++;
                tx->highest_sent.store(i, std::memory_order_release);
                highest_sent = i;
            }

            if (send_base <= highest_sent && !tx->ack_map[send_base].load(std::memory_order_relaxed)) {
                auto now = std::chrono::steady_clock::now();
                double elapsed = std::chrono::duration<double, std::milli>(now - tx->send_time_map[send_base]).count();
                if (elapsed > timeout_interval) {
                    tx->send_time_map[send_base] = now;
                    tx->was_retransmitted[send_base].store(1, std::memory_order_release);
                    timeouts_to_retransmit.push_back(send_base);
                    timeout_retransmissions++;
                    timeout_occurred = true;
                }
            }

            if (timeout_occurred) {
                total_timeouts++;
                if (cc_enabled) {
                    auto now = std::chrono::steady_clock::now();
                    if (!in_recovery || tx->send_time_map[send_base] > recovery_start_time) {
                        in_recovery = true;
                        recovery_start_time = now;
                        recovery_end_seg = highest_sent;
                        W_max = window_size;
                        ssthresh = std::max((double)INITIAL_WINDOW, W_max * 0.8);
                        window_size = std::max((double)INITIAL_WINDOW, W_max * 0.7);
                        loss_epoch_start = now;
                        K_cubic = pow(W_max * 0.2 / C_cubic, 1.0 / 3.0);
                    }
                }
            }
        }

        if (send_base > total_segments) {
            session->active_tx.store(nullptr, std::memory_order_release);
            break;
        }

        bool socket_alive = true;
        if (fast_retransmit_seg > 0) {
            if (!udp_send(socket_fd, &precomputed_data[(fast_retransmit_seg - 1) * DATAGRAM_SIZE], DATAGRAM_SIZE)) {
                socket_alive = false;
            }
        }
        if (socket_alive) {
            for (int i : segments_to_send) {
                if (!udp_send(socket_fd, &precomputed_data[(i - 1) * DATAGRAM_SIZE], DATAGRAM_SIZE)) {
                    socket_alive = false;
                    break;
                }
            }
        }
        if (socket_alive) {
            for (int i : timeouts_to_retransmit) {
                if (!udp_send(socket_fd, &precomputed_data[(i - 1) * DATAGRAM_SIZE], DATAGRAM_SIZE)) {
                    socket_alive = false;
                    break;
                }
            }
        }
        if (!socket_alive) {
            printf("[-] RDT: Conexión rechazada/rota detectada. Abortando envío.\n");
            session->active_tx.store(nullptr, std::memory_order_release);
            remove_client(socket_fd);
            break;
        }

        if (send_base <= total_segments) {
            std::this_thread::yield();
        }
    }

    auto end_tx = std::chrono::steady_clock::now();
    double tx_time = std::chrono::duration<double, std::milli>(end_tx - start_tx).count();
    printf("[Timing] Transmission: Sent and ACKed all segments in %.2f ms (Throughput: %.2f MB/s)\n", tx_time, (source.size() / 1024.0 / 1024.0) / (tx_time / 1000.0));
    printf("[Telemetry] Sends: %d new, %d timeout-retrans, %d fast-retrans, %d timeouts-occurred. Max Cwnd: %.1f. Final TimeoutInterval: %.1f ms\n",
           total_sends, timeout_retransmissions, fast_retransmissions, total_timeouts, max_window_reached, timeout_interval);
}



string rdt_recv(int socket_fd) {
    auto session = get_session(socket_fd);
    char packet[DATAGRAM_SIZE];
    int expected_total_segments = -1;
    int received_count = 0;
    int duplicate_packets_count = 0;
    std::unique_ptr<uint8_t[]> received_segments;
    int target_seq_num = -1;
    auto start_recv = std::chrono::steady_clock::now();
    auto last_debug_print = std::chrono::steady_clock::now();
    string preview_payload;

    int TOTAL_SEG_START = CHECKSUM_BYTES;
    int CURR_SEG_START = TOTAL_SEG_START + TOTAL_SEGMENTS_BYTES;
    int SEQ_NUM_START = CURR_SEG_START + CURRENT_SEGMENT_BYTES;
    int DATA_START = SEQ_NUM_START + SEQ_NUM_BYTES;

    while (true) {
        auto loop_now = std::chrono::steady_clock::now();
        if (std::chrono::duration<double, std::milli>(loop_now - last_debug_print).count() > 1000.0) {
            last_debug_print = loop_now;
            printf("[Debug Recv] Received: %d/%d, Duplicates: %d\n",
                   received_count, expected_total_segments, duplicate_packets_count);
            fflush(stdout);
        }

        int bytes = 0;
        bool got_from_session_buffer = false;
        {
            std::lock_guard<std::mutex> lock(session->buffer_mutex);
            if (!session->buffered_packets.empty()) {
                string pkt = session->buffered_packets.front();
                session->buffered_packets.erase(session->buffered_packets.begin());
                memcpy(packet, pkt.data(), pkt.size());
                bytes = pkt.size();
                got_from_session_buffer = true;
            }
        }
        if (!got_from_session_buffer) {
            bytes = recv(socket_fd, packet, DATAGRAM_SIZE, 0);
            if (bytes <= 0) {
                session->is_receiving = false;
                return "";
            }
        }

        if (bytes < DATAGRAM_SIZE) continue; 

        uint32_t computed_hash_val = compute_adler32_range(packet + CHECKSUM_BYTES, DATAGRAM_SIZE - CHECKSUM_BYTES) % 1000000;
        char str_computed_hash[CHECKSUM_BYTES + 1];
        fast_format_int(str_computed_hash, (int)computed_hash_val, CHECKSUM_BYTES);

        if (memcmp(packet, str_computed_hash, CHECKSUM_BYTES) != 0) {
            continue; 
        }

        int total_segments = fast_parse_int(packet + TOTAL_SEG_START, TOTAL_SEGMENTS_BYTES);
        int curr_segment   = fast_parse_int(packet + CURR_SEG_START, CURRENT_SEGMENT_BYTES);
        int seq_num        = fast_parse_int(packet + SEQ_NUM_START, SEQ_NUM_BYTES);

        bool is_ack = false;
        int ack_seg = -1;
        if (packet[DATA_START] == 'A' && total_segments == 1 && curr_segment == 1) {
            is_ack = true;
            int seg_idx_len = fast_parse_int(packet + DATA_START + 1, 5);
            ack_seg = fast_parse_int(packet + DATA_START + 6, seg_idx_len);
            
            TransactionState* tx = session->active_tx.load(std::memory_order_acquire);
            if (tx && tx->seq_num == seq_num && ack_seg >= 1 && ack_seg <= tx->total_segments) {
                tx->ack_map[ack_seg].store(1, std::memory_order_release);
                bool was_retrans = tx->was_retransmitted[ack_seg].load(std::memory_order_acquire);
                int highest_sent = tx->highest_sent.load(std::memory_order_acquire);
                if (ack_seg <= highest_sent && !was_retrans) {
                    auto now = std::chrono::steady_clock::now();
                    double sample_rtt = std::chrono::duration<double, std::milli>(now - tx->send_time_map[ack_seg]).count();
                    
                    double est_rtt = session->rtt_state.EstimatedRTT.load(std::memory_order_relaxed);
                    double dev_rtt = session->rtt_state.DevRTT.load(std::memory_order_relaxed);
                    est_rtt = 0.875 * est_rtt + 0.125 * sample_rtt;
                    dev_rtt = 0.75 * dev_rtt + 0.25 * std::abs(sample_rtt - est_rtt);
                    double timeout = est_rtt + 4 * dev_rtt;
                    if (timeout < 100.0) timeout = 100.0;
                    if (timeout > 1000.0) timeout = 1000.0;
                    session->rtt_state.EstimatedRTT.store(est_rtt, std::memory_order_relaxed);
                    session->rtt_state.DevRTT.store(dev_rtt, std::memory_order_relaxed);
                    session->rtt_state.TimeoutInterval.store(timeout, std::memory_order_relaxed);
                }
            }
        }

        if (is_ack) continue; 

        rdt_send_ack(socket_fd, seq_num, curr_segment);

        if (expected_total_segments == -1) {
            expected_total_segments = total_segments;
            target_seq_num = seq_num;
            start_recv = std::chrono::steady_clock::now();
            
            if (!is_sink_mode) {
                session->last_file_buffer.resize((size_t)expected_total_segments * PAYLOAD_SIZE);
            }
            received_segments = std::make_unique<uint8_t[]>(expected_total_segments + 1);
            memset(received_segments.get(), 0, expected_total_segments + 1);
        } else if (seq_num != target_seq_num) {
            continue; 
        }

        if (curr_segment >= 1 && curr_segment <= expected_total_segments) {
            if (curr_segment == 1) {
                preview_payload.assign(packet + DATA_START, PAYLOAD_SIZE);
            }
            if (!received_segments[curr_segment]) {
                if (!is_sink_mode) {
                    memcpy(&session->last_file_buffer[(curr_segment - 1) * (size_t)PAYLOAD_SIZE], packet + DATA_START, PAYLOAD_SIZE);
                }
                received_segments[curr_segment] = 1;
                received_count++;
            } else {
                duplicate_packets_count++;
            }
        }

        if (received_count == expected_total_segments) {
            break; 
        }
    }
    session->is_receiving = false;
    auto end_recv = std::chrono::steady_clock::now();
    double recv_time = std::chrono::duration<double, std::milli>(end_recv - start_recv).count();

    bool is_file = (preview_payload.size() > 0 && (preview_payload[0] == 'F' || preview_payload[0] == 'f' || preview_payload[0] == 'M' || preview_payload[0] == 'W' || preview_payload[0] == 'P' || preview_payload[0] == 'G'));

    if (is_file || is_sink_mode) {
        printf("[Timing] Reception: Received %d segments in %.2f ms. RAM Stored (Sink: %d). Duplicates: %d\n", 
               expected_total_segments, recv_time, is_sink_mode, duplicate_packets_count);
        return preview_payload;
    } else {
        auto start_flat = std::chrono::steady_clock::now();
        string full_protocol_stream(session->last_file_buffer.begin(), session->last_file_buffer.begin() + expected_total_segments * PAYLOAD_SIZE);
        auto end_flat = std::chrono::steady_clock::now();
        double flat_time = std::chrono::duration<double, std::milli>(end_flat - start_flat).count();

        printf("[Timing] Reception: Received %d segments in %.2f ms. Flattening: %.2f ms. Duplicates: %d\n", 
               expected_total_segments, recv_time, flat_time, duplicate_packets_count);
        return full_protocol_stream;
    }
}

// --- Helpers ---
void remove_client(int socket_fd) {
    std::lock_guard<std::mutex> lock(registry_write_mutex);
    auto old_registry = std::atomic_load(&global_registry);
    auto it = old_registry->sock_to_cli.find(socket_fd);
    if (it != old_registry->sock_to_cli.end()) {
        string username = it->second;
        printf("[-] Cliente desconectado: %s (Socket: %d)\n", username.c_str(), socket_fd);
        
        auto new_registry = std::make_shared<ClientRegistry>(*old_registry);
        new_registry->cli_to_sock.erase(username);
        new_registry->sock_to_cli.erase(socket_fd);
        std::atomic_store(&global_registry, std::shared_ptr<const ClientRegistry>(new_registry));
    }
    auto session = get_session(socket_fd);
    session->tx_worker_running = false;
    session->tx_queue_cv.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    shutdown(socket_fd, SHUT_RDWR);
    close(socket_fd);
}

// --- HANDLERS DE PROTOCOLO ---
void rdt_send_handshake_response(int main_sock, const struct sockaddr_in& client_addr, const string& payload) {
    string packet;
    packet.resize(DATAGRAM_SIZE);
    char* buf = &packet[0];

    int seq_num = rand() % 1000000;

    // Write total segments = 1
    fast_format_int(buf + CHECKSUM_BYTES, 1, TOTAL_SEGMENTS_BYTES);
    
    // Write current segment = 1
    fast_format_int(buf + CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES, 1, CURRENT_SEGMENT_BYTES);
    
    // Write seq_num
    fast_format_int(buf + CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES, seq_num, SEQ_NUM_BYTES);

    // Write payload
    const int header_len = CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES + SEQ_NUM_BYTES; // 26
    size_t payload_len = std::min(payload.size(), (size_t)PAYLOAD_SIZE);
    memcpy(buf + header_len, payload.data(), payload_len);
    if (payload_len < (size_t)PAYLOAD_SIZE) {
        memset(buf + header_len + payload_len, '#', PAYLOAD_SIZE - payload_len);
    }

    // Calculate hash
    uint32_t hash_value = compute_adler32_range(buf + CHECKSUM_BYTES, DATAGRAM_SIZE - CHECKSUM_BYTES) % 1000000;
    fast_format_int(buf, (int)hash_value, CHECKSUM_BYTES);

    sendto(main_sock, packet.data(), packet.size(), 0, (struct sockaddr*)&client_addr, sizeof(client_addr));
}


class VectorDataSource : public DataSource {
    const std::vector<char>& data_;
    size_t size_;
public:
    VectorDataSource(const std::vector<char>& data, size_t size) : data_(data), size_(size) {}
    size_t size() const override { return size_; }
    void read(size_t offset, size_t len, char* dest) const override {
        memcpy(dest, data_.data() + offset, len);
    }
};

void rdt_send_internal(int socket_fd, const std::vector<char>& data) {
    VectorDataSource source(data, data.size());
    rdt_send_internal_ds(socket_fd, source);
}

void rdt_send(int socket_fd, const std::vector<char>& payload) {
    auto session = get_session(socket_fd);
    {
        std::lock_guard<std::mutex> lock(session->tx_queue_mutex);
        session->tx_queue.push({payload});
    }
    session->tx_queue_cv.notify_one();
}

void rdt_send(int socket_fd, const std::string& payload) {
    std::vector<char> vec(payload.begin(), payload.end());
    rdt_send(socket_fd, vec);
}

void handle_broadcast(int client_sock, const string& full_payload) {
    size_t parse_offset = 1;
    int msg_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string msg = full_payload.substr(parse_offset, msg_len);
    
    auto registry = std::atomic_load(&global_registry);
    string sender = "";
    auto sender_it = registry->sock_to_cli.find(client_sock);
    if (sender_it != registry->sock_to_cli.end()) {
        sender = sender_it->second;
    }
    if (sender.empty()) return;
    
    char sender_len[SENDER_USERNAME_BYTES + 1];
    char msg_len_str[SENDER_USERNAME_BYTES + 1];
    sprintf(sender_len, "%0*d", SENDER_USERNAME_BYTES, (int)sender.length());
    sprintf(msg_len_str, "%0*d", SENDER_USERNAME_BYTES, msg_len);
    
    string forward_payload = "B" + string(sender_len) + sender + string(msg_len_str) + msg;
    
    std::vector<int> target_fds;
    for (auto const& [name, fd] : registry->cli_to_sock) {
        if (fd != client_sock) {
            target_fds.push_back(fd);
        }
    }
    
    for (int fd : target_fds) {
        rdt_send(fd, forward_payload);
    }
    printf("[Broadcast] %s: %s\n", sender.c_str(), msg.c_str());
}

void handle_unicast(int client_sock, const string& full_payload) {
    size_t parse_offset = 1;
    int target_len = atoi(full_payload.substr(parse_offset, TARGET_USERNAME_BYTES).c_str());
    parse_offset += TARGET_USERNAME_BYTES;
    string target = full_payload.substr(parse_offset, target_len);
    parse_offset += target_len;
    
    int sender_len_val = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string sender_from_payload = full_payload.substr(parse_offset, sender_len_val);
    parse_offset += sender_len_val;
    
    int msg_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string msg = full_payload.substr(parse_offset, msg_len);
    
    auto registry = std::atomic_load(&global_registry);
    string sender = "";
    auto sender_it = registry->sock_to_cli.find(client_sock);
    if (sender_it != registry->sock_to_cli.end()) {
        sender = sender_it->second;
    }
    
    auto it = registry->cli_to_sock.find(target);
    if (it == registry->cli_to_sock.end()) {
        string err_msg = "User " + target + " not found.";
        char err_len[SENDER_USERNAME_BYTES + 1];
        sprintf(err_len, "%0*d", SENDER_USERNAME_BYTES, (int)err_msg.length());
        string err_payload = "E" + string(err_len) + err_msg;
        rdt_send(client_sock, err_payload);
        return;
    }
    int target_sock = it->second;
    
    char sender_len[SENDER_USERNAME_BYTES + 1];
    char msg_len_str[SENDER_USERNAME_BYTES + 1];
    sprintf(sender_len, "%0*d", SENDER_USERNAME_BYTES, (int)sender.length());
    sprintf(msg_len_str, "%0*d", SENDER_USERNAME_BYTES, msg_len);
    
    string forward_payload = "U" + string(sender_len) + sender + string(msg_len_str) + msg;
    rdt_send(target_sock, forward_payload);
    printf("[Unicast] %s -> %s: %s\n", sender.c_str(), target.c_str(), msg.c_str());
}


struct FileDataSource : public DataSource {
    string header;
    string filepath;
    mutable FILE* file;
    size_t file_len;
    size_t total_len;

    static const size_t BUFFER_SIZE = 4 * 1024 * 1024;
    mutable std::vector<char> read_buffer;
    mutable size_t buffer_start_offset = 0;
    mutable size_t buffer_valid_bytes = 0;
    mutable bool buffer_initialized = false;

    FileDataSource(const string& h, const string& path, size_t flen) 
        : header(h), filepath(path), file(nullptr), file_len(flen) {
        total_len = header.size() + file_len;
        file = fopen(filepath.c_str(), "rb");
        read_buffer.resize(BUFFER_SIZE);
    }
    ~FileDataSource() {
        if (file) fclose(file);
    }
    size_t size() const override { return total_len; }
    void read(size_t offset, size_t len, char* dest) const override {
        if (offset < header.size()) {
            size_t header_read = std::min(len, header.size() - offset);
            memcpy(dest, header.data() + offset, header_read);
            if (header_read < len) {
                size_t file_read = len - header_read;
                if (file) {
                    fseek(file, 0, SEEK_SET);
                    size_t read_bytes = fread(dest + header_read, 1, file_read, file);
                    if (read_bytes < file_read) {
                        memset(dest + header_read + read_bytes, '#', file_read - read_bytes);
                    }
                } else {
                    memset(dest + header_read, '#', file_read);
                }
            }
        } else {
            size_t file_offset = offset - header.size();
            if (file) {
                if (!buffer_initialized || 
                    file_offset < buffer_start_offset || 
                    file_offset + len > buffer_start_offset + buffer_valid_bytes) {
                    
                    buffer_start_offset = (file_offset / BUFFER_SIZE) * BUFFER_SIZE;
                    fseek(file, buffer_start_offset, SEEK_SET);
                    buffer_valid_bytes = fread(read_buffer.data(), 1, BUFFER_SIZE, file);
                    buffer_initialized = true;
                }

                if (file_offset >= buffer_start_offset && file_offset + len <= buffer_start_offset + buffer_valid_bytes) {
                    memcpy(dest, read_buffer.data() + (file_offset - buffer_start_offset), len);
                } else {
                    fseek(file, file_offset, SEEK_SET);
                    size_t read_bytes = fread(dest, 1, len, file);
                    if (read_bytes < len) {
                        memset(dest + read_bytes, '#', len - read_bytes);
                    }
                }
            } else {
                memset(dest, '#', len);
            }
        }
    }
};


// FL State
struct FLState {
    int master_sock = -1;
    string master_name = "";
    int total_slaves = 0;
    int received_weights = 0;
    std::mutex fl_mutex;
    std::vector<string> weight_files;
} global_fl_state;

void handle_M_master_dataset(int client_sock, const string& full_payload) {
    size_t parse_offset = 1; 
    int target_len = atoi(full_payload.substr(parse_offset, TARGET_USERNAME_BYTES).c_str());
    parse_offset += TARGET_USERNAME_BYTES + target_len;
    
    int filename_len = atoi(full_payload.substr(parse_offset, FILENAME_BYTES).c_str());
    parse_offset += FILENAME_BYTES + filename_len;

    int sender_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string sender = full_payload.substr(parse_offset, sender_len);
    parse_offset += sender_len;

    long long file_size = atoll(full_payload.substr(parse_offset, FILESIZE_BYTES).c_str());
    parse_offset += FILESIZE_BYTES;

    auto session = get_session(client_sock);
    FILE *out = fopen("server_dataset.csv", "wb");
    if(out) {
        fwrite(session->last_file_buffer.data() + parse_offset, 1, file_size, out);
        fclose(out);
    }
    session->last_file_buffer.clear();

    // Determine slaves
    auto reg = std::atomic_load(&global_registry);
    std::vector<int> slaves;
    for (auto const& [sock, name] : reg->sock_to_cli) {
        if (sock != client_sock) {
            slaves.push_back(sock);
        }
    }

    {
        std::lock_guard<std::mutex> lock(global_fl_state.fl_mutex);
        global_fl_state.master_sock = client_sock;
        global_fl_state.master_name = sender;
        global_fl_state.total_slaves = slaves.size();
        global_fl_state.received_weights = 0;
        global_fl_state.weight_files.clear();
    }

    if (slaves.empty()) {
        printf("[FL] No hay esclavos conectados para federar.\n");
        return;
    }

    int total_rows = 10000; // hardcoded dataset size for this demo
    int train_rows = total_rows * 0.8;
    int rows_per_slave = train_rows / slaves.size();

    int start = 0;
    for (int slave : slaves) {
        string slave_name = reg->sock_to_cli.at(slave);
        // Build W header
        char header_buf[512];
        sprintf(header_buf, "W%0*d%s%0*d%s%0*d%s%0*lld",
                TARGET_USERNAME_BYTES, (int)slave_name.length(), slave_name.c_str(),
                FILENAME_BYTES, 18, "server_dataset.csv",
                SENDER_USERNAME_BYTES, 6, "server",
                FILESIZE_BYTES, file_size);

        // Send dataset in detached thread
        string header_str = header_buf;
        std::thread([slave, header_str, file_size]() {
            FileDataSource source(header_str, "server_dataset.csv", file_size);
            rdt_send_internal_ds(slave, source);
        }).detach();

        // Send range R
        int end = start + rows_per_slave;
        string range = std::to_string(start) + "_" + std::to_string(end);
        char msg_len_str[SENDER_USERNAME_BYTES + 1];
        sprintf(msg_len_str, "%0*d", SENDER_USERNAME_BYTES, (int)range.length());
        string r_payload = "R" + string(msg_len_str) + range;
        
        std::thread([slave, r_payload]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(500)); // wait for W to start sending
            rdt_send(slave, r_payload);
        }).detach();

        start = end;
    }
}

void handle_P_partial_weights(int client_sock, const string& full_payload) {
    size_t parse_offset = 1; 
    int target_len = atoi(full_payload.substr(parse_offset, TARGET_USERNAME_BYTES).c_str());
    parse_offset += TARGET_USERNAME_BYTES + target_len;
    
    int filename_len = atoi(full_payload.substr(parse_offset, FILENAME_BYTES).c_str());
    parse_offset += FILENAME_BYTES + filename_len;

    int sender_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string sender = full_payload.substr(parse_offset, sender_len);
    parse_offset += sender_len;

    long long file_size = atoll(full_payload.substr(parse_offset, FILESIZE_BYTES).c_str());
    parse_offset += FILESIZE_BYTES;

    auto session = get_session(client_sock);
    string w_filename = "server_weight_" + sender + ".pt";
    FILE *out = fopen(w_filename.c_str(), "wb");
    if(out){
        fwrite(session->last_file_buffer.data() + parse_offset, 1, file_size, out);
        fclose(out);
    }
    session->last_file_buffer.clear();

    bool done = false;
    int master_sock = -1;
    string master_name = "";
    string cmd = "python3 model/fl_avg.py global.pt";
    
    {
        std::lock_guard<std::mutex> lock(global_fl_state.fl_mutex);
        global_fl_state.received_weights++;
        global_fl_state.weight_files.push_back(w_filename);
        
        if (global_fl_state.received_weights == global_fl_state.total_slaves) {
            done = true;
            master_sock = global_fl_state.master_sock;
            master_name = global_fl_state.master_name;
            for(const auto& f : global_fl_state.weight_files) cmd += " " + f;
        }
    }

    if (done) {
        printf("[FL] Todos los pesos recibidos. Ejecutando FedAvg...\n");
        if(system(cmd.c_str()) != 0) {
            printf("[FL] Error promediando pesos.\n");
        }
        
        long long g_size = 0;
        FILE* f = fopen("global.pt", "rb");
        if(f) { fseek(f,0,SEEK_END); g_size = ftell(f); fclose(f); }

        char header_buf[512];
        sprintf(header_buf, "G%0*d%s%0*d%s%0*d%s%0*lld",
                TARGET_USERNAME_BYTES, (int)master_name.length(), master_name.c_str(),
                FILENAME_BYTES, 9, "global.pt",
                SENDER_USERNAME_BYTES, 6, "server",
                FILESIZE_BYTES, g_size);

        string header_str = header_buf;
        std::thread([master_sock, header_str, g_size]() {
            FileDataSource source(header_str, "global.pt", g_size);
            rdt_send_internal_ds(master_sock, source);
        }).detach();
    }
}

void handle_file(int client_sock, const string& full_payload) {
    size_t parse_offset = 1; 

    int target_len = atoi(full_payload.substr(parse_offset, TARGET_USERNAME_BYTES).c_str());
    parse_offset += TARGET_USERNAME_BYTES;
    string target_username = full_payload.substr(parse_offset, target_len);
    parse_offset += target_len;

    int filename_len = atoi(full_payload.substr(parse_offset, FILENAME_BYTES).c_str());
    parse_offset += FILENAME_BYTES;
    string filepath = full_payload.substr(parse_offset, filename_len);
    parse_offset += filename_len;

    auto registry = std::atomic_load(&global_registry);
    string sender_username = "";
    auto sender_it = registry->sock_to_cli.find(client_sock);
    if (sender_it != registry->sock_to_cli.end()) {
        sender_username = sender_it->second;
    }

    if (is_sink_mode) {
        printf("[Server Sink Mode] Archivo de '%s' recibido con éxito. Saltando ruteo a '%s'.\n", 
               sender_username.c_str(), target_username.c_str());
        auto session = get_session(client_sock);
        session->last_file_buffer.clear();
        session->last_file_buffer.shrink_to_fit();
        return;
    }
    printf("[Server] Solicitud de envío de archivo de '%s' para '%s'\n", 
           sender_username.c_str(), target_username.c_str());

    auto it = registry->cli_to_sock.find(target_username);
    if (it == registry->cli_to_sock.end()) {
        printf("[-] Server Error: El usuario destino '%s' no está en línea.\n", target_username.c_str());
        
        string error_msg = "El usuario destino no está en línea.";
        char error_len[SENDER_USERNAME_BYTES + 1];
        sprintf(error_len, "%0*d", SENDER_USERNAME_BYTES, (int)error_msg.length());
        string error_packet = "E" + string(error_len) + error_msg;
        rdt_send(client_sock, error_packet);
        
        auto session = get_session(client_sock);
        session->last_file_buffer.clear();
        session->last_file_buffer.shrink_to_fit();
        return;
    }
    int target_sock = it->second;

    printf("[Server] Ruteando archivo íntegro hacia '%s'...\n", target_username.c_str());
    
    auto session = get_session(client_sock);
    if (!session->last_file_buffer.empty()) {
        session->last_file_buffer[0] = 'F'; // Swap opcode to uppercase file forwarding
        rdt_send(target_sock, session->last_file_buffer);
        session->last_file_buffer.clear();
        session->last_file_buffer.shrink_to_fit();
    } else {
        string forward_payload = full_payload;
        forward_payload[0] = 'F'; 
        rdt_send(target_sock, forward_payload);
    }
}

void handle_list(int client_sock, const string& full_payload) {
    auto registry = std::atomic_load(&global_registry);
    string user_list = "";
    for (auto const& [name, fd] : registry->cli_to_sock) {
        if (!user_list.empty()) {
            user_list += ",";
        }
        user_list += name;
    }
    
    char list_len[SENDER_USERNAME_BYTES + 1];
    sprintf(list_len, "%0*d", SENDER_USERNAME_BYTES, (int)user_list.length());
    
    string response_payload = "T" + string(list_len) + user_list;
    rdt_send(client_sock, response_payload);
}

// --- WORKER DEL CLIENTE ---
void client_worker(int client_sock) {
    while (true) {
        string full_protocol_stream = rdt_recv(client_sock);
        if (full_protocol_stream.empty()) {
            break; 
        }
        
        unsigned char opcode = full_protocol_stream[0];
        switch (opcode) {
            case 'B': handle_broadcast(client_sock, full_protocol_stream); break;
            case 'U': handle_unicast(client_sock, full_protocol_stream); break;
            case 'T': handle_list(client_sock, full_protocol_stream); break;
            case 'F': handle_file(client_sock, full_protocol_stream); break;
            case 'M': handle_M_master_dataset(client_sock, full_protocol_stream); break;
            case 'P': handle_P_partial_weights(client_sock, full_protocol_stream); break;
            case 'O': remove_client(client_sock); return; //LOGOUT
            default:
                printf("[-] codigo desconocido (%c) recibido del socket %d\n", opcode, client_sock);
                break;
        }
    }

    remove_client(client_sock);
}

int main(int argc, char *argv[]) {
    std::vector<char*> clean_args;
    clean_args.push_back(argv[0]);
    for (int i = 1; i < argc; i++) {
        if ((std::string(argv[i]) == "-p" || std::string(argv[i]) == "--packet-size") && i + 1 < argc) {
            DATAGRAM_SIZE = atoi(argv[i + 1]);
            PAYLOAD_SIZE = DATAGRAM_SIZE - HEADER_SIZE;
            i++; // skip value
        } else if ((std::string(argv[i]) == "-w" || std::string(argv[i]) == "--window") && i + 1 < argc) {
            INITIAL_WINDOW = atof(argv[i + 1]);
            i++; // skip value
        } else if ((std::string(argv[i]) == "-rw" || std::string(argv[i]) == "--recv-window") && i + 1 < argc) {
            RECV_WINDOW_SIZE = atoi(argv[i + 1]);
            i++; // skip value
        } else {
            clean_args.push_back(argv[i]);
        }
    }
    int clean_argc = clean_args.size();
    char** clean_argv = clean_args.data();

    if (clean_argc < 2 || clean_argc > 3) {
        printf("Uso: %s <Puerto> [--sink] [-p <packet_size>] [-w <window_size>] [-rw <recv_window>]\n", clean_argv[0]);
        return EXIT_FAILURE;
    }

    int server_port = atoi(clean_argv[1]);
    if (clean_argc == 3 && string(clean_argv[2]) == "--sink") {
        is_sink_mode = true;
        printf("[*] Servidor iniciado en MODO SINK (sin guardar datos a disco).\n");
    }
    int server_sock;

    server_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_sock < 0) {
        perror("Error al crear socket maestro UDP");
        exit(EXIT_FAILURE);
    }
    int opt_buf = 20 * 1024 * 1024;
    setsockopt(server_sock, SOL_SOCKET, SO_RCVBUF, &opt_buf, sizeof(opt_buf));
    setsockopt(server_sock, SOL_SOCKET, SO_SNDBUF, &opt_buf, sizeof(opt_buf));

    int actual_rcv = 0, actual_snd = 0;
    socklen_t opt_len = sizeof(actual_rcv);
    getsockopt(server_sock, SOL_SOCKET, SO_RCVBUF, &actual_rcv, &opt_len);
    getsockopt(server_sock, SOL_SOCKET, SO_SNDBUF, &actual_snd, &opt_len);
    /*
    printf("[Diagnostic] Master Server Socket Buffer: Requested=20.0MB, Actual_RCV=%.2fMB, Actual_SND=%.2fMB\n", 
           actual_rcv / (1024.0 * 1024.0), actual_snd / (1024.0 * 1024.0));
    fflush(stdout);
    */

    int opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(server_port);

    if (::bind(server_sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Error en bind");
        exit(EXIT_FAILURE);
    }

    printf("[*] Servidor UDP iniciado en el puerto %d. Monitoreando con select...\n", server_port);

    fd_set current_sockets, ready_sockets;
    FD_ZERO(&current_sockets);
    FD_SET(server_sock, &current_sockets);
    int max_socket_so_far = server_sock;

    while (true) {
        ready_sockets = current_sockets; 

        if (select(max_socket_so_far + 1, &ready_sockets, NULL, NULL, NULL) < 0) {
            perror("Error en select");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i <= max_socket_so_far; i++) {
            if (FD_ISSET(i, &ready_sockets)) {
                
                if (i == server_sock) {
                    struct sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    std::vector<char> buffer_vec(DATAGRAM_SIZE, 0);
                    char* buffer = buffer_vec.data();

                    int bytes_read = recvfrom(server_sock, buffer, DATAGRAM_SIZE, 0, 
                                              (struct sockaddr*)&client_addr, &client_len);
                    
                    if (bytes_read < DATAGRAM_SIZE) continue;

                    string packet(buffer, DATAGRAM_SIZE);
                    
                    // Verify checksum
                    string received_hash = packet.substr(0, CHECKSUM_BYTES);
                    string data_to_hash = packet.substr(CHECKSUM_BYTES);
                    uint32_t computed_hash_val = compute_adler32(data_to_hash) % 1000000;
                    char str_computed_hash[CHECKSUM_BYTES + 1];
                    sprintf(str_computed_hash, "%0*d", CHECKSUM_BYTES, (int)computed_hash_val);
                    if (received_hash != string(str_computed_hash)) {
                        printf("[-] Server Login RDT: Hash corrupto.\n");
                        continue;
                    }

                    int data_start = CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES + SEQ_NUM_BYTES;
                    string payload = packet.substr(data_start, PAYLOAD_SIZE);
                    
                    if (payload[0] != 'L') {
                        if (payload[0] != 'A') {
                            printf("[-] Server Login RDT: Opcode incorrecto: %c\n", payload[0]);
                        }
                        continue;
                    }

                    // Parse seq_num and curr_segment
                    string str_total_seg   = packet.substr(CHECKSUM_BYTES, TOTAL_SEGMENTS_BYTES);
                    string str_curr_seg    = packet.substr(CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES, CURRENT_SEGMENT_BYTES);
                    string str_seq_num     = packet.substr(CHECKSUM_BYTES + TOTAL_SEGMENTS_BYTES + CURRENT_SEGMENT_BYTES, SEQ_NUM_BYTES);
                    
                    int seq_num = atoi(str_seq_num.c_str());
                    int curr_segment = atoi(str_curr_seg.c_str());

                    // Send ACK back to client
                    rdt_send_ack(server_sock, seq_num, curr_segment, &client_addr);

                    // Extract username
                    int username_len = atoi(payload.substr(1, SENDER_USERNAME_BYTES).c_str());
                    string username = payload.substr(1 + SENDER_USERNAME_BYTES, username_len);

                    int client_sock = -1;
                    {
                        std::lock_guard<std::mutex> lock(registry_write_mutex);
                        auto old_registry = std::atomic_load(&global_registry);

                        if (old_registry->cli_to_sock.size() >= MAX_CLIENTS) {
                            printf("[-] Servidor lleno. Rechazando a %s.\n", username.c_str());
                            
                            string err_msg = "El servidor está lleno.";
                            char err_len[SENDER_USERNAME_BYTES + 1];
                            sprintf(err_len, "%0*d", SENDER_USERNAME_BYTES, (int)err_msg.length());
                            string err_payload = "E" + string(err_len) + err_msg;
                            rdt_send_handshake_response(server_sock, client_addr, err_payload);
                            continue;
                        }

                        if (old_registry->cli_to_sock.find(username) != old_registry->cli_to_sock.end()) {
                            printf("[-] Usuario %s ya está conectado.\n", username.c_str());
                            
                            string err_msg = "Nombre de usuario ya en uso.";
                            char err_len[SENDER_USERNAME_BYTES + 1];
                            sprintf(err_len, "%0*d", SENDER_USERNAME_BYTES, (int)err_msg.length());
                            string err_payload = "E" + string(err_len) + err_msg;
                            rdt_send_handshake_response(server_sock, client_addr, err_payload);
                            continue;
                        }

                        client_sock = socket(AF_INET, SOCK_DGRAM, 0);
                        if (client_sock < 0) {
                            perror("Error al crear socket dedicado");
                            continue;
                        }
                        int opt_buf = 20 * 1024 * 1024;
                        setsockopt(client_sock, SOL_SOCKET, SO_RCVBUF, &opt_buf, sizeof(opt_buf));
                        setsockopt(client_sock, SOL_SOCKET, SO_SNDBUF, &opt_buf, sizeof(opt_buf));

                        int actual_rcv = 0, actual_snd = 0;
                        socklen_t opt_len = sizeof(actual_rcv);
                        getsockopt(client_sock, SOL_SOCKET, SO_RCVBUF, &actual_rcv, &opt_len);
                        getsockopt(client_sock, SOL_SOCKET, SO_SNDBUF, &actual_snd, &opt_len);
                        printf("[Diagnostic] Worker Server Socket Buffer: Requested=20.0MB, Actual_RCV=%.2fMB, Actual_SND=%.2fMB\n", 
                               actual_rcv / (1024.0 * 1024.0), actual_snd / (1024.0 * 1024.0));
                        fflush(stdout);

                        if (connect(client_sock, (struct sockaddr*)&client_addr, client_len) < 0) {
                            perror("Error en connect orientado a UDP");
                            close(client_sock);
                            continue;
                        }

                        auto new_registry = std::make_shared<ClientRegistry>(*old_registry);
                        new_registry->cli_to_sock[username] = client_sock;
                        new_registry->sock_to_cli[client_sock] = username;
                        std::atomic_store(&global_registry, std::shared_ptr<const ClientRegistry>(new_registry));
                    }

                    printf("[+] Nuevo usuario conectado: %s (Socket: %d)\n", username.c_str(), client_sock);
                    
                    // Retrieve local port of client_sock
                    struct sockaddr_in local_addr;
                    socklen_t local_len = sizeof(local_addr);
                    getsockname(client_sock, (struct sockaddr*)&local_addr, &local_len);
                    int ephemeral_port = ntohs(local_addr.sin_port);
                    
                    char port_str[32];
                    sprintf(port_str, "%d", ephemeral_port);
                    char port_len_str[SENDER_USERNAME_BYTES + 1];
                    sprintf(port_len_str, "%0*d", SENDER_USERNAME_BYTES, (int)strlen(port_str));
                    
                    string ok_payload = "K" + string(port_len_str) + string(port_str);
                    rdt_send_handshake_response(server_sock, client_addr, ok_payload);

                    FD_SET(client_sock, &current_sockets);
                    if (client_sock > max_socket_so_far) {
                        max_socket_so_far = client_sock;
                    }
                } 
                else {
                    string client_name = "desconocido";
                    {
                        auto registry = std::atomic_load(&global_registry);
                        auto it = registry->sock_to_cli.find(i);
                        if (it != registry->sock_to_cli.end()) {
                            client_name = it->second;
                        }
                    }
                    printf("[*] Actividad detectada en el cliente con fd: %d (%s)\n", i, client_name.c_str());

                    FD_CLR(i, &current_sockets);

                    try {
                        std::thread t(client_worker, i);
                        t.detach();
                    } catch (const std::exception& e) {
                        perror("Error creando hilo trabajador");
                        FD_SET(i, &current_sockets);
                    }
                }
            }
        }
    }

    close(server_sock);
    return 0;
}
