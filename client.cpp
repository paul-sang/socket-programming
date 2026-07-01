/*      Chat - Client

g++ client.cpp -o client -lpthread
./client 127.0.0.1 45000 Carlitos

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

#include <stdint.h>
#include <errno.h>

#include <iostream>
#include <string>
#include <vector>
#include <math.h>
#include <functional>
#include <map>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <condition_variable>
#include <memory>
#include <atomic>
#include <queue>
using namespace std;

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

struct SocketSession {
    int socket_fd;
    std::atomic<TransactionState*> active_tx{nullptr};
    RttState rtt_state;
    std::vector<char> last_file_buffer;
    int last_recv_seq_num = -1;
};

std::shared_ptr<SocketSession> get_session(int socket_fd) {
    static std::mutex session_init_mutex;
    static std::unordered_map<int, std::shared_ptr<SocketSession>> client_sessions;
    
    std::lock_guard<std::mutex> lock(session_init_mutex);
    auto it = client_sessions.find(socket_fd);
    if (it == client_sessions.end()) {
        auto session = std::make_shared<SocketSession>();
        session->socket_fd = socket_fd;
        client_sessions[socket_fd] = session;
        return session;
    }
    return it->second;
}

bool is_logged_in = false;
bool login_failed = false;
string login_error_msg = "";
std::mutex login_mutex;
std::condition_variable login_cv;

#define MAX_USERS 50
#define SERVER_PORT 45000
#define SERVER_IP "127.0.0.1"

//protocol metadata lengths
#define TARGET_USERNAME_BYTES 5
#define SENDER_USERNAME_BYTES 5
#define FILENAME_BYTES 3
#define FILESIZE_BYTES 22
#define SEQ_NUM_BYTES 6
#define TOTAL_SEGMENTS_BYTES 7
#define CURRENT_SEGMENT_BYTES 7
#define HEADER_OPCODE_BYTES 1
#define MESSAGE_BYTES 7
#define CHECKSUM_BYTES 6

#define SEGMENT_SIZE 500

int DATAGRAM_SIZE = 500;
constexpr int HEADER_SIZE = 26;
int PAYLOAD_SIZE = 474;
double INITIAL_WINDOW = -1.0;
int RECV_WINDOW_SIZE = 1024;

typedef struct {
    int SocketFD;
    char username[SENDER_USERNAME_BYTES * 8 + 1];
    volatile bool is_active;
    struct sockaddr_in server_addr;
    std::mutex* query_mutex;
    std::condition_variable* query_cv;
    int query_result;
} Client;


// --- BEGIN NETWORK LAYER ---
// --- SYSCALL, talk to me, baby! ---
int send_all(int socket, const char *buffer, int total_bytes) {
    int bytes_sent = 0;
    while (bytes_sent < total_bytes) {
        int result = send(socket, buffer + bytes_sent, total_bytes - bytes_sent, 0);
        if (result < 0) {
            if (errno == EINTR) continue; // Interrupción por señal, reintentar
            return -1;
        }
        if (result == 0) return 0; // Conexión cerrada
        bytes_sent += result;
    }
    return bytes_sent;
}

int recv_all(int socket, char *buffer, int total_bytes) {
    int bytes_read = 0;
    while (bytes_read < total_bytes) {
        int result = recv(socket, buffer + bytes_read, total_bytes - bytes_read, 0);
        if (result < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (result == 0) return 0; // El servidor cerró la conexión
        bytes_read += result;
    }
    return bytes_read;
}

// --- Helpers ---
// Helper para leer del socket: [000...][texto]
bool read_str(int socket, int len, char* output_buffer) {
    char header_buf[len + 1];
    memset(header_buf, 0, len + 1);

    if (recv_all(socket, header_buf, len) <= 0) return false;
    int str_len = atoi(header_buf);
    if (recv_all(socket, output_buffer, str_len) <= 0) return false;
    output_buffer[str_len] = '\0';
    
    return true;
}
//helper disconnect client
// TODO: recv a message from server
void logout(Client *client) {
}

void print_menu() {
    sleep(1);
    printf("\n\r\033[K--- Select an option---\n");
    printf("1. Send Broadcast\n");
    printf("2. Send Unicast\n");
    printf("3. List users\n");
    printf("4. Send File\n");
    printf("5. Logout\n");
    printf("6. Start Federated Learning\n");
    printf("> ");
    fflush(stdout);
}


inline bool udp_send(int socket_fd, const char* data, size_t size) {
    int res;
    while (true) {
        res = send(socket_fd, data, size, 0);
        if (res >= 0) break;
        if (errno == ECONNREFUSED || errno == EPIPE || errno == EBADF || errno == ENOTCONN) {
            printf("[-] udp_send error on fd %d: %s\n", socket_fd, strerror(errno));
            fflush(stdout);
            return false;
        }
        if (errno != ENOBUFS && errno != EAGAIN && errno != EWOULDBLOCK) {
            printf("[-] udp_send error on fd %d: %s\n", socket_fd, strerror(errno));
            fflush(stdout);
            break;
        }
        std::this_thread::yield();
    }
    return true;
}

bool udp_send(int socket_fd, const string& str) {
    return udp_send(socket_fd, str.data(), str.size());
}

int udp_recv_buf(int socket_fd, char* buffer) {
    int bytes_received = recv(socket_fd, buffer, DATAGRAM_SIZE, 0);
    if (bytes_received <= 0) {
        if (bytes_received < 0) {
            printf("[-] recv error on fd %d: %s\n", socket_fd, strerror(errno));
        }
        return -1;
    }
    return bytes_received;
}
// --- END NETWORK LAYER ---

// --- BEGIN TRANSPORT LAYER ---
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

struct FileDataSource : public DataSource {
    string header;
    string filepath;
    mutable FILE* file;
    size_t file_len;
    size_t total_len;

    static const size_t BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB
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

void rdt_send(int socket_fd, const DataSource& source) {
    auto session = get_session(socket_fd);
    auto start_prep = std::chrono::steady_clock::now();
    int total_segments = ceil((double)source.size() / PAYLOAD_SIZE);
    if (total_segments == 0) total_segments = 1;
    
    static std::atomic<int> next_seq_num{1};
    int seq_num = next_seq_num.fetch_add(1);

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

void rdt_send(int socket_fd, const string& payload) {
    MemoryDataSource source(payload);
    rdt_send(socket_fd, source);
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
        bytes = recv(socket_fd, packet, DATAGRAM_SIZE, 0);
        if (bytes <= 0) return "";

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
            if (seq_num == session->last_recv_seq_num) {
                continue;
            }
            expected_total_segments = total_segments;
            target_seq_num = seq_num;
            session->last_recv_seq_num = seq_num;
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
// --- END TRANSPORT LAYER ---

// --- BEGIN APP LAYER ---
// --- HELPERS FRAMING ---

char* read_str() {
    char *b = NULL; size_t s = 0;
    if (getline(&b, &s, stdin) == -1) { free(b); return NULL; }
    b[strcspn(b, "\n")] = '\0';
    return b;
}

// --- PROTOCOLO (Serialización / Envío) ---
void app_send_login(Client *client) {
    char sender_len[SENDER_USERNAME_BYTES + 1];
    sprintf(sender_len, "%0*d", SENDER_USERNAME_BYTES, (int)strlen(client->username));
    
    string payload = "L" + string(sender_len) + string(client->username);
    
    // Connect to server main port
    if (connect(client->SocketFD, (struct sockaddr*)&client->server_addr, sizeof(client->server_addr)) < 0) {
        perror("Error connecting to server main port");
        exit(EXIT_FAILURE);
    }
    
    rdt_send(client->SocketFD, payload);
    
    std::unique_lock<std::mutex> lock(login_mutex);
    while (!is_logged_in && !login_failed && client->is_active) {
        login_cv.wait_for(lock, std::chrono::seconds(5));
        if (!is_logged_in && !login_failed) {
            printf("[*] Waiting for login response...\n");
        }
    }
    
    if (login_failed) {
        printf("[-] Login failed: %s\n", login_error_msg.c_str());
        exit(EXIT_FAILURE);
    }
}

void app_send_list(Client *client) {
    string payload = "T";
    rdt_send(client->SocketFD, payload);
}

void app_send_broadcast(Client *client){
    printf("Type a message to broadcast: ");
    char *msg = read_str();
    if (!msg) return;
    
    char msg_len[SENDER_USERNAME_BYTES + 1];
    sprintf(msg_len, "%0*d", SENDER_USERNAME_BYTES, (int)strlen(msg));
    
    string payload = "B" + string(msg_len) + string(msg);
    rdt_send(client->SocketFD, payload);
    free(msg);
}

void app_send_unicast(Client *client) {
    printf("Type a target user: ");
    char *target = read_str();
    if (!target) return;
    
    char target_len[TARGET_USERNAME_BYTES + 1];
    sprintf(target_len, "%0*d", TARGET_USERNAME_BYTES, (int)strlen(target));



    printf("Type a message: ");
    char *msg = read_str();
    if (!msg) {
        free(target);
        return;
    }
    
    char sender_len[SENDER_USERNAME_BYTES + 1];
    char msg_len[SENDER_USERNAME_BYTES + 1];
    
    sprintf(sender_len, "%0*d", SENDER_USERNAME_BYTES, (int)strlen(client->username));
    sprintf(msg_len, "%0*d", SENDER_USERNAME_BYTES, (int)strlen(msg));
    
    string payload = "U" + string(target_len) + target + string(sender_len) + client->username + string(msg_len) + msg;
    rdt_send(client->SocketFD, payload);
    
    free(target);
    free(msg);
}


void app_send_binary(Client *client, char opcode, const string& target, const string& filepath) {
    FILE *file = fopen(filepath.c_str(), "rb");
    if (!file) { printf("[-] No se pudo abrir el archivo.\n"); return; }
    fseek(file, 0, SEEK_END);
    long long file_size = ftell(file);
    fclose(file);

    size_t last_slash = filepath.find_last_of("/\\");
    string clean_filename = (last_slash == string::npos) ? filepath : filepath.substr(last_slash + 1);

    char target_len[TARGET_USERNAME_BYTES + 1];
    char filename_len[FILENAME_BYTES + 1];
    char sender_len[SENDER_USERNAME_BYTES + 1];
    char filesize_len[FILESIZE_BYTES + 1];

    sprintf(target_len, "%0*d", TARGET_USERNAME_BYTES, (int)target.length());
    sprintf(filename_len, "%0*d", FILENAME_BYTES, (int)clean_filename.length());
    sprintf(sender_len, "%0*d", SENDER_USERNAME_BYTES, (int)strlen(client->username));
    sprintf(filesize_len, "%0*lld", FILESIZE_BYTES, file_size);

    string header;
    header.append(1, opcode);
    header.append(target_len);
    header.append(target);
    header.append(filename_len);
    header.append(clean_filename); // Use clean filename
    header.append(sender_len);
    header.append(client->username);
    header.append(filesize_len);

    std::thread([socket_fd = client->SocketFD, header, filepath, file_size]() {
        FileDataSource source(header, filepath, file_size);
        rdt_send(socket_fd, source);
    }).detach();
}

void app_start_fl_master(Client *client) {
    printf("[FL] Iniciando Aprendizaje Federado Maestro...\n");
    app_send_binary(client, 'M', "server", "model/dataset.csv");
    printf("[FL] Dataset enviado al servidor. Esperando pesos globales...\n");
}

void app_send_file(Client *client) {
    // keyboard input: target user + file path
    printf("Type a target user: ");
    char *target = read_str();
    if (!target) return;

    char target_len[TARGET_USERNAME_BYTES + 1];
    sprintf(target_len, "%0*d", TARGET_USERNAME_BYTES, (int)strlen(target));



    printf("Type a file path: ");
    char *filepath = read_str();

    auto start_ram = std::chrono::steady_clock::now();
    FILE *file = fopen(filepath, "rb");
    if (!file) { printf("[-] No se pudo abrir el archivo.\n"); free(target); free(filepath); return; }

    fseek(file, 0, SEEK_END);
    long long file_size = ftell(file);
    fclose(file); // Closed immediately to prevent memory hold

    if (file_size > 4739999526LL) {
        printf("[-] Archivo demasiado grande (máximo 4.73 GB).\n");
        free(target);
        free(filepath);
        return;
    }

    auto end_ram = std::chrono::steady_clock::now();
    double ram_time = std::chrono::duration<double, std::milli>(end_ram - start_ram).count();
    printf("[Timing] Size Check: Determined size %lld bytes in %.2f ms\n", file_size, ram_time);

    auto start_app_payload = std::chrono::steady_clock::now();
    char opcode[2] = "F";
    char filename_len[FILENAME_BYTES + 1];
    char sender_len[SENDER_USERNAME_BYTES + 1];
    char filesize_len[FILESIZE_BYTES + 1];

    sprintf(target_len, "%0*d", TARGET_USERNAME_BYTES, (int)strlen(target));
    sprintf(filename_len, "%0*d", FILENAME_BYTES, (int)strlen(filepath));
    sprintf(sender_len, "%0*d", SENDER_USERNAME_BYTES, (int)strlen(client->username));
    sprintf(filesize_len, "%0*lld", FILESIZE_BYTES, file_size);

    string header;
    header.append(opcode);
    header.append(target_len);
    header.append(target);
    header.append(filename_len);
    header.append(filepath);
    header.append(sender_len);
    header.append(client->username);
    header.append(filesize_len);

    auto end_app_payload = std::chrono::steady_clock::now();
    double app_payload_time = std::chrono::duration<double, std::milli>(end_app_payload - start_app_payload).count();
    printf("[Timing] App Payload: Constructed header payload in %.2f ms\n", app_payload_time);

    FileDataSource source(header, filepath, file_size);
    rdt_send(client->SocketFD, source);

    free(target);
    free(filepath);
    fflush(stdout);
}

void app_send_logout(Client *client) {
    string str_logout_payload = "O";
    rdt_send(client->SocketFD, str_logout_payload);
    client->is_active = false;
    shutdown(client->SocketFD, SHUT_RDWR);
    close(client->SocketFD);
}


void app_recv_ok(Client *client, const string& full_payload) {
    size_t parse_offset = 1;
    int port_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string port_str = full_payload.substr(parse_offset, port_len);
    int ephemeral_port = atoi(port_str.c_str());
    
    struct sockaddr_in dedicated_addr = client->server_addr;
    dedicated_addr.sin_port = htons(ephemeral_port);
    if (connect(client->SocketFD, (struct sockaddr*)&dedicated_addr, sizeof(dedicated_addr)) < 0) {
        perror("Error reconnecting to dedicated port");
    }
    
    std::unique_lock<std::mutex> lock(login_mutex);
    is_logged_in = true;
    login_cv.notify_all();
}

void app_recv_error(Client *client, const string& full_payload) {
    size_t parse_offset = 1;
    int err_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string err_msg = full_payload.substr(parse_offset, err_len);
    
    bool logged_in;
    {
        std::lock_guard<std::mutex> lock(login_mutex);
        logged_in = is_logged_in;
    }
    if (!logged_in) {
        std::unique_lock<std::mutex> lock(login_mutex);
        login_failed = true;
        login_error_msg = err_msg;
        login_cv.notify_all();
    } else {
        printf("\r\033[K[-] Error from server: %s\n> ", err_msg.c_str());
        fflush(stdout);
    }
}

void app_recv_broadcast(Client *client, const string& full_payload) {
    size_t parse_offset = 1;
    int sender_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string sender = full_payload.substr(parse_offset, sender_len);
    parse_offset += sender_len;
    
    int msg_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string msg = full_payload.substr(parse_offset, msg_len);
    
    printf("\r\033[K[Broadcast] %s: %s\n> ", sender.c_str(), msg.c_str());
    fflush(stdout);
}

void app_recv_unicast(Client *client, const string& full_payload) {
    size_t parse_offset = 1;
    int sender_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string sender = full_payload.substr(parse_offset, sender_len);
    parse_offset += sender_len;
    
    int msg_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string msg = full_payload.substr(parse_offset, msg_len);
    
    printf("\r\033[K[Private from %s]: %s\n> ", sender.c_str(), msg.c_str());
    fflush(stdout);
}

void app_recv_list(Client *client, const string& full_payload) {
    size_t parse_offset = 1;
    int list_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string list_str = full_payload.substr(parse_offset, list_len);
    
    printf("\r\033[K[Users Online]: %s\n> ", list_str.c_str());
    fflush(stdout);
}


void app_recv_W_dataset(Client *client, const string& full_payload) {
    size_t parse_offset = 1; 
    int target_len = atoi(full_payload.substr(parse_offset, TARGET_USERNAME_BYTES).c_str());
    parse_offset += TARGET_USERNAME_BYTES;
    string target = full_payload.substr(parse_offset, target_len);
    parse_offset += target_len;

    int filename_len = atoi(full_payload.substr(parse_offset, FILENAME_BYTES).c_str());
    parse_offset += FILENAME_BYTES;
    string filepath = full_payload.substr(parse_offset, filename_len);
    parse_offset += filename_len;

    int sender_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string sender = full_payload.substr(parse_offset, sender_len);
    parse_offset += sender_len;

    long long file_size = atoll(full_payload.substr(parse_offset, FILESIZE_BYTES).c_str());
    parse_offset += FILESIZE_BYTES;

    string save_path = "descargas_dataset.csv";

    FILE *out_file = fopen(save_path.c_str(), "wb");
    if (!out_file) {
        printf("\n[-] FL Error: No se pudo escribir dataset local.\n");
        return;
    }
    auto session = get_session(client->SocketFD);
    if (session->last_file_buffer.size() >= parse_offset + file_size) {
        fwrite(session->last_file_buffer.data() + parse_offset, 1, file_size, out_file);
    }
    fclose(out_file);
    session->last_file_buffer.clear();
    session->last_file_buffer.shrink_to_fit();
    printf("\n[FL] Dataset local guardado.\n> ");
    fflush(stdout);
}

void app_recv_R_range(Client *client, const string& full_payload) {
    size_t parse_offset = 1;
    int msg_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string range_str = full_payload.substr(parse_offset, msg_len);
    
    printf("\n[FL] Rango asignado recibido: %s. Entrenando...\n> ", range_str.c_str());
    fflush(stdout);
    
    // Parse range "0_2000"
    size_t underscore = range_str.find('_');
    if (underscore != string::npos) {
        string start_idx = range_str.substr(0, underscore);
        string end_idx = range_str.substr(underscore + 1);
        
        string cmd = "python3 model/fl_train.py descargas_dataset.csv " + start_idx + " " + end_idx + " mis_pesos.pt";
        if (system(cmd.c_str()) != 0) {
             printf("[FL] Error ejecutando entrenamiento local.\n");
        }
        
        printf("\n[FL] Entrenamiento terminado. Enviando pesos al servidor...\n> ");
        app_send_binary(client, 'P', "server", "mis_pesos.pt");
    }
}

void app_recv_G_global_weights(Client *client, const string& full_payload) {
    size_t parse_offset = 1; 
    // skip target
    int target_len = atoi(full_payload.substr(parse_offset, TARGET_USERNAME_BYTES).c_str());
    parse_offset += TARGET_USERNAME_BYTES + target_len;
    // skip filename
    int filename_len = atoi(full_payload.substr(parse_offset, FILENAME_BYTES).c_str());
    parse_offset += FILENAME_BYTES + filename_len;
    // skip sender
    int sender_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES + sender_len;
    
    long long file_size = atoll(full_payload.substr(parse_offset, FILESIZE_BYTES).c_str());
    parse_offset += FILESIZE_BYTES;

    FILE *out_file = fopen("descargas_global_weights.pt", "wb");
    if (out_file) {
        auto session = get_session(client->SocketFD);
        fwrite(session->last_file_buffer.data() + parse_offset, 1, file_size, out_file);
        fclose(out_file);
        session->last_file_buffer.clear();
        session->last_file_buffer.shrink_to_fit();
        
        printf("\n[FL] Pesos Globales recibidos. Evaluando modelo...\n");
        if (system("python3 model/fl_test.py descargas_global_weights.pt model/dataset.csv 8000 10000") != 0) {
             printf("[FL] Error ejecutando evaluacion.\n");
        }
    }
}

void app_recv_file(Client *client, const string& full_payload) {
    size_t parse_offset = 1; 

    int target_len = atoi(full_payload.substr(parse_offset, TARGET_USERNAME_BYTES).c_str());
    parse_offset += TARGET_USERNAME_BYTES;
    string target = full_payload.substr(parse_offset, target_len);
    parse_offset += target_len;

    int filename_len = atoi(full_payload.substr(parse_offset, FILENAME_BYTES).c_str());
    parse_offset += FILENAME_BYTES;
    string filepath = full_payload.substr(parse_offset, filename_len);
    parse_offset += filename_len;

    int sender_len = atoi(full_payload.substr(parse_offset, SENDER_USERNAME_BYTES).c_str());
    parse_offset += SENDER_USERNAME_BYTES;
    string sender = full_payload.substr(parse_offset, sender_len);
    parse_offset += sender_len;

    long long file_size = atoll(full_payload.substr(parse_offset, FILESIZE_BYTES).c_str());
    parse_offset += FILESIZE_BYTES;

    size_t last_slash = filepath.find_last_of("/\\");
    string clean_filename = (last_slash == string::npos) ? filepath : filepath.substr(last_slash + 1);
    string save_path = "descargas_" + clean_filename;

    if (is_sink_mode) {
        printf("\\n[Sink] Recibidos %lld bytes de archivo de '%s' (Descartando de RAM sin escribir a disco).\\n> ", file_size, sender.c_str());
        fflush(stdout);
        auto session = get_session(client->SocketFD);
        session->last_file_buffer.clear();
        session->last_file_buffer.shrink_to_fit();
        return;
    }

    auto start_write = std::chrono::steady_clock::now();
    FILE *out_file = fopen(save_path.c_str(), "wb");
    if (!out_file) {
        printf("\n[-] Error de aplicación: No se pudo escribir el archivo en disco.\n");
        return;
    }
    
    auto session = get_session(client->SocketFD);
    if (session->last_file_buffer.size() >= parse_offset + file_size) {
        fwrite(session->last_file_buffer.data() + parse_offset, 1, file_size, out_file);
    } else {
        printf("[-] Error: El buffer en RAM es menor que el tamaño esperado del archivo.\n");
    }
    fclose(out_file);
    session->last_file_buffer.clear();
    session->last_file_buffer.shrink_to_fit();

    auto end_write = std::chrono::steady_clock::now();
    double write_time = std::chrono::duration<double, std::milli>(end_write - start_write).count();

    printf("\n[Timing] Disk Write: Wrote %lld bytes to '%s' in %.2f ms\n", file_size, save_path.c_str(), write_time);
    printf("[+] Archivo '%s' (%lld bytes) recibido correctamente de '%s'.\n> ", 
           clean_filename.c_str(), file_size, sender.c_str());
    fflush(stdout);
}
// --- END PROTOCOLO ---

// --- THREADS ---
void listener_worker(Client* client) {
    unsigned char opcode;
    
    while(client->is_active) {
        string protocol = rdt_recv(client->SocketFD);
        
        if (protocol.empty()) { 
            client->is_active = false;
            break; 
        }

        opcode = protocol[0];
        switch (opcode){
            case 'B': app_recv_broadcast(client, protocol); break;
            case 'U': app_recv_unicast(client, protocol); break;
            case 'T': app_recv_list(client, protocol); break;
            case 'F': app_recv_file(client, protocol); break;
            case 'K': app_recv_ok(client, protocol); break;
            case 'E': app_recv_error(client, protocol); break;
            case 'Y':
            case 'N': {
                std::lock_guard<std::mutex> lock(*(client->query_mutex));
                client->query_result = (opcode == 'Y') ? 1 : 0;
                client->query_cv->notify_one();
                break;
            }
            case 'W': app_recv_W_dataset(client, protocol); break;
            case 'R': app_recv_R_range(client, protocol); break;
            case 'G': app_recv_G_global_weights(client, protocol); break;
            default: 
                printf("\r\033[K[!] Código desconocido recibido: %c\n> ", opcode);
                fflush(stdout);
                break;
        }
    }
}

void sender_worker(Client* client) {
    while (client->is_active) {
        print_menu();
        char *choice_str = read_str();
        if (!choice_str) {
            client->is_active = false;
            break;
        }
        
        int choice = atoi(choice_str);
        free(choice_str);
        
        if (!client->is_active) break;
        
        switch (choice) {
            case 1: app_send_broadcast(client); break;
            case 2: app_send_unicast(client); break;
            case 3: app_send_list(client); break;
            case 4: app_send_file(client); break;
            case 5: app_send_logout(client); break;
            case 6: app_start_fl_master(client); break;
            default: printf("Invalid option. Please try again.\n"); break;
        }
    }
}

// --- CONFIGURACIÓN INICIAL ---
void connect_to_server(Client* client, const char* server_ip, int server_port) {
    client->SocketFD = socket(AF_INET, SOCK_DGRAM, 0);
    if (client->SocketFD == -1) {
        perror(" No se pudo crear el socket");
        exit(EXIT_FAILURE);
    }
    int opt_buf = 20 * 1024 * 1024;
    setsockopt(client->SocketFD, SOL_SOCKET, SO_RCVBUF, &opt_buf, sizeof(opt_buf));
    setsockopt(client->SocketFD, SOL_SOCKET, SO_SNDBUF, &opt_buf, sizeof(opt_buf));

    int actual_rcv = 0, actual_snd = 0;
    socklen_t opt_len = sizeof(actual_rcv);
    getsockopt(client->SocketFD, SOL_SOCKET, SO_RCVBUF, &actual_rcv, &opt_len);
    getsockopt(client->SocketFD, SOL_SOCKET, SO_SNDBUF, &actual_snd, &opt_len);
    printf("[Diagnostic] Client Socket Buffer: Requested=20.0MB, Actual_RCV=%.2fMB, Actual_SND=%.2fMB\n", 
           actual_rcv / (1024.0 * 1024.0), actual_snd / (1024.0 * 1024.0));
    fflush(stdout);

    struct sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = INADDR_ANY;
    local_addr.sin_port = 0; // OS chooses ephemeral port
    if (::bind(client->SocketFD, (struct sockaddr*)&local_addr, sizeof(local_addr)) < 0) {
        perror("Error binding socket locally");
        close(client->SocketFD);
        exit(EXIT_FAILURE);
    }

    memset(&client->server_addr, 0, sizeof(struct sockaddr_in));
    client->server_addr.sin_family = AF_INET;
    client->server_addr.sin_port = htons(server_port);
    
    if (inet_pton(AF_INET, server_ip, &client->server_addr.sin_addr) <= 0) {
        perror("Dirección IP no válida");
        close(client->SocketFD);
        exit(EXIT_FAILURE);
    }
    
    client->is_active = true;
    printf("[*] Configuración inicial UDP completada para %s:%d\n", server_ip, server_port);
}

// --- END APP LAYER ---


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

    if (clean_argc < 4) {
        printf("Uso : %s <IP_Servidor> <Puerto> <TuUsuario> [--send <target> <filepath> | --recv] [--sink] [-p <packet_size>] [-w <window_size>] [-rw <recv_window>]\n", clean_argv[0]);
        printf("Ejem: %s 127.0.0.1 45000 Carlitos\n", clean_argv[0]);
        return EXIT_FAILURE;
    }

    for (int i = 4; i < clean_argc; i++) {
        if (string(clean_argv[i]) == "--sink") {
            is_sink_mode = true;
            // clean the arg so it doesn't break mode parsing
            clean_args.erase(clean_args.begin() + i);
            clean_argc--;
            i--;
        }
    }
    clean_argv = clean_args.data();

    const char* server_ip = clean_argv[1];
    int server_port = atoi(clean_argv[2]);
    const char* client_username = clean_argv[3];
    
    bool is_recv_mode = false;
    bool is_send_mode = false;
    string target_user = "";
    string file_path = "";

    if (clean_argc >= 5) {
        string mode = clean_argv[4];
        if (mode == "--recv") {
            is_recv_mode = true;
        } else if (mode == "--send" && clean_argc == 7) {
            is_send_mode = true;
            target_user = clean_argv[5];
            file_path = clean_argv[6];
        } else {
            printf("Uso extendido:\n");
            printf("  Recibir: %s <IP> <Puerto> <Usuario> --recv [--sink] [-p <packet_size>] [-w <window_size>] [-rw <recv_window>]\n", clean_argv[0]);
            printf("  Enviar:  %s <IP> <Puerto> <Usuario> --send <Target> <FilePath> [-p <packet_size>] [-w <window_size>] [-rw <recv_window>]\n", clean_argv[0]);
            return EXIT_FAILURE;
        }
    }

    Client client;
    memset(&client, 0, sizeof(Client));
    client.query_mutex = new std::mutex();
    client.query_cv = new std::condition_variable();
    client.query_result = -1;
    strncpy(client.username, client_username, sizeof(client.username) - 1);
    client.username[sizeof(client.username) - 1] = '\0';

    connect_to_server(&client, server_ip, server_port);

    // Launch listener thread first so it's always active and handles handshake
    std::thread listener_thread(listener_worker, &client);

    app_send_login(&client);

    if (is_send_mode) {
        // Enviar archivo modo automático
        auto start_ram = std::chrono::steady_clock::now();
        FILE *file = fopen(file_path.c_str(), "rb");
        if (!file) {
            printf("[-] No se pudo abrir el archivo %s\n", file_path.c_str());
            return EXIT_FAILURE;
        }
        fseek(file, 0, SEEK_END);
        long long file_size = ftell(file);
        fclose(file); // Closed immediately to prevent memory hold

        if (file_size > 4739999526LL) {
            printf("[-] Archivo demasiado grande (máximo 4.73 GB).\n");
            return EXIT_FAILURE;
        }

        auto end_ram = std::chrono::steady_clock::now();
        double ram_time = std::chrono::duration<double, std::milli>(end_ram - start_ram).count();
        printf("[Timing] Size Check: Determined size %lld bytes in %.2f ms\n", file_size, ram_time);

        auto start_app_payload = std::chrono::steady_clock::now();
        char opcode[2] = "F";
        char target_len[TARGET_USERNAME_BYTES + 1];
        char filename_len[FILENAME_BYTES + 1];
        char sender_len[SENDER_USERNAME_BYTES + 1];
        char filesize_len[FILESIZE_BYTES + 1];

        sprintf(target_len, "%0*d", TARGET_USERNAME_BYTES, (int)target_user.length());
        sprintf(filename_len, "%0*d", FILENAME_BYTES, (int)file_path.length());
        sprintf(sender_len, "%0*d", SENDER_USERNAME_BYTES, (int)strlen(client.username));
        sprintf(filesize_len, "%0*lld", FILESIZE_BYTES, file_size);

        string header;
        header.append(opcode);
        header.append(target_len);
        header.append(target_user);
        header.append(filename_len);
        header.append(file_path);
        header.append(sender_len);
        header.append(client.username);
        header.append(filesize_len);

        auto end_app_payload = std::chrono::steady_clock::now();
        double app_payload_time = std::chrono::duration<double, std::milli>(end_app_payload - start_app_payload).count();
        printf("[Timing] App Payload: Constructed header payload in %.2f ms\n", app_payload_time);

        if (file_size < 512 * 1024 * 1024LL) {
            FILE *f_in = fopen(file_path.c_str(), "rb");
            string full_data = header;
            full_data.resize(header.size() + file_size);
            size_t bytes_read = fread(&full_data[header.size()], 1, file_size, f_in);
            fclose(f_in);
            
            MemoryDataSource source(full_data);
            rdt_send(client.SocketFD, source);
        } else {
            FileDataSource source(header, file_path, file_size);
            rdt_send(client.SocketFD, source);
        }

        // logout
        app_send_logout(&client);
        printf("[+] Automated send completed.\n");
        if (listener_thread.joinable()) {
            listener_thread.join();
        }
        return 0;
    }

    if (is_recv_mode) {
        printf("[*] Automated receiver mode started. Waiting for files...\n");
        if (listener_thread.joinable()) {
            listener_thread.join();
        }
        close(client.SocketFD);
        return 0;
    }

    std::thread sender_thread(sender_worker, &client);

    if (sender_thread.joinable()) {
        sender_thread.join();
    }
    if (listener_thread.joinable()) {
        listener_thread.join();
    }
    
    close(client.SocketFD);
    printf("\n[*] Cliente finalizado \n");
    
    return 0;
}