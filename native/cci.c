#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#ifndef CCI_VERSION
#define CCI_VERSION "dev"
#endif

enum {
    frame_request = 'q',
    frame_stdout = 'o',
    frame_stderr = 'e',
    frame_exit = 'x',
    frame_fallback = 'f',
    frame_need_in = 'r',
    frame_ack = 'a',
    frame_stdin = 'i',
};

enum {
    header_bytes = 5,
    read_bytes = 64 << 10,
    max_frame_bytes = 16 << 20,
    lost_exit = 75,
};

static const int64_t redial_for_ns = 30000000000LL;
static const int64_t redial_every_ns = 100000000LL;
static const int64_t nanos_per_ms = 1000000LL;

extern char **environ;

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} text;

typedef struct {
    int conn;
    bool wired;
    bool reading;
    bool eof;
    bool sent;
    char **held;
    size_t *held_len;
    size_t held_count;
} pump;

typedef struct {
    int code;
    bool ran;
    bool lost;
} outcome;

typedef struct {
    int fd;
    char data[read_bytes];
    size_t at;
    size_t end;
    pump *in;
    int err;
    bool eof;
} reader;

static void *grown(void *old, size_t size) {
    void *out = realloc(old, size);
    if (out == NULL) {
        abort();
    }
    return out;
}

static void reserve(text *t, size_t more) {
    if (t->len + more <= t->cap) {
        return;
    }
    t->cap = (t->len + more) * 2;
    t->data = grown(t->data, t->cap);
}

static void append(text *t, const char *s, size_t n) {
    reserve(t, n);
    memcpy(t->data + t->len, s, n);
    t->len += n;
}

static void append_str(text *t, const char *s) {
    append(t, s, strlen(s));
}

static size_t valid_utf8(const unsigned char *s) {
    unsigned char c = s[0];
    if (c < 0x80) {
        return 1;
    }
    if (c >= 0xC2 && c <= 0xDF) {
        return (s[1] & 0xC0) == 0x80 ? 2 : 0;
    }
    if (c >= 0xE0 && c <= 0xEF) {
        unsigned char low = c == 0xE0 ? 0xA0 : 0x80;
        unsigned char high = c == 0xED ? 0x9F : 0xBF;
        return s[1] >= low && s[1] <= high && (s[2] & 0xC0) == 0x80 ? 3 : 0;
    }
    if (c >= 0xF0 && c <= 0xF4) {
        unsigned char low = c == 0xF0 ? 0x90 : 0x80;
        unsigned char high = c == 0xF4 ? 0x8F : 0xBF;
        return s[1] >= low && s[1] <= high && (s[2] & 0xC0) == 0x80 && (s[3] & 0xC0) == 0x80 ? 4 : 0;
    }
    return 0;
}

static void append_json(text *t, const char *s) {
    static const char hex[] = "0123456789abcdef";
    const unsigned char *p = (const unsigned char *)s;
    reserve(t, strlen(s) + 2);
    t->data[t->len++] = '"';
    while (*p != 0) {
        if (*p == '"' || *p == '\\') {
            char escaped[2] = {'\\', (char)*p};
            append(t, escaped, 2);
            p++;
        } else if (*p < 0x20) {
            char escaped[6] = {'\\', 'u', '0', '0', hex[*p >> 4], hex[*p & 15]};
            append(t, escaped, 6);
            p++;
        } else {
            size_t n = valid_utf8(p);
            if (n == 0) {
                append_str(t, "\\ufffd");
                p++;
            } else {
                append(t, (const char *)p, n);
                p += n;
            }
        }
    }
    append(t, "\"", 1);
}

static void append_list(text *t, char **items) {
    append(t, "[", 1);
    for (char **item = items; *item != NULL; item++) {
        if (item != items) {
            append(t, ",", 1);
        }
        append_json(t, *item);
    }
    append(t, "]", 1);
}

static int64_t monotonic_ns(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (int64_t)now.tv_sec * 1000000000LL + now.tv_nsec;
}

static bool starts(const char *s, const char *prefix) {
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

static bool device_path(const char *s) {
    return starts(s, "/dev/") || starts(s, "/proc/");
}

static bool relayed(int argc, char **argv) {
    static const char *const commands[] = {"post", "tail", "grep", "show", "digest", "who", "watch", "channel", "window", "embed", NULL};
    static const char *const hooks[] = {"post-tool-use", "stop", "subagent-stop", "prompt", NULL};
    if (argc < 2) {
        return false;
    }
    bool known = false;
    if (strcmp(argv[1], "hook") == 0) {
        for (const char *const *hook = hooks; argc >= 3 && *hook != NULL; hook++) {
            known = known || strcmp(argv[2], *hook) == 0;
        }
    } else {
        for (const char *const *command = commands; *command != NULL; command++) {
            known = known || strcmp(argv[1], *command) == 0;
        }
    }
    if (!known) {
        return false;
    }
    for (int i = 2; i < argc; i++) {
        const char *value = strchr(argv[i], '=');
        if (device_path(argv[i]) || value != NULL && device_path(value + 1)) {
            return false;
        }
    }
    return true;
}

static int count(char **items) {
    int n = 0;
    while (items[n] != NULL) {
        n++;
    }
    return n;
}

static bool has(char **argv, const char *arg) {
    for (; *argv != NULL; argv++) {
        if (strcmp(*argv, arg) == 0) {
            return true;
        }
    }
    return false;
}

static bool stream(char **argv) {
    int argc = count(argv);
    return argc > 2 && strcmp(argv[1], "embed") == 0 && strcmp(argv[2], "follow") == 0 ||
           argc > 1 && (strcmp(argv[1], "watch") == 0 || strcmp(argv[1], "channel") == 0);
}

static bool channel(char **argv) {
    return count(argv) > 1 && strcmp(argv[1], "channel") == 0;
}

static char **with_arg(char **argv, char *arg) {
    int argc = count(argv);
    char **out = grown(NULL, (size_t)(argc + 2) * sizeof(char *));
    memcpy(out, argv, (size_t)argc * sizeof(char *));
    out[argc] = arg;
    out[argc + 1] = NULL;
    return out;
}

static char **resumable(char **argv) {
    if (strcmp(argv[1], "watch") != 0 || !stream(argv)) {
        return argv;
    }
    for (char **arg = argv; *arg != NULL; arg++) {
        if (strcmp(*arg, "--cursor") == 0 || starts(*arg, "--cursor=")) {
            return argv;
        }
    }
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    char *cursor = grown(NULL, 64);
    snprintf(cursor, 64, "--cursor=watch-%d-%lld", (int)getpid(), (long long)now.tv_sec * 1000000000LL + now.tv_nsec);
    return with_arg(argv, cursor);
}

static char **resumed(char **argv) {
    if (!channel(argv) || has(argv, "--resumed")) {
        return argv;
    }
    return with_arg(argv, "--resumed");
}

static bool parse_duration(const char *s, int64_t *out) {
    static const struct {
        const char *name;
        double nanos;
    } units[] = {{"ns", 1}, {"us", 1e3}, {"\xC2\xB5s", 1e3}, {"\xCE\xBCs", 1e3}, {"ms", 1e6}, {"s", 1e9}, {"m", 60e9}, {"h", 3600e9}};
    double sign = 1;
    double total = 0;
    if (*s == '-' || *s == '+') {
        sign = *s == '-' ? -1 : 1;
        s++;
    }
    if (strcmp(s, "0") == 0) {
        *out = 0;
        return true;
    }
    if (*s == 0) {
        return false;
    }
    while (*s != 0) {
        char *end;
        if (!(*s == '.' || *s >= '0' && *s <= '9')) {
            return false;
        }
        double value = strtod(s, &end);
        if (end == s) {
            return false;
        }
        s = end;
        size_t matched = 0;
        double nanos = 0;
        for (size_t i = 0; i < sizeof(units) / sizeof(units[0]); i++) {
            size_t n = strlen(units[i].name);
            if (n > matched && strncmp(s, units[i].name, n) == 0) {
                matched = n;
                nanos = units[i].nanos;
            }
        }
        if (matched == 0) {
            return false;
        }
        total += value * nanos;
        s += matched;
    }
    *out = (int64_t)(sign * total);
    return true;
}

static char *left_of(const char *prefix, const char *given, int64_t started) {
    int64_t bound;
    if (!parse_duration(given, &bound)) {
        return NULL;
    }
    int64_t left = bound - (monotonic_ns() - started);
    if (left < nanos_per_ms) {
        left = nanos_per_ms;
    }
    char *out = grown(NULL, 64);
    snprintf(out, 64, "%s%lldns", prefix, (long long)left);
    return out;
}

static char **for_what_is_left(char **argv, int64_t started) {
    int argc = count(argv);
    char **out = grown(NULL, (size_t)(argc + 1) * sizeof(char *));
    memcpy(out, argv, (size_t)(argc + 1) * sizeof(char *));
    for (int i = 0; i < argc; i++) {
        char *left = NULL;
        if (starts(out[i], "--for=")) {
            left = left_of("--for=", out[i] + 6, started);
            out[i] = left != NULL ? left : out[i];
        } else if (strcmp(out[i], "--for") == 0 && i + 1 < argc) {
            left = left_of("", out[i + 1], started);
            out[i + 1] = left != NULL ? left : out[i + 1];
        }
    }
    return out;
}

static void fail(const char *format, ...) {
    va_list args;
    va_start(args, format);
    fputs("cci: ", stderr);
    vfprintf(stderr, format, args);
    fputc('\n', stderr);
    va_end(args);
    exit(1);
}

static void run_core(char **argv) {
    char resolved[PATH_MAX];
#ifdef __APPLE__
    char exe[PATH_MAX];
    uint32_t size = sizeof(exe);
    if (_NSGetExecutablePath(exe, &size) != 0) {
        fail("executable path too long");
    }
    if (realpath(exe, resolved) == NULL) {
        fail("%s: %s", exe, strerror(errno));
    }
#else
    if (realpath("/proc/self/exe", resolved) == NULL) {
        fail("/proc/self/exe: %s", strerror(errno));
    }
#endif
    char *slash = strrchr(resolved, '/');
    text core = {0};
    append(&core, resolved, (size_t)(slash - resolved));
    append_str(&core, "/cci-core");
    append(&core, "", 1);
    if (chmod(core.data, 0755) != 0) {
        fail("%s: %s", core.data, strerror(errno));
    }
    execve(core.data, argv, environ);
    fail("%s: %s", core.data, strerror(errno));
}

static bool write_all(int fd, const char *data, size_t n) {
    while (n > 0) {
        ssize_t wrote = write(fd, data, n);
        if (wrote < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        data += wrote;
        n -= (size_t)wrote;
    }
    return true;
}

static bool write_frame(int conn, char kind, const char *payload, size_t n) {
    static text frame;
    frame.len = 0;
    reserve(&frame, header_bytes + n);
    frame.data[0] = kind;
    frame.data[1] = (char)(n >> 24);
    frame.data[2] = (char)(n >> 16);
    frame.data[3] = (char)(n >> 8);
    frame.data[4] = (char)n;
    if (n > 0) {
        memcpy(frame.data + header_bytes, payload, n);
    }
    return write_all(conn, frame.data, header_bytes + n);
}

static bool attach(pump *p, int conn) {
    p->conn = conn;
    p->wired = true;
    size_t next = 0;
    for (; next < p->held_count; next++) {
        if (!write_frame(conn, frame_stdin, p->held[next], p->held_len[next])) {
            break;
        }
    }
    p->held_count -= next;
    memmove(p->held, p->held + next, p->held_count * sizeof(char *));
    memmove(p->held_len, p->held_len + next, p->held_count * sizeof(size_t));
    return p->held_count == 0;
}

static void hold(pump *p, const char *data, size_t n) {
    p->held = grown(p->held, (p->held_count + 1) * sizeof(char *));
    p->held_len = grown(p->held_len, (p->held_count + 1) * sizeof(size_t));
    p->held[p->held_count] = grown(NULL, n + 1);
    memcpy(p->held[p->held_count], data, n);
    p->held_len[p->held_count] = n;
    p->held_count++;
}

static void pump_read(pump *p) {
    static char data[read_bytes];
    ssize_t n = read(STDIN_FILENO, data, sizeof(data));
    if (n < 0 && (errno == EINTR || errno == EAGAIN)) {
        return;
    }
    size_t got = n > 0 ? (size_t)n : 0;
    p->reading = false;
    p->eof = got == 0;
    p->sent = p->sent || got > 0;
    if (!write_frame(p->conn, frame_stdin, data, got)) {
        hold(p, data, got);
    }
}

static bool refill(reader *r) {
    while (r->in->reading) {
        fd_set ready;
        FD_ZERO(&ready);
        FD_SET(STDIN_FILENO, &ready);
        FD_SET(r->fd, &ready);
        if (select(r->fd + 1, &ready, NULL, NULL, NULL) < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EBADF) {
                pump_read(r->in);
                continue;
            }
            r->err = errno;
            return false;
        }
        if (FD_ISSET(STDIN_FILENO, &ready)) {
            pump_read(r->in);
        }
        if (FD_ISSET(r->fd, &ready)) {
            break;
        }
    }
    for (;;) {
        ssize_t n = read(r->fd, r->data, sizeof(r->data));
        if (n > 0) {
            r->at = 0;
            r->end = (size_t)n;
            return true;
        }
        if (n == 0) {
            r->eof = true;
            return false;
        }
        if (errno != EINTR) {
            r->err = errno;
            return false;
        }
    }
}

static bool read_exact(reader *r, char *out, size_t n) {
    while (n > 0) {
        if (r->at == r->end && !refill(r)) {
            return false;
        }
        size_t take = r->end - r->at < n ? r->end - r->at : n;
        memcpy(out, r->data + r->at, take);
        r->at += take;
        out += take;
        n -= take;
    }
    return true;
}

static bool exchange(int conn, const text *body, char **argv, pump *p, outcome *result) {
    if (!write_frame(conn, frame_request, body->data, body->len) || !attach(p, conn)) {
        *result = (outcome){.code = 0, .ran = false, .lost = true};
        return true;
    }
    static reader r;
    r.fd = conn;
    r.at = r.end = 0;
    r.in = p;
    r.err = 0;
    r.eof = false;
    static text payload;
    bool spoke = false;
    for (;;) {
        unsigned char header[header_bytes];
        bool complete = read_exact(&r, (char *)header, header_bytes);
        uint32_t size = 0;
        const char *why = NULL;
        if (complete) {
            size = (uint32_t)header[1] << 24 | (uint32_t)header[2] << 16 | (uint32_t)header[3] << 8 | header[4];
            if (size > max_frame_bytes) {
                why = "frame too large";
                complete = false;
            } else {
                payload.len = 0;
                reserve(&payload, size + 1);
                complete = read_exact(&r, payload.data, size);
            }
        }
        if (!complete) {
            if (!spoke) {
                *result = (outcome){.code = 0, .ran = false, .lost = true};
                return true;
            }
            if (stream(argv)) {
                *result = (outcome){.code = 0, .ran = true, .lost = true};
                return true;
            }
            fprintf(stderr, "cci: the daemon went away: %s\n", why != NULL ? why : r.eof ? "EOF" : strerror(r.err));
            *result = (outcome){.code = lost_exit, .ran = true, .lost = true};
            return true;
        }
        switch (header[0]) {
        case frame_fallback:
            *result = (outcome){.code = 0, .ran = false, .lost = false};
            return true;
        case frame_exit: {
            const unsigned char *code = (const unsigned char *)payload.data;
            *result = (outcome){.code = (int)((uint32_t)code[0] << 24 | (uint32_t)code[1] << 16 | (uint32_t)code[2] << 8 | code[3]), .ran = true, .lost = false};
            return true;
        }
        case frame_stderr:
            spoke = true;
            (void)write_all(STDERR_FILENO, payload.data, size);
            break;
        case frame_stdout:
            spoke = true;
            if (!write_all(STDOUT_FILENO, payload.data, size)) {
                fprintf(stderr, "cci: %s\n", strerror(errno));
                *result = (outcome){.code = 1, .ran = true, .lost = false};
                return true;
            }
            if (!write_frame(conn, frame_ack, NULL, 0)) {
                *result = (outcome){.code = lost_exit, .ran = true, .lost = true};
                return true;
            }
            break;
        case frame_need_in:
            spoke = true;
            if (!p->reading && p->held_count == 0) {
                p->reading = true;
            }
            break;
        default:
            break;
        }
    }
}

static int dial(const char *path) {
    struct sockaddr_un address;
    memset(&address, 0, sizeof(address));
    if (strlen(path) >= sizeof(address.sun_path)) {
        return -1;
    }
    address.sun_family = AF_UNIX;
    strcpy(address.sun_path, path);
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
#ifdef SO_NOSIGPIPE
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
    if (fd >= FD_SETSIZE || connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int redial(const char *socket_path) {
    struct timespec pause = {.tv_sec = 0, .tv_nsec = redial_every_ns};
    int64_t deadline = monotonic_ns() + redial_for_ns;
    while (monotonic_ns() < deadline) {
        int conn = dial(socket_path);
        if (conn >= 0) {
            return conn;
        }
        nanosleep(&pause, NULL);
    }
    return -1;
}

static bool working_dir(text *out) {
    char cwd[PATH_MAX];
    const char *pwd = getenv("PWD");
    if (pwd != NULL && pwd[0] == '/') {
        struct stat named;
        struct stat here;
        if (stat(pwd, &named) == 0 && stat(".", &here) == 0 && named.st_dev == here.st_dev && named.st_ino == here.st_ino) {
            append_json(out, pwd);
            return true;
        }
    }
    if (getcwd(cwd, sizeof(cwd)) == NULL) {
        return false;
    }
    append_json(out, cwd);
    return true;
}

static char **environment(void) {
    int n = count(environ);
    char **out = grown(NULL, (size_t)(n + 1) * sizeof(char *));
    int kept = 0;
    for (int i = 0; i < n; i++) {
        const char *equals = strchr(environ[i], '=');
        size_t key = equals != NULL ? (size_t)(equals - environ[i]) + 1 : strlen(environ[i]);
        bool seen = false;
        for (int j = 0; j < kept && !seen; j++) {
            seen = strncmp(out[j], environ[i], key) == 0;
        }
        if (!seen) {
            out[kept++] = environ[i];
        }
    }
    out[kept] = NULL;
    return out;
}

static bool request_body(text *body, char **argv, char **env, const text *cwd, bool stdout_char) {
    char pid[32];
    snprintf(pid, sizeof(pid), "%d", (int)getpid());
    body->len = 0;
    append_str(body, "{\"version\":");
    append_json(body, CCI_VERSION);
    append_str(body, ",\"argv\":");
    append_list(body, argv);
    append_str(body, ",\"env\":");
    append_list(body, env);
    append_str(body, ",\"cwd\":");
    append(body, cwd->data, cwd->len);
    append_str(body, ",\"pid\":");
    append_str(body, pid);
    append_str(body, stdout_char ? ",\"stdout_char\":true}" : ",\"stdout_char\":false}");
    return true;
}

int main(int argc, char **argv) {
    const char *home = getenv("HOME");
    const char *cci_home = getenv("CCI_HOME");
    const char *unsent = getenv("CCI_UNSENT");
    if (!relayed(argc, argv) || cci_home != NULL && *cci_home != 0 || unsent != NULL && *unsent != 0 || home == NULL || *home == 0) {
        run_core(argv);
    }
    text socket_path = {0};
    append_str(&socket_path, home);
    append_str(&socket_path, "/.cc-inbox/cci.sock");
    append(&socket_path, "", 1);
    int conn = dial(socket_path.data);
    if (conn < 0) {
        run_core(argv);
    }
    text cwd = {0};
    struct stat out;
    if (!working_dir(&cwd) || fstat(STDOUT_FILENO, &out) != 0) {
        close(conn);
        run_core(argv);
    }
    char **env = environment();
    char **first = resumable(argv);
    char **current = first;
    bool stdout_char = S_ISCHR(out.st_mode);
    int64_t started = monotonic_ns();
    pump p = {0};
    text body = {0};
    for (;;) {
        outcome result;
        request_body(&body, current, env, &cwd, stdout_char);
        exchange(conn, &body, current, &p, &result);
        close(conn);
        if (channel(current) && result.ran && !result.lost && !p.eof) {
            run_core(p.sent ? resumed(first) : current);
        }
        if (!result.lost || !stream(current)) {
            if (!result.ran) {
                run_core(current);
            }
            return result.code;
        }
        current = for_what_is_left(resumed(first), started);
        conn = redial(socket_path.data);
        if (conn < 0) {
            run_core(current);
        }
    }
}
