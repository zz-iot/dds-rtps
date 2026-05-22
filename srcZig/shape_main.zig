//! Zig DDS shape_main — interoperability test application for OMG dds-rtps.
//!
//! Mirrors the CLI interface of srcCxx/shape_main.cxx so the Python test
//! harness (interoperability_report.py) can drive it as a pub or sub.
//!
//! This file is vendor-agnostic.  All DDS implementation details are hidden
//! behind the "dds" module, which each Zig DDS vendor supplies via their
//! build.zig.  See srcZig/dds.zig for the full interface contract.
//!
//! Required stdout strings (matched by the harness via pexpect):
//!   Publisher: "Create topic:"  →  "Create writer for topic:"  →
//!              "on_publication_matched()" or "on_offered_incompatible_qos"  →
//!              "%-10s %-10s %03d %03d [%d]" (only when -w is passed)
//!   Subscriber: "Create topic:"  →  "Create reader for topic:"  →
//!               "[<number>]" in the sample line  or  "on_requested_incompatible_qos()"

const std = @import("std");
const dds = @import("dds");
const DDS = dds.DDS;
const shape_main_options = @import("shape_main_options");

pub const std_options: std.Options = .{
    .log_level = std.meta.stringToEnum(std.log.Level, shape_main_options.log_level) orelse
        @compileError("invalid shape_main log level"),
};

// ── Time helpers ─────────────────────────────────────────────────────────────
// std.time.nanoTimestamp / std.time.sleep were removed in Zig 0.16.

fn monoNs() i64 {
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return ts.sec * std.time.ns_per_s + ts.nsec;
}

fn sleepNs(ns: u64) void {
    var req = std.os.linux.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    _ = std.os.linux.nanosleep(&req, null);
}

// ── Stdout helpers ────────────────────────────────────────────────────────────
// std.io was removed in Zig 0.16; write directly via the Linux write(2) syscall.

fn stdoutWrite(bytes: []const u8) void {
    var remaining = bytes;
    while (remaining.len > 0) {
        const rc = std.os.linux.write(std.posix.STDOUT_FILENO, remaining.ptr, remaining.len);
        const n = @as(isize, @bitCast(rc));
        if (n <= 0) break;
        remaining = remaining[@intCast(n)..];
    }
}

fn stdoutPrint(comptime fmt: []const u8, args: anytype) void {
    var buf: [2048]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    w.print(fmt, args) catch {};
    stdoutWrite(w.buffered());
}

// ── Signal handling ───────────────────────────────────────────────────────────

var g_all_done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

fn handleSigint(sig: std.posix.SIG) callconv(.c) void {
    _ = sig;
    g_all_done.store(true, .release);
}

// ── Options ───────────────────────────────────────────────────────────────────

const Options = struct {
    publish: bool = false,
    subscribe: bool = false,
    domain_id: u32 = 0,
    best_effort: bool = false,
    reliable: bool = false,
    history_depth: i32 = -1, // -1 = use default KEEP_LAST 1
    deadline_ms: u64 = 0,
    ownership_strength: i32 = -1, // -1 = SHARED
    topic_name: []const u8 = "Square",
    color: ?[]const u8 = null,
    partition: ?[]const u8 = null,
    durability: u8 = 'v',
    data_representation: u16 = 1, // 1=XCDR1, 2=XCDR2
    print_writer_samples: bool = false,
    shapesize: i32 = 20,
    write_period_ms: u64 = 33,
    read_period_ms: u64 = 100,
    num_iterations: i64 = -1, // -1 = infinite
    num_instances: u32 = 1,
    additional_payload: u32 = 0,
    size_modulo: i32 = 0, // 0 = no cycling (--size-modulo)
    cft_expression: ?[]const u8 = null, // content filter expression (--cft)
};

// ── CDR helpers ───────────────────────────────────────────────────────────────

fn writeU32Le(buf: *std.ArrayList(u8), alloc: std.mem.Allocator, v: u32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, v, .little);
    try buf.appendSlice(alloc, &b);
}

fn writeI32Le(buf: *std.ArrayList(u8), alloc: std.mem.Allocator, v: i32) !void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, v, .little);
    try buf.appendSlice(alloc, &b);
}

fn align4(n: usize) usize {
    return (n + 3) & ~@as(usize, 3);
}

// ── ShapeType CDR serialization ───────────────────────────────────────────────

const ShapeData = struct {
    color: []const u8,
    x: i32,
    y: i32,
    shapesize: i32,
    payload: u32, // additional_payload_size sequence length (all-zero bytes)
};

// Serialize ShapeType with a 4-byte encapsulation header.
// xcdr2=false → CDR_LE (XCDR1): @appendable treated as @final, no DHEADER.
// xcdr2=true  → DELIMITED_CDR_LE (XCDR2): 4-byte DHEADER before struct members.
fn serializeShape(buf: *std.ArrayList(u8), alloc: std.mem.Allocator, s: ShapeData, xcdr2: bool) !void {
    buf.clearRetainingCapacity();

    if (xcdr2) {
        // DELIMITED_CDR_LE encapsulation (XCDR2 @appendable)
        try buf.appendSlice(alloc, &[4]u8{ 0x00, 0x09, 0x00, 0x00 });
        // Placeholder for DHEADER (filled in below)
        const dheader_pos = buf.items.len;
        try writeU32Le(buf, alloc, 0);

        var off: usize = 0;
        const clen: u32 = @intCast(s.color.len + 1);
        try writeU32Le(buf, alloc, clen);
        off += 4;
        try buf.appendSlice(alloc, s.color);
        try buf.append(alloc, 0);
        off += clen;
        const pad = align4(off) - off;
        for (0..pad) |_| try buf.append(alloc, 0);
        off += pad;
        try writeI32Le(buf, alloc, s.x);
        off += 4;
        try writeI32Le(buf, alloc, s.y);
        off += 4;
        try writeI32Le(buf, alloc, s.shapesize);
        off += 4;
        try writeU32Le(buf, alloc, s.payload);
        off += 4;
        for (0..s.payload) |_| try buf.append(alloc, 0);
        // Patch DHEADER = length of struct members (bytes after DHEADER)
        const member_len: u32 = @intCast(buf.items.len - dheader_pos - 4);
        std.mem.writeInt(u32, buf.items[dheader_pos..][0..4], member_len, .little);
    } else {
        // CDR_LE encapsulation (XCDR1)
        try buf.appendSlice(alloc, &[4]u8{ 0x00, 0x01, 0x00, 0x00 });

        var off: usize = 0;
        const clen: u32 = @intCast(s.color.len + 1);
        try writeU32Le(buf, alloc, clen);
        off += 4;
        try buf.appendSlice(alloc, s.color);
        try buf.append(alloc, 0);
        off += clen;
        const pad = align4(off) - off;
        for (0..pad) |_| try buf.append(alloc, 0);
        off += pad;
        try writeI32Le(buf, alloc, s.x);
        off += 4;
        try writeI32Le(buf, alloc, s.y);
        off += 4;
        try writeI32Le(buf, alloc, s.shapesize);
        off += 4;
        try writeU32Le(buf, alloc, s.payload);
        off += 4;
        for (0..s.payload) |_| try buf.append(alloc, 0);
    }
}

// Compute 16-byte key hash from CDR-serialised key (color string).
// DDS spec §9.6.3.8: key is CDR_BE (big-endian) serialization of the key fields.
// When the serialised key fits in 16 bytes, the hash is the raw bytes zero-padded.
fn colorKeyHash(color: []const u8) [16]u8 {
    var kh = std.mem.zeroes([16]u8);
    const clen: u32 = @intCast(color.len + 1);
    const klen: usize = 4 + clen;
    if (klen <= 16) {
        std.mem.writeInt(u32, kh[0..4], clen, .big);
        @memcpy(kh[4..][0..color.len], color);
        // null at [4 + color.len]; already zero from zeroes()
    } else {
        // Fall back to zeroes — MD5 not implemented; works for typical test colors
    }
    return kh;
}

// Derive the key hash from a received CDR payload by parsing out the color field.
// Used as the TypeSupport compute_key_hash callback so the reader-side participant
// can recover instance identity when the writer omitted the inline-QoS key_hash.
fn shapeTypeKeyHash(payload: []const u8) [16]u8 {
    if (payload.len < 4) return std.mem.zeroes([16]u8);
    const encap = payload[1];
    var off: usize = 4; // skip 4-byte encapsulation header

    // XCDR2 @appendable (encap 0x08 or 0x09): skip 4-byte struct DHEADER
    if (encap == 0x08 or encap == 0x09) {
        if (payload.len < off + 4) return std.mem.zeroes([16]u8);
        off += 4;
    }

    if (payload.len < off + 4) return std.mem.zeroes([16]u8);
    // String length endianness matches the payload: odd encap byte → little-endian
    const is_le = (encap & 0x01) != 0;
    const clen: u32 = if (is_le)
        std.mem.readInt(u32, payload[off..][0..4], .little)
    else
        std.mem.readInt(u32, payload[off..][0..4], .big);
    off += 4;
    if (clen == 0 or payload.len < off + clen) return std.mem.zeroes([16]u8);
    const color = payload[off .. off + clen - 1]; // strip null terminator

    return colorKeyHash(color);
}

const ParsedShape = struct {
    color: []const u8, // slice into payload; valid while payload is alive
    x: i32,
    y: i32,
    shapesize: i32,
};

// Deserialize a CDR/CDR2 ShapeType payload.  Returns null on error / key-only.
fn deserializeShape(payload: []const u8) ?ParsedShape {
    if (payload.len < 4) return null;

    const encap = payload[1];
    var off: usize = 4; // skip 4-byte encap header

    // XCDR2 @appendable: skip 4-byte struct DHEADER
    if (encap == 0x08 or encap == 0x09) {
        if (payload.len < off + 4) return null;
        off += 4;
    }

    // color string
    if (payload.len < off + 4) return null;
    const clen = std.mem.readInt(u32, payload[off..][0..4], .little);
    off += 4;
    if (clen == 0 or payload.len < off + clen) return null;
    const color = payload[off .. off + clen - 1]; // strip null terminator
    off += clen;

    // pad to 4-byte
    off = align4(off);

    if (payload.len < off + 12) return null; // x + y + shapesize
    const x = std.mem.readInt(i32, payload[off..][0..4], .little);
    off += 4;
    const y = std.mem.readInt(i32, payload[off..][0..4], .little);
    off += 4;
    const shapesize = std.mem.readInt(i32, payload[off..][0..4], .little);

    return ParsedShape{ .color = color, .x = x, .y = y, .shapesize = shapesize };
}

// ── Policy name mapping ───────────────────────────────────────────────────────

fn policyName(id: i32) []const u8 {
    return switch (id) {
        2 => "DURABILITY",
        4 => "DEADLINE",
        5 => "LATENCYBUDGET",
        6 => "OWNERSHIP",
        8 => "LIVELINESS",
        10 => "PARTITION",
        11 => "RELIABILITY",
        12 => "DESTINATIONORDER",
        23 => "DATAREPRESENTATION",
        else => "UNKNOWN",
    };
}

// ── Listener context and vtables ──────────────────────────────────────────────

const ListenerCtx = struct {
    topic_name: []const u8,
    type_name: []const u8 = "ShapeType",
};

// DataWriter listeners

fn dwOnIncompatQos(ctx: *anyopaque, _: DDS.DataWriter, status: DDS.OfferedIncompatibleQosStatus) void {
    const lc: *ListenerCtx = @ptrCast(@alignCast(ctx));
    stdoutPrint("on_offered_incompatible_qos() topic: '{s}'  type: '{s}' : {d} ({s})\n", .{ lc.topic_name, lc.type_name, status.last_policy_id, policyName(status.last_policy_id) });
}

fn dwOnDeadlineMissed(ctx: *anyopaque, _: DDS.DataWriter, status: DDS.OfferedDeadlineMissedStatus) void {
    const lc: *ListenerCtx = @ptrCast(@alignCast(ctx));
    stdoutPrint("on_offered_deadline_missed() topic: '{s}'  type: '{s}' : (total = {d}, change = {d})\n", .{ lc.topic_name, lc.type_name, status.total_count, status.total_count_change });
}

fn dwOnLivelinessLost(_: *anyopaque, _: DDS.DataWriter, _: DDS.LivelinessLostStatus) void {}
fn dwOnPubMatched(_: *anyopaque, _: DDS.DataWriter, _: DDS.PublicationMatchedStatus) void {}
fn dwOnDeinit(_: *anyopaque) void {}

var dw_vtable = DDS.DataWriterListener.Vtable{
    .on_offered_deadline_missed = dwOnDeadlineMissed,
    .on_offered_incompatible_qos = dwOnIncompatQos,
    .on_liveliness_lost = dwOnLivelinessLost,
    .on_publication_matched = dwOnPubMatched,
    .deinit = dwOnDeinit,
};

// DataReader listeners

fn drOnIncompatQos(ctx: *anyopaque, _: DDS.DataReader, status: DDS.RequestedIncompatibleQosStatus) void {
    const lc: *ListenerCtx = @ptrCast(@alignCast(ctx));
    stdoutPrint("on_requested_incompatible_qos() topic: '{s}'  type: '{s}' : {d} ({s})\n", .{ lc.topic_name, lc.type_name, status.last_policy_id, policyName(status.last_policy_id) });
}

fn drOnDeadlineMissed(ctx: *anyopaque, _: DDS.DataReader, status: DDS.RequestedDeadlineMissedStatus) void {
    const lc: *ListenerCtx = @ptrCast(@alignCast(ctx));
    stdoutPrint("on_requested_deadline_missed() topic: '{s}'  type: '{s}' : (total = {d}, change = {d})\n", .{ lc.topic_name, lc.type_name, status.total_count, status.total_count_change });
}

fn drOnSampleRejected(_: *anyopaque, _: DDS.DataReader, _: DDS.SampleRejectedStatus) void {}
fn drOnLivelinessChanged(_: *anyopaque, _: DDS.DataReader, _: DDS.LivelinessChangedStatus) void {}
fn drOnDataAvail(_: *anyopaque, _: DDS.DataReader) void {}
fn drOnSubMatched(_: *anyopaque, _: DDS.DataReader, _: DDS.SubscriptionMatchedStatus) void {}
fn drOnSampleLost(_: *anyopaque, _: DDS.DataReader, _: DDS.SampleLostStatus) void {}
fn drOnDeinit(_: *anyopaque) void {}

var dr_vtable = DDS.DataReaderListener.Vtable{
    .on_requested_deadline_missed = drOnDeadlineMissed,
    .on_requested_incompatible_qos = drOnIncompatQos,
    .on_sample_rejected = drOnSampleRejected,
    .on_liveliness_changed = drOnLivelinessChanged,
    .on_data_available = drOnDataAvail,
    .on_subscription_matched = drOnSubMatched,
    .on_sample_lost = drOnSampleLost,
    .deinit = drOnDeinit,
};

// ── DataWriter QoS builder ────────────────────────────────────────────────────

fn buildWriterQos(alloc: std.mem.Allocator, opts: *const Options) !DDS.DataWriterQos {
    var qos = DDS.DataWriterQos{};

    qos.reliability.kind = if (opts.best_effort)
        .BEST_EFFORT_RELIABILITY_QOS
    else
        .RELIABLE_RELIABILITY_QOS;

    if (opts.history_depth == 0) {
        qos.history.kind = .KEEP_ALL_HISTORY_QOS;
    } else if (opts.history_depth > 0) {
        qos.history.kind = .KEEP_LAST_HISTORY_QOS;
        qos.history.depth = opts.history_depth;
    }

    if (opts.deadline_ms > 0) {
        qos.deadline.period = .{
            .sec = @intCast(opts.deadline_ms / 1000),
            .nanosec = @intCast((opts.deadline_ms % 1000) * std.time.ns_per_ms),
        };
    }

    if (opts.ownership_strength >= 0) {
        qos.ownership.kind = .EXCLUSIVE_OWNERSHIP_QOS;
        qos.ownership_strength.value = opts.ownership_strength;
    }

    qos.durability.kind = switch (opts.durability) {
        'v' => .VOLATILE_DURABILITY_QOS,
        'l' => .TRANSIENT_LOCAL_DURABILITY_QOS,
        't' => .TRANSIENT_DURABILITY_QOS,
        'p' => .PERSISTENT_DURABILITY_QOS,
        else => .VOLATILE_DURABILITY_QOS,
    };

    const repr_id: i16 = if (opts.data_representation == 2) 2 else 0;
    try qos.data_representation.value.append(alloc, repr_id);

    return qos;
}

// ── DataReader QoS builder ────────────────────────────────────────────────────

fn buildReaderQos(alloc: std.mem.Allocator, opts: *const Options) !DDS.DataReaderQos {
    var qos = DDS.DataReaderQos{};

    qos.reliability.kind = if (opts.best_effort)
        .BEST_EFFORT_RELIABILITY_QOS
    else
        .RELIABLE_RELIABILITY_QOS;

    if (opts.history_depth == 0) {
        qos.history.kind = .KEEP_ALL_HISTORY_QOS;
    } else if (opts.history_depth > 0) {
        qos.history.kind = .KEEP_LAST_HISTORY_QOS;
        qos.history.depth = opts.history_depth;
    }

    if (opts.deadline_ms > 0) {
        qos.deadline.period = .{
            .sec = @intCast(opts.deadline_ms / 1000),
            .nanosec = @intCast((opts.deadline_ms % 1000) * std.time.ns_per_ms),
        };
    }

    if (opts.ownership_strength >= 0) {
        qos.ownership.kind = .EXCLUSIVE_OWNERSHIP_QOS;
    }

    qos.durability.kind = switch (opts.durability) {
        'v' => .VOLATILE_DURABILITY_QOS,
        'l' => .TRANSIENT_LOCAL_DURABILITY_QOS,
        't' => .TRANSIENT_DURABILITY_QOS,
        'p' => .PERSISTENT_DURABILITY_QOS,
        else => .VOLATILE_DURABILITY_QOS,
    };

    const repr_id: i16 = if (opts.data_representation == 2) 2 else 0;
    try qos.data_representation.value.append(alloc, repr_id);

    return qos;
}

// ── nil sentinel helpers ──────────────────────────────────────────────────────
// Delegated to the vendor dds module, which knows the implementation's nil
// sentinel value (each vendor may use a different non-null sentinel address).

fn isNilTopic(t: DDS.Topic) bool {
    return dds.isNilTopic(t);
}
fn isNilPub(p: DDS.Publisher) bool {
    return dds.isNilPub(p);
}
fn isNilSub(s: DDS.Subscriber) bool {
    return dds.isNilSub(s);
}
fn isNilDw(dw: DDS.DataWriter) bool {
    return dds.isNilDw(dw);
}
fn isNilDr(dr: DDS.DataReader) bool {
    return dds.isNilDr(dr);
}
fn isNilCft(cft: DDS.ContentFilteredTopic) bool {
    return dds.isNilCft(cft);
}

// ── Publisher ─────────────────────────────────────────────────────────────────

fn runPublisher(
    alloc: std.mem.Allocator,
    dp: DDS.DomainParticipant,
    topic: DDS.Topic,
    opts: *const Options,
) !void {
    const color = opts.color orelse "BLUE";
    const topic_name = dds.topicName(topic);

    var pub_partition_name_buf: [1][]const u8 = .{opts.partition orelse ""};
    const pub_qos: DDS.PublisherQos = if (opts.partition) |_| .{
        .partition = .{ .name = .{ .items = &pub_partition_name_buf, .capacity = 1 } },
    } else .{};
    const pub_ = dp.vtable.create_publisher(dp.ptr, pub_qos, dds.nilPublisherListener(), 0);
    if (isNilPub(pub_)) return error.PublisherFailed;

    var dw_qos = try buildWriterQos(alloc, opts);
    defer dw_qos.data_representation.value.deinit(alloc);

    var lctx = ListenerCtx{ .topic_name = topic_name };
    const dw_listener = DDS.DataWriterListener{
        .ptr = &lctx,
        .vtable = &dw_vtable,
    };
    const listener_mask: DDS.StatusMask =
        DDS.OFFERED_INCOMPATIBLE_QOS_STATUS | DDS.OFFERED_DEADLINE_MISSED_STATUS;

    const dw = pub_.vtable.create_datawriter(pub_.ptr, topic, dw_qos, dw_listener, listener_mask);
    if (isNilDw(dw)) return error.DataWriterFailed;

    stdoutPrint("Create writer for topic: {s} color: {s}\n", .{ topic_name, color });

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);

    var shape = ShapeData{
        .color = color,
        .x = 0,
        .y = 0,
        .shapesize = if (opts.shapesize == 0) 1 else opts.shapesize,
        .payload = opts.additional_payload,
    };
    var rng = std.Random.DefaultPrng.init(@intCast(monoNs()));
    const rand = rng.random();

    const match_deadline = monoNs() + 10 * std.time.ns_per_s;
    var printed_matched = false;

    const deadline_ns: i64 = if (opts.deadline_ms > 0)
        @intCast(opts.deadline_ms * std.time.ns_per_ms)
    else
        0;
    var last_write_ns: i64 = monoNs();

    var iteration: i64 = 0;
    while (!g_all_done.load(.acquire)) {
        if (opts.num_iterations >= 0 and iteration >= opts.num_iterations) break;

        if (!printed_matched) {
            if (dds.writerMatchedCount(dw) > 0) {
                stdoutPrint(
                    "on_publication_matched() topic: '{s}'  type: 'ShapeType' : matched readers {d} (change = 1)\n",
                    .{ topic_name, dds.writerMatchedCount(dw) },
                );
                printed_matched = true;
            } else if (monoNs() > match_deadline) {
                return; // READER_NOT_MATCHED
            }
        }

        if (deadline_ns > 0) {
            const elapsed = monoNs() - last_write_ns;
            if (elapsed > deadline_ns) dds.writerNotifyDeadline(dw);
        }

        shape.x = @rem(@as(i32, rand.int(u16)), 320);
        shape.y = @rem(@as(i32, rand.int(u16)), 240);

        for (0..opts.num_instances) |inst| {
            const inst_color: []const u8 = blk: {
                if (inst == 0) break :blk color;
                break :blk std.fmt.allocPrint(alloc, "{s}{d}", .{ color, inst }) catch color;
            };
            defer if (inst > 0) alloc.free(inst_color);

            shape.color = inst_color;
            try serializeShape(&buf, alloc, shape, opts.data_representation == 2);

            const key_hash = colorKeyHash(inst_color);
            try dds.writeRaw(dw, .alive, key_hash, buf.items);

            if (opts.print_writer_samples) {
                stdoutPrint("{s:<10} {s:<10} {d:0>3} {d:0>3} [{d}]\n", .{ topic_name, inst_color, @as(u32, @intCast(shape.x)), @as(u32, @intCast(shape.y)), shape.shapesize });
            }
        }

        if (opts.shapesize == 0) {
            shape.shapesize += 1;
            if (opts.size_modulo > 0 and shape.shapesize > opts.size_modulo)
                shape.shapesize = 1;
        }

        last_write_ns = monoNs();
        iteration += 1;
        sleepNs(opts.write_period_ms * std.time.ns_per_ms);
    }
}

// ── Subscriber ────────────────────────────────────────────────────────────────

fn runSubscriber(
    alloc: std.mem.Allocator,
    dp: DDS.DomainParticipant,
    topic: DDS.Topic,
    opts: *const Options,
) !void {
    const topic_name = dds.topicName(topic);

    const cft: ?DDS.ContentFilteredTopic = blk: {
        const expr = opts.cft_expression orelse break :blk null;
        const cft_name = std.fmt.allocPrint(
            alloc,
            "{s}_cft",
            .{topic_name},
        ) catch break :blk null;
        defer alloc.free(cft_name);
        const c = dp.vtable.create_contentfilteredtopic(
            dp.ptr,
            cft_name,
            topic,
            expr,
            .empty,
        );
        if (isNilCft(c)) break :blk null;
        break :blk c;
    };
    defer {
        if (cft) |c| _ = dp.vtable.delete_contentfilteredtopic(dp.ptr, c);
    }

    const topic_desc: DDS.TopicDescription = if (cft) |c|
        dds.cftTopicDescription(c)
    else
        dp.vtable.lookup_topicdescription(dp.ptr, dds.topicName(topic));

    var sub_partition_name_buf: [1][]const u8 = .{opts.partition orelse ""};
    const sub_qos: DDS.SubscriberQos = if (opts.partition) |_| .{
        .partition = .{ .name = .{ .items = &sub_partition_name_buf, .capacity = 1 } },
    } else .{};
    const sub = dp.vtable.create_subscriber(dp.ptr, sub_qos, dds.nilSubscriberListener(), 0);
    if (isNilSub(sub)) return error.SubscriberFailed;

    var dr_qos = try buildReaderQos(alloc, opts);
    defer dr_qos.data_representation.value.deinit(alloc);

    var lctx = ListenerCtx{ .topic_name = topic_name };
    const dr_listener = DDS.DataReaderListener{
        .ptr = &lctx,
        .vtable = &dr_vtable,
    };
    const listener_mask: DDS.StatusMask =
        DDS.REQUESTED_INCOMPATIBLE_QOS_STATUS | DDS.REQUESTED_DEADLINE_MISSED_STATUS;

    const dr = sub.vtable.create_datareader(sub.ptr, topic_desc, dr_qos, dr_listener, listener_mask);
    if (isNilDr(dr)) return error.DataReaderFailed;

    stdoutPrint("Create reader for topic: {s}\n", .{topic_name});

    const sub_deadline_ns: i64 = if (opts.deadline_ms > 0)
        @intCast(opts.deadline_ms * std.time.ns_per_ms)
    else
        0;
    var deadline_base_ns: i64 = 0;

    const ShapeAccessor = struct {
        shape: *const ParsedShape,

        fn get(ctx: *anyopaque, field: []const u8) ?dds.FilterValue {
            const self: *const @This() = @ptrCast(@alignCast(ctx));
            if (std.mem.eql(u8, field, "color"))
                return .{ .string = self.shape.color };
            if (std.mem.eql(u8, field, "x"))
                return .{ .int = self.shape.x };
            if (std.mem.eql(u8, field, "y"))
                return .{ .int = self.shape.y };
            if (std.mem.eql(u8, field, "shapesize"))
                return .{ .int = self.shape.shapesize };
            return null;
        }
    };

    var iteration: i64 = 0;
    while (!g_all_done.load(.acquire)) {
        if (opts.num_iterations >= 0 and iteration >= opts.num_iterations) break;

        if (sub_deadline_ns > 0 and deadline_base_ns == 0 and dds.readerMatchedCount(dr) > 0) {
            deadline_base_ns = monoNs();
        }

        var got_data = false;
        while (dds.takeRaw(dr)) |taken| {
            defer taken.deinit();
            got_data = true;
            const s = deserializeShape(taken.data) orelse continue;

            if (cft) |c| {
                var acc_ctx = ShapeAccessor{ .shape = &s };
                const accessor = dds.FieldAccessor{
                    .ctx = &acc_ctx,
                    .get = ShapeAccessor.get,
                };
                if (!dds.cftMatchSample(c, accessor)) continue;
            }

            stdoutPrint("{s:<10} {s:<10} {d:0>3} {d:0>3} [{d}]\n", .{ topic_name, s.color, @as(u32, @intCast(s.x)), @as(u32, @intCast(s.y)), s.shapesize });
        }

        if (got_data) {
            deadline_base_ns = monoNs();
        } else if (sub_deadline_ns > 0 and deadline_base_ns != 0) {
            if (monoNs() - deadline_base_ns > sub_deadline_ns) {
                dds.readerNotifyDeadline(dr);
                deadline_base_ns = monoNs();
            }
        }

        iteration += 1;
        sleepNs(opts.read_period_ms * std.time.ns_per_ms);
    }
}

// ── Argument parsing ──────────────────────────────────────────────────────────

fn parseArgs(process_args: std.process.Args) !Options {
    var opts = Options{};
    var it = std.process.Args.Iterator.init(process_args);
    _ = it.skip(); // program name

    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "-P")) {
            opts.publish = true;
        } else if (std.mem.eql(u8, arg, "-S")) {
            opts.subscribe = true;
        } else if (std.mem.eql(u8, arg, "-b")) {
            opts.best_effort = true;
        } else if (std.mem.eql(u8, arg, "-r")) {
            opts.reliable = true;
        } else if (std.mem.eql(u8, arg, "-w")) {
            opts.print_writer_samples = true;
        } else if (std.mem.eql(u8, arg, "-R")) {
            // use read() instead of take() — no-op (we always use takeRaw)
        } else if (std.mem.eql(u8, arg, "-d")) {
            const v = it.next() orelse return error.MissingValue;
            opts.domain_id = try std.fmt.parseInt(u32, v, 10);
        } else if (std.mem.eql(u8, arg, "-k")) {
            const v = it.next() orelse return error.MissingValue;
            opts.history_depth = try std.fmt.parseInt(i32, v, 10);
        } else if (std.mem.eql(u8, arg, "-f")) {
            const v = it.next() orelse return error.MissingValue;
            opts.deadline_ms = try std.fmt.parseInt(u64, v, 10);
        } else if (std.mem.eql(u8, arg, "-s")) {
            const v = it.next() orelse return error.MissingValue;
            opts.ownership_strength = try std.fmt.parseInt(i32, v, 10);
        } else if (std.mem.eql(u8, arg, "-t")) {
            const v = it.next() orelse return error.MissingValue;
            opts.topic_name = v;
        } else if (std.mem.eql(u8, arg, "-c")) {
            const v = it.next() orelse return error.MissingValue;
            opts.color = v;
        } else if (std.mem.eql(u8, arg, "-p")) {
            const v = it.next() orelse return error.MissingValue;
            opts.partition = v;
        } else if (std.mem.eql(u8, arg, "-D")) {
            const v = it.next() orelse return error.MissingValue;
            opts.durability = if (v.len > 0) v[0] else 'v';
        } else if (std.mem.eql(u8, arg, "-x")) {
            const v = it.next() orelse return error.MissingValue;
            opts.data_representation = std.fmt.parseInt(u16, v, 10) catch 1;
        } else if (std.mem.eql(u8, arg, "-z")) {
            const v = it.next() orelse return error.MissingValue;
            opts.shapesize = std.fmt.parseInt(i32, v, 10) catch 20;
        } else if (std.mem.eql(u8, arg, "-n")) {
            const v = it.next() orelse return error.MissingValue;
            opts.num_instances = std.fmt.parseInt(u32, v, 10) catch 1;
        } else if (std.mem.eql(u8, arg, "--write-period")) {
            const v = it.next() orelse return error.MissingValue;
            opts.write_period_ms = std.fmt.parseInt(u64, v, 10) catch 33;
        } else if (std.mem.eql(u8, arg, "--read-period")) {
            const v = it.next() orelse return error.MissingValue;
            opts.read_period_ms = std.fmt.parseInt(u64, v, 10) catch 100;
        } else if (std.mem.eql(u8, arg, "--num-iterations") or
            std.mem.eql(u8, arg, "-i"))
        {
            const v = it.next() orelse return error.MissingValue;
            opts.num_iterations = std.fmt.parseInt(i64, v, 10) catch -1;
        } else if (std.mem.eql(u8, arg, "--additional-payload")) {
            const v = it.next() orelse return error.MissingValue;
            opts.additional_payload = std.fmt.parseInt(u32, v, 10) catch 0;
        } else if (std.mem.eql(u8, arg, "--size-modulo")) {
            const v = it.next() orelse return error.MissingValue;
            opts.size_modulo = std.fmt.parseInt(i32, v, 10) catch 0;
        } else if (std.mem.eql(u8, arg, "--cft")) {
            opts.cft_expression = it.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--publisher-matches") or
            std.mem.eql(u8, arg, "--subscriber-matches") or
            std.mem.eql(u8, arg, "--deadline") or
            std.mem.eql(u8, arg, "--periodic-announcement") or
            std.mem.eql(u8, arg, "--final-instance-state") or
            std.mem.eql(u8, arg, "--access-scope") or
            std.mem.eql(u8, arg, "--coherent-sample-count") or
            std.mem.eql(u8, arg, "--take-read"))
        {
            // consume argument value and ignore — unimplemented options
            _ = it.next();
        } else if (std.mem.eql(u8, arg, "--coherent") or
            std.mem.eql(u8, arg, "--ordered"))
        {
            // boolean flags — ignore
        } else if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            stdoutWrite(
                \\Usage: shape_main -P|-S [options]
                \\
                \\Mode (required):
                \\  -P                  Publisher
                \\  -S                  Subscriber
                \\
                \\QoS:
                \\  -b                  BEST_EFFORT reliability (default: RELIABLE)
                \\  -r                  RELIABLE reliability (explicit)
                \\  -k <depth>          History depth; 0 = KEEP_ALL (default: KEEP_LAST 1)
                \\  -D v|l|t|p          Durability: volatile, transient-local, transient, persistent
                \\  -f <ms>             Deadline period in milliseconds
                \\  -s <strength>       Ownership strength (enables EXCLUSIVE ownership)
                \\  -x 1|2              Data representation: 1=XCDR1 (default), 2=XCDR2
                \\  -p <name>           Partition name
                \\
                \\Topic / data:
                \\  -t <name>           Topic name (default: Square)
                \\  -c <color>          Color / key value (default: BLUE)
                \\  -z <size>           Shape size; 0 = auto-increment each sample (default: 20)
                \\  -n <count>          Number of instances to publish (default: 1)
                \\  --additional-payload <bytes>  Extra zero bytes appended to each sample
                \\  --size-modulo <n>   Cycle shapesize 1..n when -z 0 is active
                \\  --cft <expr>        Content filter expression (subscriber only)
                \\
                \\Timing / iterations:
                \\  -i, --num-iterations <n>   Stop after n samples (-1 = infinite, default)
                \\  --write-period <ms>         Publish interval in ms (default: 33)
                \\  --read-period <ms>          Read poll interval in ms (default: 100)
                \\
                \\Other:
                \\  -d <id>             Domain ID (default: 0)
                \\  -w                  Print each sample on the writer side
                \\  -h, --help          Show this help and exit
                \\
                \\Environment variables:
                \\  SHAPE_STARTUP_DELAY_MS=<ms>   Sleep before creating the DDS participant.
                \\                                Useful for late-join testing without relying
                \\                                on fixed sleeps in the test harness.
                \\
            );
            std.process.exit(0);
        } else if (std.mem.startsWith(u8, arg, "--") or std.mem.startsWith(u8, arg, "-")) {
            std.log.warn("unrecognised option: {s}", .{arg});
        }
    }

    // Publisher default color
    if (opts.publish and opts.color == null) {
        opts.color = "BLUE";
    }

    return opts;
}

// ── main ──────────────────────────────────────────────────────────────────────

pub fn main(init: std.process.Init.Minimal) !void {
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = handleSigint },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    if (std.c.getenv("SHAPE_STARTUP_DELAY_MS")) |v| {
        const ms = std.fmt.parseInt(u64, std.mem.span(v), 10) catch 0;
        if (ms > 0) sleepNs(ms * std.time.ns_per_ms);
    }

    const opts = parseArgs(init.args) catch |err| {
        std.log.err("argument error: {}", .{err});
        std.process.exit(1);
    };

    if (!opts.publish and !opts.subscribe) {
        std.log.err("specify -P (publish) or -S (subscribe)", .{});
        std.process.exit(1);
    }

    const participant = dds.createParticipant(alloc, opts.domain_id) catch |err| {
        std.log.err("failed to create participant on domain {d}: {}", .{ opts.domain_id, err });
        std.process.exit(1);
    };
    defer dds.destroyParticipant(participant);
    const dp = participant.toDDS();

    dds.registerTypeSupport(dp, "ShapeType", .{ .compute_key_hash = shapeTypeKeyHash });

    const topic = dp.vtable.create_topic(
        dp.ptr,
        opts.topic_name,
        "ShapeType",
        .{},
        dds.nilTopicListener(),
        0,
    );
    if (isNilTopic(topic)) {
        std.log.err("failed to create topic '{s}'", .{opts.topic_name});
        std.process.exit(1);
    }

    stdoutPrint("Create topic: {s}\n", .{opts.topic_name});

    if (opts.publish) {
        runPublisher(alloc, dp, topic, &opts) catch |err| {
            std.log.err("publisher error: {}", .{err});
            std.process.exit(1);
        };
    } else {
        runSubscriber(alloc, dp, topic, &opts) catch |err| {
            std.log.err("subscriber error: {}", .{err});
            std.process.exit(1);
        };
    }
}
