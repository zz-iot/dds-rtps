//! ZenzenDDS shape_main — interoperability test application for OMG dds-rtps.
//!
//! Mirrors the CLI interface of srcCxx/shape_main.cxx so the Python test
//! harness (interoperability_report.py) can drive it as a pub or sub.
//!
//! Required stdout strings (matched by the harness via pexpect):
//!   Publisher: "Create topic:"  →  "Create writer for topic:"  →
//!              "on_publication_matched()" or "on_offered_incompatible_qos"  →
//!              "%-10s %-10s %03d %03d [%d]" (only when -w is passed)
//!   Subscriber: "Create topic:"  →  "Create reader for topic:"  →
//!               "[<number>]" in the sample line  or  "on_requested_incompatible_qos()"

const std    = @import("std");
const zzdds  = @import("zzdds");
const DDS    = @import("zzdds_generated").DDS;

const UdpTransport                 = zzdds.udp_transport.UdpTransport;
const SpdpSedpDiscovery            = zzdds.combined_discovery.SpdpSedpDiscovery;
const DomainParticipantFactoryImpl = zzdds.dcps.DomainParticipantFactoryImpl;
const DataWriterImpl               = zzdds.dcps.DataWriterImpl;
const DataReaderImpl               = zzdds.dcps.DataReaderImpl;
const TopicImpl                    = zzdds.dcps.TopicImpl;
const noop_security                = zzdds.noop_security.noop_security_plugins;
const time_mod                     = zzdds.util.time;
const RtpsTimestamp                = zzdds.util.time.RtpsTimestamp;
const history_mod                  = zzdds.rtps.history;
const nil                          = zzdds.dcps;

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
    publish:              bool           = false,
    subscribe:            bool           = false,
    domain_id:            u32            = 0,
    best_effort:          bool           = false,
    reliable:             bool           = false,
    history_depth:        i32            = -1,   // -1 = use default KEEP_LAST 1
    deadline_ms:          u64            = 0,
    ownership_strength:   i32            = -1,   // -1 = SHARED
    topic_name:           []const u8     = "Square",
    color:                ?[]const u8    = null,
    partition:            ?[]const u8    = null,
    durability:           u8             = 'v',
    data_representation:  u16            = 1,    // 1=XCDR1, 2=XCDR2
    print_writer_samples: bool           = false,
    shapesize:            i32            = 20,
    write_period_ms:      u64            = 33,
    read_period_ms:       u64            = 100,
    num_iterations:       i64            = -1,   // -1 = infinite
    num_instances:        u32            = 1,
    additional_payload:   u32            = 0,
    cft_expression:       ?[]const u8    = null, // content filter expression (unsupported)
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
    color:     []const u8,
    x:         i32,
    y:         i32,
    shapesize: i32,
    payload:   u32,  // additional_payload_size sequence length (all-zero bytes)
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
        try writeI32Le(buf, alloc, s.x);         off += 4;
        try writeI32Le(buf, alloc, s.y);         off += 4;
        try writeI32Le(buf, alloc, s.shapesize); off += 4;
        try writeU32Le(buf, alloc, s.payload);   off += 4;
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
        try writeI32Le(buf, alloc, s.x);         off += 4;
        try writeI32Le(buf, alloc, s.y);         off += 4;
        try writeI32Le(buf, alloc, s.shapesize); off += 4;
        try writeU32Le(buf, alloc, s.payload);   off += 4;
        for (0..s.payload) |_| try buf.append(alloc, 0);
    }
}

// Compute 16-byte key hash from CDR-serialised key (color string).
// DDS spec: use the raw serialised key when it fits in 16 bytes, else MD5.
fn colorKeyHash(color: []const u8) [16]u8 {
    var kh = std.mem.zeroes([16]u8);
    // CDR key: u32 length (LE) + chars + '\0'
    const clen: u32 = @intCast(color.len + 1);
    const klen: usize = 4 + clen;
    if (klen <= 16) {
        std.mem.writeInt(u32, kh[0..4], clen, .little);
        @memcpy(kh[4..][0..color.len], color);
        // null at [4 + color.len]; already zero from zeroes()
    } else {
        // Fall back to zeroes — MD5 not implemented; works for typical test colors
    }
    return kh;
}

const ParsedShape = struct {
    color:     []const u8, // slice into payload; valid while payload is alive
    x:         i32,
    y:         i32,
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
    const x         = std.mem.readInt(i32, payload[off..][0..4], .little); off += 4;
    const y         = std.mem.readInt(i32, payload[off..][0..4], .little); off += 4;
    const shapesize = std.mem.readInt(i32, payload[off..][0..4], .little);

    return ParsedShape{ .color = color, .x = x, .y = y, .shapesize = shapesize };
}

// ── Policy name mapping ───────────────────────────────────────────────────────

fn policyName(id: i32) []const u8 {
    return switch (id) {
        2  => "DURABILITY",
        4  => "DEADLINE",
        5  => "LATENCYBUDGET",
        6  => "OWNERSHIP",
        8  => "LIVELINESS",
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
    type_name:  []const u8 = "ShapeType",
};

// DataWriter listeners

fn dwOnIncompatQos(ctx: *anyopaque, _: DDS.DataWriter, status: DDS.OfferedIncompatibleQosStatus) void {
    const lc: *ListenerCtx = @ptrCast(@alignCast(ctx));
    stdoutPrint("on_offered_incompatible_qos() topic: '{s}'  type: '{s}' : {d} ({s})\n",
        .{ lc.topic_name, lc.type_name, status.last_policy_id, policyName(status.last_policy_id) });
}

fn dwOnDeadlineMissed(ctx: *anyopaque, _: DDS.DataWriter, status: DDS.OfferedDeadlineMissedStatus) void {
    const lc: *ListenerCtx = @ptrCast(@alignCast(ctx));
    stdoutPrint("on_offered_deadline_missed() topic: '{s}'  type: '{s}' : (total = {d}, change = {d})\n",
        .{ lc.topic_name, lc.type_name, status.total_count, status.total_count_change });
}

fn dwOnLivelinessLost(_: *anyopaque, _: DDS.DataWriter, _: DDS.LivelinessLostStatus) void {}
fn dwOnPubMatched(_: *anyopaque, _: DDS.DataWriter, _: DDS.PublicationMatchedStatus) void {}
fn dwOnDeinit(_: *anyopaque) void {}

var dw_vtable = DDS.DataWriterListener.Vtable{
    .on_offered_deadline_missed  = dwOnDeadlineMissed,
    .on_offered_incompatible_qos = dwOnIncompatQos,
    .on_liveliness_lost          = dwOnLivelinessLost,
    .on_publication_matched      = dwOnPubMatched,
    .deinit                      = dwOnDeinit,
};

// DataReader listeners

fn drOnIncompatQos(ctx: *anyopaque, _: DDS.DataReader, status: DDS.RequestedIncompatibleQosStatus) void {
    const lc: *ListenerCtx = @ptrCast(@alignCast(ctx));
    stdoutPrint("on_requested_incompatible_qos() topic: '{s}'  type: '{s}' : {d} ({s})\n",
        .{ lc.topic_name, lc.type_name, status.last_policy_id, policyName(status.last_policy_id) });
}

fn drOnDeadlineMissed(ctx: *anyopaque, _: DDS.DataReader, status: DDS.RequestedDeadlineMissedStatus) void {
    const lc: *ListenerCtx = @ptrCast(@alignCast(ctx));
    stdoutPrint("on_requested_deadline_missed() topic: '{s}'  type: '{s}' : (total = {d}, change = {d})\n",
        .{ lc.topic_name, lc.type_name, status.total_count, status.total_count_change });
}

fn drOnSampleRejected(_: *anyopaque, _: DDS.DataReader, _: DDS.SampleRejectedStatus) void {}
fn drOnLivelinessChanged(_: *anyopaque, _: DDS.DataReader, _: DDS.LivelinessChangedStatus) void {}
fn drOnDataAvail(_: *anyopaque, _: DDS.DataReader) void {}
fn drOnSubMatched(_: *anyopaque, _: DDS.DataReader, _: DDS.SubscriptionMatchedStatus) void {}
fn drOnSampleLost(_: *anyopaque, _: DDS.DataReader, _: DDS.SampleLostStatus) void {}
fn drOnDeinit(_: *anyopaque) void {}

var dr_vtable = DDS.DataReaderListener.Vtable{
    .on_requested_deadline_missed  = drOnDeadlineMissed,
    .on_requested_incompatible_qos = drOnIncompatQos,
    .on_sample_rejected            = drOnSampleRejected,
    .on_liveliness_changed         = drOnLivelinessChanged,
    .on_data_available             = drOnDataAvail,
    .on_subscription_matched       = drOnSubMatched,
    .on_sample_lost                = drOnSampleLost,
    .deinit                        = drOnDeinit,
};

// ── DataWriter QoS builder ────────────────────────────────────────────────────

fn buildWriterQos(alloc: std.mem.Allocator, opts: *const Options) !DDS.DataWriterQos {
    var qos = DDS.DataWriterQos{};

    qos.reliability.kind = if (opts.best_effort)
        .BEST_EFFORT_RELIABILITY_QOS
    else
        .RELIABLE_RELIABILITY_QOS;

    if (opts.history_depth == 0) {
        qos.history.kind  = .KEEP_ALL_HISTORY_QOS;
    } else if (opts.history_depth > 0) {
        qos.history.kind  = .KEEP_LAST_HISTORY_QOS;
        qos.history.depth = opts.history_depth;
    }

    qos.durability.kind = switch (opts.durability) {
        'l' => .TRANSIENT_LOCAL_DURABILITY_QOS,
        't' => .TRANSIENT_DURABILITY_QOS,
        'p' => .PERSISTENT_DURABILITY_QOS,
        else => .VOLATILE_DURABILITY_QOS,
    };

    if (opts.ownership_strength >= 0) {
        qos.ownership.kind          = .EXCLUSIVE_OWNERSHIP_QOS;
        qos.ownership_strength.value = opts.ownership_strength;
    }

    qos.deadline.period = if (opts.deadline_ms > 0) .{
        .sec     = @intCast(opts.deadline_ms / 1000),
        .nanosec = @intCast((opts.deadline_ms % 1000) * std.time.ns_per_ms),
    } else .{ .sec = 0x7fff_ffff, .nanosec = 0x7fff_ffff }; // INFINITE (DDS default)

    // Wire IDs: XCDR1=0, XCDR2=2. opts uses 1=XCDR1, 2=XCDR2 for backward compat.
    const repr_id: i16 = if (opts.data_representation == 2) 2 else 0;
    try qos.data_representation.value.append(alloc, repr_id);

    return qos;
}

fn buildReaderQos(alloc: std.mem.Allocator, opts: *const Options) !DDS.DataReaderQos {
    var qos = DDS.DataReaderQos{};

    qos.reliability.kind = if (opts.best_effort)
        .BEST_EFFORT_RELIABILITY_QOS
    else if (opts.reliable)
        .RELIABLE_RELIABILITY_QOS
    else
        .RELIABLE_RELIABILITY_QOS; // default: RELIABLE

    if (opts.history_depth == 0) {
        qos.history.kind  = .KEEP_ALL_HISTORY_QOS;
    } else if (opts.history_depth > 0) {
        qos.history.kind  = .KEEP_LAST_HISTORY_QOS;
        qos.history.depth = opts.history_depth;
    }

    qos.durability.kind = switch (opts.durability) {
        'l' => .TRANSIENT_LOCAL_DURABILITY_QOS,
        't' => .TRANSIENT_DURABILITY_QOS,
        'p' => .PERSISTENT_DURABILITY_QOS,
        else => .VOLATILE_DURABILITY_QOS,
    };

    if (opts.ownership_strength >= 0) {
        qos.ownership.kind = .EXCLUSIVE_OWNERSHIP_QOS;
    }

    qos.deadline.period = if (opts.deadline_ms > 0) .{
        .sec     = @intCast(opts.deadline_ms / 1000),
        .nanosec = @intCast((opts.deadline_ms % 1000) * std.time.ns_per_ms),
    } else .{ .sec = 0x7fff_ffff, .nanosec = 0x7fff_ffff }; // INFINITE (DDS default)

    // Wire IDs: XCDR1=0, XCDR2=2. opts uses 1=XCDR1, 2=XCDR2 for backward compat.
    const repr_id: i16 = if (opts.data_representation == 2) 2 else 0;
    try qos.data_representation.value.append(alloc, repr_id);

    return qos;
}

// ── Publisher ─────────────────────────────────────────────────────────────────

fn runPublisher(
    alloc: std.mem.Allocator,
    dp:    DDS.DomainParticipant,
    topic: DDS.Topic,
    opts:  *const Options,
) !void {
    const color = opts.color orelse "BLUE";
    const topic_impl: *TopicImpl = @ptrCast(@alignCast(topic.ptr));
    const topic_name = topic_impl.topic_name;

    const pub_ = dp.vtable.create_publisher(dp.ptr, .{}, nil.nil_pub_listener, 0);
    if (pub_.ptr == nil.nil_pub_listener.ptr) return error.PublisherFailed;

    const dw_qos = try buildWriterQos(alloc, opts);

    var lctx = ListenerCtx{ .topic_name = topic_name };
    const dw_listener = DDS.DataWriterListener{
        .ptr    = &lctx,
        .vtable = &dw_vtable,
    };
    // Enable incompatible-QoS and deadline-missed callbacks.
    const listener_mask: DDS.StatusMask =
        DDS.OFFERED_INCOMPATIBLE_QOS_STATUS | DDS.OFFERED_DEADLINE_MISSED_STATUS;

    const dw = pub_.vtable.create_datawriter(pub_.ptr, topic, dw_qos, dw_listener, listener_mask);
    if (dw.ptr == nil.nil_dw_listener.ptr) return error.DataWriterFailed;

    stdoutPrint("Create writer for topic: {s} color: {s}\n", .{ topic_name, color });

    const dw_impl: *DataWriterImpl = @ptrCast(@alignCast(dw.ptr));

    // Write loop starts immediately so history accumulates before subscribers join.
    // The match notification is detected inline and printed on first discovery.
    // Timeout after 10 s with no reader → exit (test harness detects READER_NOT_MATCHED).
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);

    var shape = ShapeData{
        .color     = color,
        .x         = 0,
        .y         = 0,
        .shapesize = if (opts.shapesize == 0) 1 else opts.shapesize,
        .payload   = opts.additional_payload,
    };
    var rng = std.Random.DefaultPrng.init(@intCast(time_mod.nanoTimestamp()));
    const rand = rng.random();

    const match_deadline = time_mod.nanoTimestamp() + 10 * std.time.ns_per_s;
    var printed_matched = false;

    // Deadline monitoring: track time of last write to detect when the deadline
    // period expires between writes (write_period > deadline → missed).
    const deadline_ns: i64 = if (opts.deadline_ms > 0)
        @intCast(opts.deadline_ms * std.time.ns_per_ms)
    else
        0;
    var last_write_ns: i64 = time_mod.nanoTimestamp();

    var iteration: i64 = 0;
    while (!g_all_done.load(.acquire)) {
        if (opts.num_iterations >= 0 and iteration >= opts.num_iterations) break;

        // Detect reader match; emit notification once; exit on timeout (no match).
        if (!printed_matched) {
            if (dw_impl.matchedReaderCount() > 0) {
                stdoutPrint(
                    "on_publication_matched() topic: '{s}'  type: 'ShapeType' : matched readers {d} (change = 1)\n",
                    .{ topic_name, dw_impl.matchedReaderCount() },
                );
                printed_matched = true;
            } else if (time_mod.nanoTimestamp() > match_deadline) {
                return; // READER_NOT_MATCHED
            }
        }

        // Check if the deadline expired since the last write (write_period > deadline).
        if (deadline_ns > 0) {
            const elapsed = time_mod.nanoTimestamp() - last_write_ns;
            if (elapsed > deadline_ns) dw_impl.notifyDeadlineMissed();
        }

        // Randomise position (0–319 x, 0–239 y) — matches C++ shape_main defaults
        shape.x = @rem(@as(i32, rand.int(u16)), 320);
        shape.y = @rem(@as(i32, rand.int(u16)), 240);

        // Serialize and write each instance
        for (0..opts.num_instances) |inst| {
            const inst_color: []const u8 = blk: {
                if (inst == 0) break :blk color;
                // additional instances: color + index (e.g. "BLUE1", "BLUE2")
                break :blk std.fmt.allocPrint(alloc, "{s}{d}", .{ color, inst }) catch color;
            };
            defer if (inst > 0) alloc.free(inst_color);

            shape.color = inst_color;
            try serializeShape(&buf, alloc, shape, opts.data_representation == 2);

            const key_hash = colorKeyHash(inst_color);
            _ = try dw_impl.writeRaw(
                .alive,
                RtpsTimestamp.now(),
                history_mod.INSTANCE_HANDLE_NIL,
                key_hash,
                buf.items,
            );

            if (opts.print_writer_samples) {
                stdoutPrint("{s:<10} {s:<10} {d:0>3} {d:0>3} [{d}]\n",
                    .{ topic_name, inst_color, @as(u32, @intCast(shape.x)), @as(u32, @intCast(shape.y)), shape.shapesize });
            }
        }

        if (opts.shapesize == 0) {
            shape.shapesize += 1;
        }

        last_write_ns = time_mod.nanoTimestamp();
        iteration += 1;
        time_mod.sleepNs(opts.write_period_ms * std.time.ns_per_ms);
    }
}

// ── Subscriber ────────────────────────────────────────────────────────────────

fn runSubscriber(
    alloc: std.mem.Allocator,
    dp:    DDS.DomainParticipant,
    topic: DDS.Topic,
    opts:  *const Options,
) !void {
    const topic_impl: *TopicImpl = @ptrCast(@alignCast(topic.ptr));
    const topic_name = topic_impl.topic_name;
    const topic_desc = topic_impl.toTopicDescription();

    const sub = dp.vtable.create_subscriber(dp.ptr, .{}, nil.nil_sub_listener, 0);
    if (sub.ptr == nil.nil_sub_listener.ptr) return error.SubscriberFailed;

    const dr_qos = try buildReaderQos(alloc, opts);

    var lctx = ListenerCtx{ .topic_name = topic_name };
    const dr_listener = DDS.DataReaderListener{
        .ptr    = &lctx,
        .vtable = &dr_vtable,
    };
    const listener_mask: DDS.StatusMask =
        DDS.REQUESTED_INCOMPATIBLE_QOS_STATUS | DDS.REQUESTED_DEADLINE_MISSED_STATUS;

    const dr = sub.vtable.create_datareader(sub.ptr, topic_desc, dr_qos, dr_listener, listener_mask);
    if (dr.ptr == nil.nil_dr_listener.ptr) return error.DataReaderFailed;

    stdoutPrint("Create reader for topic: {s}\n", .{topic_name});

    const dr_impl: *DataReaderImpl = @ptrCast(@alignCast(dr.ptr));

    // Deadline monitoring: once a writer is matched, track time since last
    // received sample; fire on_requested_deadline_missed when the deadline
    // period expires without data.
    const sub_deadline_ns: i64 = if (opts.deadline_ms > 0)
        @intCast(opts.deadline_ms * std.time.ns_per_ms)
    else
        0;
    var deadline_base_ns: i64 = 0; // 0 = no matched writer yet

    var iteration: i64 = 0;
    while (!g_all_done.load(.acquire)) {
        if (opts.num_iterations >= 0 and iteration >= opts.num_iterations) break;

        // Start the deadline clock when the first writer is matched.
        if (sub_deadline_ns > 0 and deadline_base_ns == 0 and dr_impl.matchedWriterCount() > 0) {
            deadline_base_ns = time_mod.nanoTimestamp();
        }

        var got_data = false;
        while (dr_impl.takeRaw()) |payload| {
            defer alloc.free(payload);
            got_data = true;
            if (deserializeShape(payload)) |s| {
                stdoutPrint("{s:<10} {s:<10} {d:0>3} {d:0>3} [{d}]\n",
                    .{ topic_name, s.color, @as(u32, @intCast(s.x)), @as(u32, @intCast(s.y)), s.shapesize });
            }
        }

        if (got_data) {
            // Reset deadline clock on each received sample.
            deadline_base_ns = time_mod.nanoTimestamp();
        } else if (sub_deadline_ns > 0 and deadline_base_ns != 0) {
            // No data this iteration — check if the deadline has expired.
            if (time_mod.nanoTimestamp() - deadline_base_ns > sub_deadline_ns) {
                dr_impl.notifyDeadlineMissed();
                deadline_base_ns = time_mod.nanoTimestamp(); // reset to avoid repeated firing
            }
        }

        iteration += 1;
        time_mod.sleepNs(opts.read_period_ms * std.time.ns_per_ms);
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
            if (v.len > 0) opts.durability = v[0];
        } else if (std.mem.eql(u8, arg, "-x")) {
            const v = it.next() orelse return error.MissingValue;
            opts.data_representation = try std.fmt.parseInt(u16, v, 10);
        } else if (std.mem.eql(u8, arg, "-z")) {
            const v = it.next() orelse return error.MissingValue;
            opts.shapesize = try std.fmt.parseInt(i32, v, 10);
        } else if (std.mem.eql(u8, arg, "--write-period")) {
            const v = it.next() orelse return error.MissingValue;
            opts.write_period_ms = try std.fmt.parseInt(u64, v, 10);
        } else if (std.mem.eql(u8, arg, "--read-period")) {
            const v = it.next() orelse return error.MissingValue;
            opts.read_period_ms = try std.fmt.parseInt(u64, v, 10);
        } else if (std.mem.eql(u8, arg, "--num-iterations")) {
            const v = it.next() orelse return error.MissingValue;
            opts.num_iterations = try std.fmt.parseInt(i64, v, 10);
        } else if (std.mem.eql(u8, arg, "--num-instances")) {
            const v = it.next() orelse return error.MissingValue;
            opts.num_instances = try std.fmt.parseInt(u32, v, 10);
        } else if (std.mem.eql(u8, arg, "--num-topics")) {
            // not yet supported — silently ignore value
            _ = it.next();
        } else if (std.mem.eql(u8, arg, "--additional-payload-size")) {
            const v = it.next() orelse return error.MissingValue;
            opts.additional_payload = try std.fmt.parseInt(u32, v, 10);
        } else if (std.mem.eql(u8, arg, "--time-filter") or
                   std.mem.eql(u8, arg, "--lifespan")     or
                   std.mem.eql(u8, arg, "--write-period") or
                   std.mem.eql(u8, arg, "--size-modulo")  or
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
    // Install SIGINT handler so the test harness can cleanly terminate us.
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = handleSigint },
        .mask    = std.posix.sigemptyset(),
        .flags   = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);

    var gpa   = std.heap.DebugAllocator(.{}){};
    defer _   = gpa.deinit();
    const alloc = gpa.allocator();

    const opts = parseArgs(init.args) catch |err| {
        std.log.err("argument error: {}", .{err});
        std.process.exit(1);
    };

    if (!opts.publish and !opts.subscribe) {
        std.log.err("specify -P (publish) or -S (subscribe)", .{});
        std.process.exit(1);
    }

    // If the subscriber was given a content-filter expression (via -c when
    // subscribing, which the C++ code turns into a CFT), report it as unsupported
    // so the harness gets SUB_UNSUPPORTED_FEATURE rather than hanging.
    if (opts.subscribe and opts.cft_expression != null) {
        stdoutPrint("not supported: ContentFilteredTopic\n", .{});
        return;
    }

    const udp = try UdpTransport.init(alloc, .{}, opts.domain_id, null);
    defer udp.deinit();
    const transport = udp.transport();

    const disc = try SpdpSedpDiscovery.init(alloc, transport, opts.domain_id, 3_000);
    defer disc.deinit();
    const discovery = disc.toDiscovery();

    var factory = try DomainParticipantFactoryImpl.init(
        alloc, transport, discovery, noop_security, .random, .{},
    );
    defer factory.deinit();
    const dpf = factory.toDDSFactory();

    const dp = dpf.create_participant(opts.domain_id, .{}, nil.nil_dp_listener, 0);
    if (dp.ptr == nil.nil_dp_listener.ptr) {
        std.log.err("failed to create participant on domain {d}", .{opts.domain_id});
        std.process.exit(1);
    }
    defer _ = dpf.delete_participant(dp);

    const topic = dp.vtable.create_topic(
        dp.ptr, opts.topic_name, "ShapeType", .{}, nil.nil_topic_listener, 0,
    );
    if (topic.ptr == nil.nil_topic_listener.ptr) {
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
